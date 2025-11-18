use anyhow::{bail, Result};
use metrics::{
    counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram, Unit,
};
use std::io::{Cursor, ErrorKind, Read};
use std::net::ToSocketAddrs;
use std::thread;
use std::time;
use tinyjson::JsonValue;
use tokio_util::sync::CancellationToken;
use tungstenite::{client::IntoClientRequest, Error as TError, Message};
use zstd::dict::DecoderDictionary;

const JETSTREAM_ZSTD_DICTIONARY: &[u8] = include_bytes!("../../zstd/dictionary");

pub fn consume_jetstream(
    sender: flume::Sender<JsonValue>,
    cursor: Option<u64>,
    stream: String,
    staying_alive: CancellationToken,
) -> Result<()> {
    describe_counter!(
        "jetstream_connnect",
        Unit::Count,
        "attempts to connect to a jetstream server"
    );
    describe_counter!(
        "jetstream_read",
        Unit::Count,
        "attempts to read an event from jetstream"
    );
    describe_counter!(
        "jetstream_read_fail",
        Unit::Count,
        "failures to read events from jetstream"
    );
    describe_counter!(
        "jetstream_read_bytes",
        Unit::Bytes,
        "total received message bytes from jetstream"
    );
    describe_counter!(
        "jetstream_read_bytes_decompressed",
        Unit::Bytes,
        "total decompressed message bytes from jetstream"
    );
    describe_histogram!(
        "jetstream_read_bytes_decompressed",
        Unit::Bytes,
        "decompressed size of jetstream messages"
    );
    describe_counter!(
        "jetstream_events",
        Unit::Count,
        "valid json messages received"
    );
    describe_histogram!(
        "jetstream_events_queued",
        Unit::Count,
        "event messages waiting in queue"
    );
    describe_gauge!(
        "jetstream_cursor_age",
        Unit::Microseconds,
        "microseconds between our clock and the jetstream event's time_us"
    );

    let dict = DecoderDictionary::copy(JETSTREAM_ZSTD_DICTIONARY);
    let mut connect_retries = 0;
    let mut latest_cursor = cursor;
    'outer: loop {
        let stream_url = format!(
            "{stream}?compress=true{}",
            latest_cursor
                .map(|c| {
                    println!("starting with cursor from {:?} ago...", ts_age(c));
                    format!("&cursor={c}")
                })
                .unwrap_or("".into())
        );
        let mut req = (&stream_url).into_client_request()?;
        let ua = format!("microcosm/constellation v{}", env!("CARGO_PKG_VERSION"));
        req.headers_mut().insert("user-agent", ua.parse()?);

        let host = req.uri().host().expect("jetstream request uri has a host");
        let port = req.uri().port().map(|p| p.as_u16()).unwrap_or(443);
        let dest = format!("{host}:{port}");
        let addr = match dest.to_socket_addrs().map(|mut d| d.next()) {
            Ok(Some(a)) => a,
            Ok(None) => {
                eprintln!(
                    "jetstream: could not resolve an address for {dest:?}. retrying after a bit?"
                );
                thread::sleep(time::Duration::from_secs(15));
                continue;
            }
            Err(e) => {
                eprintln!("jetstream failed to resolve address {dest:?}: {e:?} waiting and then retrying...");
                thread::sleep(time::Duration::from_secs(3));
                continue;
            }
        };
        let tcp_stream = match std::net::TcpStream::connect_timeout(
            &addr,
            time::Duration::from_secs(8),
        ) {
            Ok(s) => s,
            Err(e) => {
                eprintln!(
                    "jetstream failed to make tcp connection: {e:?}. (todo: clean up retry logic)"
                );
                connect_retries += 1;
                if connect_retries >= 7 {
                    eprintln!("jetstream: no more connect retries, breaking out.");
                    break;
                }
                let backoff = time::Duration::from_secs(connect_retries.try_into().unwrap());
                eprintln!("jetstream tcp failed to connect: {e:?}. backing off {backoff:?} before retrying...");
                thread::sleep(backoff);
                continue;
            }
        };
        tcp_stream.set_read_timeout(Some(time::Duration::from_secs(4)))?;
        tcp_stream.set_write_timeout(Some(time::Duration::from_secs(4)))?;

        counter!("jetstream_connect", "url" => stream.clone(), "is_retry" => (connect_retries > 0).to_string()).increment(1);
        println!("jetstream connecting, attempt #{connect_retries}, {stream_url:?} with user-agent: {ua:?}");
        let mut socket = match tungstenite::client_tls(req, tcp_stream) {
            Ok((socket, _)) => {
                println!("jetstream connected.");
                // connect_retries = 0; // only reset once we have received a message vvv
                socket
            }
            Err(e) => {
                connect_retries += 1;
                if connect_retries >= 7 {
                    eprintln!("jetstream: no more connect retries, breaking out.");
                    break;
                }
                let backoff = time::Duration::from_secs(connect_retries.try_into().unwrap());
                eprintln!("jetstream failed to connect: {e:?}. backing off {backoff:?} before retrying...");
                thread::sleep(backoff);
                continue;
            }
        };

        loop {
            if !socket.can_read() {
                eprintln!("jetstream: socket says we cannot read -- flushing then breaking out.");
                if let Err(e) = socket.flush() {
                    eprintln!("error while flushing socket: {e:?}");
                }
                break;
            }

            if staying_alive.is_cancelled() {
                eprintln!("jetstream: cancelling");
                // TODO: cleanly close the connection?
                break 'outer;
            }

            counter!("jetstream_read").increment(1);
            let b = match socket.read() {
                Ok(Message::Binary(b)) => b,
                Ok(Message::Text(_)) => {
                    counter!("jetstream_read_fail", "url" => stream.clone(), "reason" => "received text")
                        .increment(1);
                    eprintln!("jetstream: unexpected text message, should be binary for compressed (ignoring)");
                    continue;
                }
                Ok(Message::Close(f)) => {
                    counter!("jetstream_read_fail", "url" => stream.clone(), "reason" => "server closed")
                        .increment(1);
                    println!("jetstream: closing the connection: {f:?}");
                    continue;
                }
                Ok(Message::Ping(bytes)) => {
                    let _ = socket.send(Message::Pong(bytes));
                    continue;
                }
                Ok(m) => {
                    counter!("jetstream_read_fail", "url" => stream.clone(), "reason" => "unexpected message", "message" => format!("{m:?}")).increment(1);
                    eprintln!("jetstream: unexpected from read (ignoring): {m:?}");
                    continue;
                }
                Err(TError::ConnectionClosed) => {
                    // clean exit
                    counter!("jetstream_read_fail", "url" => stream.clone(), "reason" => "clean close")
                        .increment(1);
                    println!("jetstream closed the websocket cleanly.");
                    break;
                }
                Err(TError::AlreadyClosed) => {
                    // programming error
                    counter!("jetstream_read_fail", "url" => stream.clone(), "reason" => "already closed")
                        .increment(1);
                    eprintln!(
                        "jetstream: got AlreadyClosed trying to .read() websocket. probably a bug."
                    );
                    break;
                }
                Err(TError::Capacity(e)) => {
                    counter!("jetstream_read_fail", "url" => stream.clone(), "reason" => "capacity error")
                        .increment(1);
                    eprintln!("jetstream: capacity error (ignoring): {e:?}");
                    continue;
                }
                Err(TError::Utf8) => {
                    counter!("jetstream_read_fail", "url" => stream.clone(), "reason" => "utf8 error")
                        .increment(1);
                    eprintln!("jetstream: utf8 error (ignoring)");
                    continue;
                }
                Err(e) => {
                    eprintln!("jetstream: could not read message from socket. closing: {e:?}");
                    if let TError::Io(io_err) = e {
                        if matches!(io_err.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) {
                            counter!("jetstream_read_fail", "url" => stream.clone(), "reason" => "timed out").increment(1);
                            println!("jetstream socket timed out. bailing to reconnect -- should we be trying to close first?");
                            break;
                        }
                    }
                    match socket.close(None) {
                        Err(TError::ConnectionClosed) => {
                            counter!("jetstream_read_fail", "url" => stream.clone(), "reason" => "clean close").increment(1);
                            println!("jetstream closed the websocket cleanly.");
                            break;
                        }
                        Err(_) => {
                            counter!("jetstream_read_fail", "url" => stream.clone(), "reason" => "dirty close").increment(1);
                            println!("jetstream failed to close the websocket cleanly.");
                            break;
                        }
                        Ok(r) => {
                            eprintln!("jetstream: close result after error: {r:?}");
                            counter!("jetstream_read_fail", "url" => stream.clone(), "reason" => "read error")
                                .increment(1);
                            // if we didn't immediately get ConnectionClosed, we should keep polling read
                            // until we get it.
                            continue;
                        }
                    }
                }
            };

            counter!("jetstream_read_bytes", "url" => stream.clone()).increment(b.len() as u64);
            let mut cursor = Cursor::new(b);
            let mut decoder = match zstd::stream::Decoder::with_prepared_dictionary(
                &mut cursor,
                &dict,
            ) {
                Ok(d) => d,
                Err(e) => {
                    counter!("jetstream_read_fail", "url" => stream.clone(), "reason" => "zstd decompress")
                        .increment(1);
                    eprintln!("jetstream: failed to decompress zstd message: {e:?}");
                    continue;
                }
            };

            let mut s = String::new();
            match decoder.read_to_string(&mut s) {
                Ok(n) => {
                    counter!("jetstream_read_bytes_decompressed", "url" => stream.clone())
                        .increment(n as u64);
                    histogram!("jetstream_read_bytes_decompressed", "url" => stream.clone())
                        .record(n as f64);
                }
                Err(e) => {
                    counter!("jetstream_read_fail", "url" => stream.clone(), "reason" => "zstd string decode")
                        .increment(1);
                    eprintln!("jetstream: failed to decode zstd: {e:?}");
                    continue;
                }
            }

            let v = match s.parse() {
                Ok(v) => v,
                Err(e) => {
                    counter!("jetstream_read_fail", "url" => stream.clone(), "reason" => "json parse")
                        .increment(1);
                    eprintln!("jetstream: failed to parse message as json: {e:?}");
                    continue;
                }
            };

            // bit of a hack to have this here for now...
            let ts = match get_event_time(&v) {
                Some(ts) => ts,
                None => {
                    counter!("jetstream_read_fail", "url" => stream.clone(), "reason" => "invalid event")
                        .increment(1);
                    eprintln!("jetstream: encountered an event without a timestamp: ignoring it.");
                    continue;
                }
            };

            if let Err(flume::SendError(_rejected)) = sender.send(v) {
                counter!("jetstream_events", "url" => stream.clone()).increment(1);
                if sender.is_disconnected() {
                    eprintln!("jetstream: send channel disconnected -- nothing to do, bye.");
                    bail!("jetstream: send channel disconnected");
                }
                eprintln!(
                    "jetstream: failed to send on channel, dropping update! (FIXME / HANDLEME)"
                );
            }
            histogram!("jetstream_events_queued", "url" => stream.clone())
                .record(sender.len() as f64);

            // only actually update our cursor after we've managed to queue the event
            latest_cursor = Some(ts);
            gauge!("jetstream_cursor_age", "url" => stream.clone())
                .set(ts_age(ts).as_micros() as f64);

            // great news if we got this far -- might be safe to assume the connection is up.
            connect_retries = 0;
        }
    }
    Ok(())
}

fn get_event_time(v: &JsonValue) -> Option<u64> {
    if let JsonValue::Object(root) = v {
        if let JsonValue::Number(time_us) = root.get("time_us")? {
            return Some(*time_us as u64);
        };
    };
    None
}

fn ts_age(ts: u64) -> time::Duration {
    (time::UNIX_EPOCH + time::Duration::from_micros(ts))
        .elapsed()
        .unwrap_or(time::Duration::from_secs(0)) // saturate zero if ts > our system time
}
