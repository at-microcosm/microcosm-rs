use anyhow::{bail, Result};
use clap::{Parser, ValueEnum};
use metrics::{describe_counter, describe_gauge, describe_histogram, Unit};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::net::SocketAddr;
use std::num::NonZero;
use std::path::PathBuf;
use std::sync::{atomic::AtomicU32, Arc};
use std::thread;
use std::time;
use tokio::runtime;
use tokio_util::sync::CancellationToken;

use constellation::consumer::consume;
use constellation::server::serve;
#[cfg(feature = "rocks")]
use constellation::storage::RocksStorage;
use constellation::storage::{LinkReader, LinkStorage, MemStorage, StorageStats};

const MONITOR_INTERVAL: time::Duration = time::Duration::from_secs(15);

/// Aggregate links in the at-mosphere
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// constellation server's listen address
    #[arg(long)]
    #[clap(default_value = "0.0.0.0:6789")]
    bind: SocketAddr,
    /// enable metrics collection and serving
    #[arg(long, action)]
    collect_metrics: bool,
    /// metrics server's listen address
    #[arg(long, requires("collect_metrics"))]
    #[clap(default_value = "0.0.0.0:8765")]
    bind_metrics: SocketAddr,
    /// Jetstream server to connect to (exclusive with --fixture). Provide either a wss:// URL, or a shorhand value:
    /// 'us-east-1', 'us-east-2', 'us-west-1', or 'us-west-2'
    #[arg(short, long)]
    jetstream: String,
    // TODO: make this part of rocks' own sub-config?
    /// Where to store data on disk, for backends that use disk storage
    #[arg(short, long)]
    data: Option<PathBuf>,
    /// Storage backend to use
    #[arg(short, long)]
    #[clap(value_enum, default_value_t = StorageBackend::Memory)]
    backend: StorageBackend,
    /// Serve a did:web document for this domain
    #[arg(long)]
    did_web_domain: Option<String>,
    /// Initiate a database backup into this dir, if supported by the storage
    #[arg(long)]
    backup: Option<PathBuf>,
    /// Start a background task to take backups every N hours
    #[arg(long)]
    backup_interval: Option<u64>,
    /// Keep at most this many backups purging oldest first, requires --backup-interval
    #[arg(long)]
    max_old_backups: Option<usize>,
    /// Saved jsonl from jetstream to use instead of a live subscription
    #[arg(short, long)]
    fixture: Option<PathBuf>,
    /// Don't change the database jetstream cursor when using a fixture
    #[arg(long, requires("fixture"))]
    fixture_preserve_cursor: bool,
    /// fix the constellation start date (funny previous bug oops)
    #[arg(long, action)]
    reset_db_start: bool,
}

#[derive(Debug, Clone, ValueEnum)]
enum StorageBackend {
    Memory,
    #[cfg(feature = "rocks")]
    Rocks,
}

fn jetstream_url(provided: &str) -> String {
    match provided {
        "us-east-1" => "wss://jetstream1.us-east.bsky.network/subscribe".into(),
        "us-east-2" => "wss://jetstream2.us-east.bsky.network/subscribe".into(),
        "us-west-1" => "wss://jetstream1.us-west.bsky.network/subscribe".into(),
        "us-west-2" => "wss://jetstream2.us-west.bsky.network/subscribe".into(),
        custom => custom.into(),
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("starting with storage backend: {:?}...", args.backend);

    let fixture = args.fixture;
    let fixture_preserve_cursor = args.fixture_preserve_cursor;
    if let Some(ref p) = fixture {
        println!("using fixture at {p:?}, preserving cursor? {fixture_preserve_cursor:?}...");
    }

    let stream = jetstream_url(&args.jetstream);
    println!("using jetstream server {stream:?}...",);

    let bind = args.bind;
    let metrics_bind = args.bind_metrics;

    let collect_metrics = args.collect_metrics;
    let stay_alive = CancellationToken::new();

    match args.backend {
        StorageBackend::Memory => run(
            MemStorage::new(),
            fixture,
            fixture_preserve_cursor,
            None,
            args.did_web_domain,
            stream,
            bind,
            metrics_bind,
            collect_metrics,
            stay_alive,
        ),
        #[cfg(feature = "rocks")]
        StorageBackend::Rocks => {
            let storage_dir = args.data.clone().unwrap_or("rocks.test".into());
            println!("starting rocksdb...");
            let mut rocks = RocksStorage::new(storage_dir)?;
            if let Some(backup_dir) = args.backup {
                let auto_backup = match (args.backup_interval, args.max_old_backups) {
                    (Some(interval_hrs), copies) => Some((interval_hrs, copies)),
                    (None, None) => None,
                    (None, Some(_)) => bail!("invalid backup config: --max-old-backups requires --backup-interval to be configured"),
                };
                rocks.start_backup(backup_dir, auto_backup, stay_alive.clone())?;
            }
            println!("rocks ready.");
            std::thread::scope(|s| {
                if args.reset_db_start {
                    let rocks = rocks.clone();
                    s.spawn(move || {
                        let res = rocks.reset_start();
                        eprintln!("reset start finished: {res:?}");
                    });
                }
                s.spawn(|| {
                    let r = run(
                        rocks,
                        fixture,
                        fixture_preserve_cursor,
                        args.data,
                        args.did_web_domain,
                        stream,
                        bind,
                        metrics_bind,
                        collect_metrics,
                        stay_alive,
                    );
                    eprintln!("run finished: {r:?}");
                    r
                });
            });
            Ok(())
        }
    }
}

#[allow(clippy::too_many_lines)]
#[allow(clippy::too_many_arguments)]
fn run(
    mut storage: impl LinkStorage,
    fixture: Option<PathBuf>,
    fixture_preserve_cursor: bool,
    data_dir: Option<PathBuf>,
    did_web_domain: Option<String>,
    stream: String,
    bind: SocketAddr,
    metrics_bind: SocketAddr,
    collect_metrics: bool,
    stay_alive: CancellationToken,
) -> Result<()> {
    ctrlc::set_handler({
        let mut desperation: u8 = 0;
        let stay_alive = stay_alive.clone();
        move || match desperation {
            0 => {
                println!("ok, shutting down...");
                stay_alive.cancel();
                desperation += 1;
            }
            1.. => panic!("fine, panicking!"),
        }
    })?;

    // Install metrics server only if requested
    if collect_metrics {
        install_metrics_server(metrics_bind)?;
    }

    let qsize = Arc::new(AtomicU32::new(0));

    thread::scope(|s| {
        let readable = storage.to_readable();

        s.spawn({
            let qsize = qsize.clone();
            let stay_alive = stay_alive.clone();
            let staying_alive = stay_alive.clone();
            move || {
                if let Err(e) = consume(
                    storage,
                    qsize,
                    fixture,
                    fixture_preserve_cursor,
                    stream,
                    staying_alive,
                ) {
                    eprintln!("jetstream finished with error: {e}");
                }
                stay_alive.drop_guard();
            }
        });

        s.spawn({
            let readable = readable.clone();
            let stay_alive = stay_alive.clone();
            let staying_alive = stay_alive.clone();
            || {
                runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .max_blocking_threads(2)
                    .enable_all()
                    .build()
                    .expect("axum startup")
                    .block_on(serve(readable, bind, did_web_domain, staying_alive))
                    .unwrap();
                stay_alive.drop_guard();
            }
        });

        // only spawn monitoring thread if the metrics server is running
        if collect_metrics {
            s.spawn(move || { // monitor thread
                let stay_alive = stay_alive.clone();
                let check_alive = stay_alive.clone();

                let process_collector = metrics_process::Collector::default();
                if let Some(ref p) = data_dir {
                    if let Err(e) = fs4::available_space(p) {
                        eprintln!("fs4 failed to get available space. may not be supported here? space metrics may be absent. e: {e:?}");
                    } else {
                        println!("disk space monitoring should work, watching at {p:?}");
                    }
                }

                'monitor: loop {
                    match readable.get_stats() {
                        Ok(StorageStats { dids, targetables, linking_records, .. }) => {
                            metrics::gauge!("storage.stats.dids").set(dids as f64);
                            metrics::gauge!("storage.stats.targetables").set(targetables as f64);
                            metrics::gauge!("storage.stats.linking_records").set(linking_records as f64);
                        }
                        Err(e) => eprintln!("failed to get stats: {e:?}"),
                    }

                    process_collector.collect();
                    if let Some(ref p) = data_dir {
                        if let Ok(avail) = fs4::available_space(p) {
                            metrics::gauge!("storage.available").set(avail as f64);
                        }
                        if let Ok(free) = fs4::free_space(p) {
                            metrics::gauge!("storage.free").set(free as f64);
                        }
                    }
                    let wait = time::Instant::now();
                    while wait.elapsed() < MONITOR_INTERVAL {
                        thread::sleep(time::Duration::from_millis(100));
                        if check_alive.is_cancelled() {
                            break 'monitor
                        }
                    }
                }
                stay_alive.drop_guard();
                });
        }
    });

    println!("byeeee");

    Ok(())
}

fn install_metrics_server(metrics_bind: SocketAddr) -> Result<()> {
    println!("installing metrics server...");
    #[expect(
        deprecated,
        reason = "would change counters to _total suffix, needs dash updates"
    )]
    PrometheusBuilder::new()
        .idle_timeout(
            metrics_util::MetricKindMask::ALL,
            Some(time::Duration::from_secs(900)), // 15 min
        )
        .set_quantiles(&[0.5, 0.9, 0.99, 1.0])?
        .set_bucket_duration(time::Duration::from_secs(30))?
        .set_bucket_count(NonZero::new(10).unwrap()) // count * duration = 5 mins. stuff doesn't happen that fast here.
        .set_enable_unit_suffix(true)
        .with_http_listener(metrics_bind)
        .install()?;
    describe_metrics();
    println!("metrics server installed! listening at {metrics_bind:?}");
    Ok(())
}

fn describe_metrics() {
    metrics_process::Collector::default().describe();
    describe_gauge!(
        "storage_available",
        Unit::Bytes,
        "available to be allocated"
    );
    describe_gauge!("storage_free", Unit::Bytes, "unused bytes in filesystem");
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
    describe_counter!(
        "consumer_events_non_actionable",
        Unit::Count,
        "count of non-actionable events"
    );
    describe_counter!(
        "consumer_events_actionable",
        Unit::Count,
        "count of action by type. *all* atproto record delete events are included"
    );
    describe_counter!(
        "consumer_events_actionable_links",
        Unit::Count,
        "total links encountered"
    );
    describe_histogram!(
        "consumer_events_actionable_links",
        Unit::Count,
        "number of links per message"
    );
    #[cfg(feature = "rocks")]
    {
        describe_histogram!(
            "storage_rocksdb_read_seconds",
            Unit::Seconds,
            "duration of the read stage of actions"
        );
        describe_histogram!(
            "storage_rocksdb_action_seconds",
            Unit::Seconds,
            "duration of read + write of actions"
        );
        describe_counter!(
            "storage_rocksdb_batch_ops_total",
            Unit::Count,
            "total batched operations from actions"
        );
        describe_histogram!(
            "storage_rocksdb_delete_account_ops",
            Unit::Count,
            "total batched ops for account deletions"
        );
    }
}

#[cfg(test)]
mod tests {
    use constellation::consumer::get_actionable;
    use constellation::storage::{LinkReader, LinkStorage, MemStorage};

    #[test]
    fn test_create_like_integrated() {
        let mut storage = MemStorage::new();

        let rec = r#"{
            "did":"did:plc:icprmty6ticzracr5urz4uum",
            "time_us":1736448492661668,
            "kind":"commit",
            "commit":{"rev":"3lfddpt5qa62c","operation":"create","collection":"app.bsky.feed.like","rkey":"3lfddpt5djw2c","record":{
                "$type":"app.bsky.feed.like",
                "createdAt":"2025-01-09T18:48:10.412Z",
                "subject":{"cid":"bafyreihazf62qvmusup55ojhkzwbmzee6rxtsug3e6eg33mnjrgthxvozu","uri":"at://did:plc:lphckw3dz4mnh3ogmfpdgt6z/app.bsky.feed.post/3lfdau5f7wk23"}
            },
            "cid":"bafyreidgcs2id7nsbp6co42ind2wcig3riwcvypwan6xdywyfqklovhdjq"}
        }"#.parse().unwrap();
        let (action, ts) = get_actionable(&rec).unwrap();
        storage.push(&action, ts).unwrap();
        assert_eq!(
            storage
                .get_count(
                    "at://did:plc:lphckw3dz4mnh3ogmfpdgt6z/app.bsky.feed.post/3lfdau5f7wk23",
                    "app.bsky.feed.like",
                    ".subject.uri"
                )
                .unwrap(),
            1
        );
    }
}
