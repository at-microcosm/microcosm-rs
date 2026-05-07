use crate::ClientMessage;
use crate::error::ConsumerError;
use crate::removable_delay_queue;
use jetstream::{
    DefaultJetstreamEndpoints, JetstreamCompression, JetstreamConfig, JetstreamConnector,
    events::{CommitOp, Cursor, EventKind},
};
use microcosm_links::collect_links;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;

const MAX_LINKS_PER_EVENT: usize = 100;

pub async fn consume(
    b: broadcast::Sender<Arc<ClientMessage>>,
    d: removable_delay_queue::Input<(String, usize), Arc<ClientMessage>>,
    jetstream_endpoint: String,
    cursor: Option<Cursor>,
    no_zstd: bool,
    shutdown: CancellationToken,
) -> Result<(), ConsumerError> {
    let endpoint = DefaultJetstreamEndpoints::endpoint_or_shortcut(&jetstream_endpoint);
    if endpoint == jetstream_endpoint {
        log::info!("consumer: connecting jetstream at {endpoint}");
    } else {
        log::info!("consumer: connecting jetstream at {jetstream_endpoint} => {endpoint}");
    }
    let config: JetstreamConfig = JetstreamConfig {
        endpoint,
        compression: if no_zstd {
            JetstreamCompression::None
        } else {
            JetstreamCompression::Zstd
        },
        replay_on_reconnect: true,
        channel_size: 1024, // buffer up to ~1s of jetstream events
        ..Default::default()
    };
    let mut receiver = JetstreamConnector::new(config)?
        .connect_cursor(cursor)
        .await?;

    log::info!("consumer: receiving messages..");
    loop {
        if shutdown.is_cancelled() {
            log::info!("consumer: exiting for shutdown");
            return Ok(());
        }
        let Some(event) = receiver.recv().await else {
            log::error!("consumer: could not receive event, bailing");
            break;
        };

        if event.kind != EventKind::Commit {
            continue;
        }
        let Some(ref commit) = event.commit else {
            log::warn!("consumer: commit event missing commit data, ignoring");
            continue;
        };

        // TODO: something a bit more robust
        let at_uri = format!(
            "at://{}/{}/{}",
            &*event.did, &*commit.collection, &*commit.rkey
        );

        // TODO: keep a buffer and remove quick deletes to debounce notifs
        // for now we just drop all deletes eek
        if commit.operation == CommitOp::Delete {
            d.remove_range((at_uri.clone(), 0)..=(at_uri.clone(), MAX_LINKS_PER_EVENT))
                .await;
            continue;
        }
        let Some(ref record) = commit.record else {
            log::warn!("consumer: commit update/delete missing record, ignoring");
            continue;
        };

        let jv = match record.get().parse() {
            Ok(v) => v,
            Err(e) => {
                log::warn!("consumer: record failed to parse, ignoring: {e}");
                continue;
            }
        };

        for (i, link) in collect_links(&jv).into_iter().enumerate() {
            if i >= MAX_LINKS_PER_EVENT {
                // todo: indicate if the link limit was reached (-> links omitted)
                log::warn!("consumer: event has too many links, ignoring the rest");
                metrics::counter!("consumer_dropped_links", "reason" => "too_many_links")
                    .increment(1);
                break;
            }
            let client_message = match ClientMessage::new_link(link, &at_uri, commit) {
                Ok(m) => m,
                Err(e) => {
                    // TODO indicate to clients that a link has been dropped
                    log::warn!("consumer: failed to serialize link to json: {e:?}");
                    metrics::counter!("consumer_dropped_links", "reason" => "failed_to_serialize")
                        .increment(1);
                    continue;
                }
            };
            let message = Arc::new(client_message);
            let _ = b.send(message.clone()); // only errors if no subscribers are connected, which is just fine.
            d.enqueue((at_uri.clone(), i), message)
                .await
                .map_err(|_| ConsumerError::DelayQueueOutputDropped)?;
        }
    }

    Err(ConsumerError::JetstreamEnded)
}
