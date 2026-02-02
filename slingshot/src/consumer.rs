use crate::error::ConsumerError;
use crate::{CachedRecord, Identity, IdentityKey};
use foyer::HybridCache;
use jetstream::{
    DefaultJetstreamEndpoints, JetstreamCompression, JetstreamConfig, JetstreamConnector,
    events::{CommitOp, Cursor, EventKind},
};
use tokio_util::sync::CancellationToken;

pub async fn consume(
    jetstream_endpoint: String,
    cursor: Option<Cursor>,
    no_zstd: bool,
    identity: Identity,
    shutdown: CancellationToken,
    cache: HybridCache<String, CachedRecord>,
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
        let Some(mut event) = receiver.recv().await else {
            log::error!("consumer: could not receive event, bailing");
            break;
        };

        match event.kind {
            EventKind::Commit => {
                let Some(ref mut commit) = event.commit else {
                    log::warn!("consumer: commit event missing commit data, ignoring");
                    continue;
                };

                // TODO: something a bit more robust
                let at_uri = format!(
                    "at://{}/{}/{}",
                    &*event.did, &*commit.collection, &*commit.rkey
                );

                if commit.operation == CommitOp::Delete {
                    cache.insert(at_uri, CachedRecord::Deleted);
                } else {
                    let Some(record) = commit.record.take() else {
                        log::warn!("consumer: commit insert or update missing record, ignoring");
                        continue;
                    };
                    let Some(cid) = commit.cid.take() else {
                        log::warn!("consumer: commit insert or update missing CID, ignoring");
                        continue;
                    };

                    cache.insert(at_uri, CachedRecord::Found((cid, record).into()));
                }
            }
            EventKind::Identity => {
                let Some(ident) = event.identity else {
                    log::warn!("consumer: identity event missing identity data, ignoring");
                    continue;
                };
                if let Some(handle) = ident.handle {
                    metrics::counter!("identity_handle_refresh_queued", "reason" => "identity event").increment(1);
                    identity.queue_refresh(IdentityKey::Handle(handle)).await;
                }
                metrics::counter!("identity_did_refresh_queued", "reason" => "identity event")
                    .increment(1);
                identity.queue_refresh(IdentityKey::Did(ident.did)).await;
            }
            EventKind::Account => {} // TODO: handle account events (esp hiding content on deactivate, clearing on delete)
        }
    }

    Err(ConsumerError::JetstreamEnded)
}
