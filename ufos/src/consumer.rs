use crate::store_types::SketchSecretPrefix;
use jetstream::{
    events::{Cursor, EventKind, JetstreamEvent},
    exports::{Did, Nsid},
    DefaultJetstreamEndpoints, JetstreamCompression, JetstreamConfig, JetstreamConnector,
    JetstreamReceiver,
};
use metrics::{
    counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram, Unit,
};
use std::mem;
use std::time::Duration;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{timeout, Interval};

use crate::error::{BatchInsertError, FirehoseEventError};
use crate::{DeleteAccount, EventBatch, UFOsCommit};

pub const MAX_BATCHED_RECORDS: usize = 128; // *non-blocking* limit. drops oldest batched record per collection once reached.
pub const MAX_ACCOUNT_REMOVES: usize = 1024; // hard limit, extremely unlikely to reach, but just in case
pub const MAX_BATCHED_COLLECTIONS: usize = 64; // hard limit, MAX_BATCHED_RECORDS applies per-collection
pub const MIN_BATCH_SPAN_SECS: f64 = 2.; // breathe
pub const MAX_BATCH_SPAN_SECS: f64 = 60.; // hard limit, pause consumer if we're unable to send by now
pub const SEND_TIMEOUT_S: f64 = 150.; // if the channel is blocked longer than this, something is probably up
pub const BATCH_QUEUE_SIZE: usize = 64; // used to be 1, but sometimes inserts are just really slow????????

pub type LimitedBatch = EventBatch<MAX_BATCHED_RECORDS>;

#[derive(Debug, Default)]
struct CurrentBatch {
    initial_cursor: Option<Cursor>,
    batch: LimitedBatch,
}

#[derive(Debug)]
pub struct Batcher {
    jetstream_receiver: JetstreamReceiver,
    batch_sender: Sender<LimitedBatch>,
    current_batch: CurrentBatch,
    sketch_secret: SketchSecretPrefix,
    rate_limit: Interval,
}

pub async fn consume(
    jetstream_endpoint: &str,
    cursor: Option<Cursor>,
    no_compress: bool,
    sketch_secret: SketchSecretPrefix,
) -> anyhow::Result<Receiver<LimitedBatch>> {
    let endpoint = DefaultJetstreamEndpoints::endpoint_or_shortcut(jetstream_endpoint);
    if endpoint == jetstream_endpoint {
        log::info!("connecting to jetstream at {endpoint}");
    } else {
        log::info!("connecting to jetstream at {jetstream_endpoint} => {endpoint}");
    }
    let config: JetstreamConfig = JetstreamConfig {
        endpoint,
        compression: if no_compress {
            JetstreamCompression::None
        } else {
            JetstreamCompression::Zstd
        },
        replay_on_reconnect: true,
        channel_size: 1024, // buffer up to ~1s of jetstream events
        ..Default::default()
    };
    let jetstream_receiver = JetstreamConnector::new(config)?
        .connect_cursor(cursor)
        .await?;
    let (batch_sender, batch_reciever) = channel::<LimitedBatch>(BATCH_QUEUE_SIZE);
    let mut batcher = Batcher::new(jetstream_receiver, batch_sender, sketch_secret);
    tokio::task::spawn(async move {
        let r = batcher.run().await;
        log::warn!("batcher ended: {r:?}");
    });
    Ok(batch_reciever)
}

impl Batcher {
    pub fn new(
        jetstream_receiver: JetstreamReceiver,
        batch_sender: Sender<LimitedBatch>,
        sketch_secret: SketchSecretPrefix,
    ) -> Self {
        describe_counter!(
            "batcher_batches_sent",
            Unit::Count,
            "how many batches of events were sent from Batcher to storage"
        );
        describe_gauge!(
            "batcher_batch_age",
            Unit::Microseconds,
            "how old the last-sent batch was"
        );
        describe_gauge!(
            "batcher_send_queue_capacity",
            Unit::Count,
            "how many spaces are available for batches in the send queue"
        );
        describe_histogram!(
            "batcher_total_collections",
            Unit::Count,
            "how many collections are in this batch"
        );
        let mut rate_limit = tokio::time::interval(std::time::Duration::from_millis(3));
        rate_limit.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Self {
            jetstream_receiver,
            batch_sender,
            current_batch: Default::default(),
            sketch_secret,
            rate_limit,
        }
    }

    pub async fn run(&mut self) -> anyhow::Result<()> {
        // TODO: report errors *from here* probably, since this gets shipped off into a spawned task that might just vanish
        loop {
            match timeout(Duration::from_secs_f64(30.), self.jetstream_receiver.recv()).await {
                Err(_elapsed) => self.no_events_step().await?,
                Ok(Some(event)) => self.handle_event(event).await?,
                Ok(None) => anyhow::bail!("channel closed"),
            }
        }
    }

    async fn no_events_step(&mut self) -> anyhow::Result<()> {
        let empty = self.current_batch.batch.is_empty();
        log::info!("no events received, stepping batcher (empty? {empty})");
        if !empty {
            self.send_current_batch_now(true, "no events step").await?;
        }
        Ok(())
    }

    async fn handle_event(&mut self, event: JetstreamEvent) -> anyhow::Result<()> {
        if let Some(earliest) = &self.current_batch.initial_cursor {
            if event.cursor.duration_since(earliest)? > Duration::from_secs_f64(MAX_BATCH_SPAN_SECS)
            {
                self.send_current_batch_now(false, "time since event")
                    .await?;
            }
        } else {
            self.current_batch.initial_cursor = Some(event.cursor);
        }

        match event.kind {
            EventKind::Commit => {
                let commit = event
                    .commit
                    .ok_or(FirehoseEventError::CommitEventMissingCommit)?;
                let (commit, nsid) = UFOsCommit::from_commit_info(commit, event.did, event.cursor)?;
                self.handle_commit(commit, nsid).await?;
            }
            EventKind::Account => {
                let account = event
                    .account
                    .ok_or(FirehoseEventError::AccountEventMissingAccount)?;
                if !account.active {
                    self.handle_delete_account(event.did, event.cursor).await?;
                }
            }
            _ => {}
        }

        // if the queue is empty and we have enough, send immediately. otherewise, let the current batch fill up.
        if let Some(earliest) = &self.current_batch.initial_cursor {
            if event.cursor.duration_since(earliest)?.as_secs_f64() > MIN_BATCH_SPAN_SECS
                && self.batch_sender.capacity() == BATCH_QUEUE_SIZE
            {
                self.send_current_batch_now(true, "available queue").await?;
            }
        }
        Ok(())
    }

    async fn handle_commit(&mut self, commit: UFOsCommit, collection: Nsid) -> anyhow::Result<()> {
        let optimistic_res = self.current_batch.batch.insert_commit_by_nsid(
            &collection,
            commit,
            MAX_BATCHED_COLLECTIONS,
            &self.sketch_secret,
        );

        if let Err(BatchInsertError::BatchFull(commit)) = optimistic_res {
            self.send_current_batch_now(false, "handle commit").await?;
            self.current_batch.batch.insert_commit_by_nsid(
                &collection,
                commit,
                MAX_BATCHED_COLLECTIONS,
                &self.sketch_secret,
            )?;
        } else {
            optimistic_res?;
        }

        Ok(())
    }

    async fn handle_delete_account(&mut self, did: Did, cursor: Cursor) -> anyhow::Result<()> {
        if self.current_batch.batch.account_removes.len() >= MAX_ACCOUNT_REMOVES {
            self.send_current_batch_now(false, "delete account").await?;
        }
        self.current_batch
            .batch
            .account_removes
            .push(DeleteAccount { did, cursor });
        Ok(())
    }

    // holds up all consumer progress until it can send to the channel
    // use this when the current batch is too full to add more to it
    async fn send_current_batch_now(&mut self, small: bool, referrer: &str) -> anyhow::Result<()> {
        let size_label = if small { "small" } else { "full" };
        let queue_cap = self.batch_sender.capacity();

        if let Some(cursor) = self.current_batch.initial_cursor {
            gauge!("batcher_batch_age", "size" => size_label).set(cursor.elapsed_micros_f64());
        }
        histogram!("batcher_total_collections", "size" => size_label)
            .record(self.current_batch.batch.total_collections() as f64);
        gauge!("batcher_send_queue_capacity").set(queue_cap as f64);

        let beginning = match self.current_batch.initial_cursor.map(|c| c.elapsed()) {
            None => "unknown".to_string(),
            Some(Ok(t)) => format!("{t:?}"),
            Some(Err(e)) => format!("+{:?}", e.duration()),
        };
        log::trace!(
            "sending batch now from {beginning}, {size_label}, queue capacity: {queue_cap}, referrer: {referrer}",
        );
        let current = mem::take(&mut self.current_batch);
        self.rate_limit.tick().await;
        self.batch_sender
            .send_timeout(current.batch, Duration::from_secs_f64(SEND_TIMEOUT_S))
            .await?;
        counter!("batcher_batches_sent", "size" => size_label, "referrer" => referrer.to_string())
            .increment(1);
        Ok(())
    }
}
