use crate::store_types::{CountsValue, HourTruncatedCursor, SketchSecretPrefix};
use crate::{
    error::StorageError, ConsumerInfo, Cursor, EventBatch, JustCount, NsidCount, NsidPrefix,
    OrderCollectionsBy, PrefixChild, UFOsRecord,
};
use async_trait::async_trait;
use jetstream::exports::{Did, Nsid};
use metrics::{describe_histogram, histogram, Unit};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;

pub type StorageResult<T> = Result<T, StorageError>;

pub trait StorageWhatever<R: StoreReader, W: StoreWriter<B>, B: StoreBackground, C> {
    fn init(
        path: impl AsRef<Path>,
        endpoint: String,
        force_endpoint: bool,
        config: C,
    ) -> StorageResult<(R, W, Option<Cursor>, SketchSecretPrefix)>
    where
        Self: Sized;
}

#[async_trait]
pub trait StoreWriter<B: StoreBackground>: Clone + Send + Sync
where
    Self: 'static,
{
    fn background_tasks(&mut self, reroll: bool) -> StorageResult<B>;

    async fn receive_batches<const LIMIT: usize>(
        self,
        mut batches: Receiver<EventBatch<LIMIT>>,
    ) -> StorageResult<()> {
        describe_histogram!(
            "storage_slow_batches",
            Unit::Microseconds,
            "batches that took more than 3s to insert"
        );
        describe_histogram!(
            "storage_batch_insert_time",
            Unit::Microseconds,
            "total time to insert one commit batch"
        );
        while let Some(event_batch) = batches.recv().await {
            let token = CancellationToken::new();
            let cancelled = token.clone();
            tokio::spawn(async move {
                let started = Instant::now();
                let mut concerned = false;
                loop {
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(3)) => {
                            if !concerned {
                                log::warn!("taking a long time to insert an event batch...");
                            }
                            concerned = true;
                        }
                        _ = cancelled.cancelled() => {
                            if concerned {
                                log::warn!("finally inserted slow event batch (or failed) after {:?}", started.elapsed());
                                histogram!("storage_slow_batches").record(started.elapsed().as_micros() as f64);
                            }
                            break
                        }
                    }
                }
            });
            tokio::task::spawn_blocking({
                let mut me = self.clone();
                move || {
                    let _guard = token.drop_guard();
                    let t0 = Instant::now();
                    let r = me.insert_batch(event_batch);
                    histogram!("storage_batch_insert_time").record(t0.elapsed().as_micros() as f64);
                    r
                }
            })
            .await??;
        }

        Err(StorageError::BatchSenderExited)
    }

    fn insert_batch<const LIMIT: usize>(
        &mut self,
        event_batch: EventBatch<LIMIT>,
    ) -> StorageResult<()>;

    fn step_rollup(&mut self) -> StorageResult<(usize, HashSet<Nsid>)>;

    fn trim_collection(
        &mut self,
        collection: &Nsid,
        limit: usize,
        full_scan: bool,
    ) -> StorageResult<(usize, usize, bool)>;

    fn delete_account(&mut self, did: &Did) -> StorageResult<usize>;
}

#[async_trait]
pub trait StoreBackground: Send + Sync {
    async fn run(mut self, backfill: bool) -> StorageResult<()>;
}

#[async_trait]
pub trait StoreReader: Send + Sync {
    fn name(&self) -> String;

    fn update_metrics(&self) {}

    async fn get_storage_stats(&self) -> StorageResult<serde_json::Value>;

    async fn get_consumer_info(&self) -> StorageResult<ConsumerInfo>;

    async fn get_collections(
        &self,
        limit: usize,
        order: OrderCollectionsBy,
        since: Option<HourTruncatedCursor>,
        until: Option<HourTruncatedCursor>,
    ) -> StorageResult<(Vec<NsidCount>, Option<Vec<u8>>)>;

    async fn get_prefix(
        &self,
        prefix: NsidPrefix,
        limit: usize,
        order: OrderCollectionsBy,
        since: Option<HourTruncatedCursor>,
        until: Option<HourTruncatedCursor>,
    ) -> StorageResult<(JustCount, Vec<PrefixChild>, Option<Vec<u8>>)>;

    async fn get_timeseries(
        &self,
        collections: Vec<Nsid>,
        since: HourTruncatedCursor,
        until: Option<HourTruncatedCursor>,
        step: u64,
    ) -> StorageResult<(Vec<HourTruncatedCursor>, HashMap<Nsid, Vec<CountsValue>>)>;

    async fn get_collection_counts(
        &self,
        collection: &Nsid,
        since: HourTruncatedCursor,
        until: Option<HourTruncatedCursor>,
    ) -> StorageResult<JustCount>;

    async fn get_records_by_collections(
        &self,
        collections: HashSet<Nsid>,
        limit: usize,
        expand_each_collection: bool,
    ) -> StorageResult<Vec<UFOsRecord>>;

    async fn search_collections(&self, terms: Vec<String>) -> StorageResult<Vec<NsidCount>>;
}
