use hickory_resolver::{ResolveError, TokioResolver};
use std::collections::{HashSet, VecDeque};
use std::path::Path;
use std::sync::Arc;
/// for now we're gonna just keep doing more cache
///
/// plc.director x foyer, ttl kept with data, refresh deferred to background on fetch
///
/// things we need:
///
/// 1. handle -> DID resolution: getRecord must accept a handle for `repo` param
/// 2. DID -> PDS resolution: so we know where to getRecord
/// 3. DID -> handle resolution: for bidirectional handle validation and in case we want to offer this
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::error::IdentityError;
use atrium_api::{
    did_doc::DidDocument,
    types::string::{Did, Handle},
};
use atrium_common::resolver::Resolver;
use atrium_identity::{
    did::{CommonDidResolver, CommonDidResolverConfig, DEFAULT_PLC_DIRECTORY_URL},
    handle::{AtprotoHandleResolver, AtprotoHandleResolverConfig, DnsTxtResolver},
};
use atrium_oauth::DefaultHttpClient; // it's probably not worth bringing all of atrium_oauth for this but
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    PsyncIoEngineConfig,
};
use serde::{Deserialize, Serialize};
use time::UtcDateTime;

/// once we have something resolved, don't re-resolve until after this period
const MIN_TTL: Duration = Duration::from_secs(4 * 3600); // probably shoudl have a max ttl
const MIN_NOT_FOUND_TTL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum IdentityKey {
    Handle(Handle),
    Did(Did),
}

impl IdentityKey {
    fn weight(&self) -> usize {
        let s = match self {
            IdentityKey::Handle(h) => h.as_str(),
            IdentityKey::Did(d) => d.as_str(),
        };
        std::mem::size_of::<Self>() + std::mem::size_of_val(s)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct IdentityVal(UtcDateTime, IdentityData);

#[derive(Debug, Serialize, Deserialize)]
enum IdentityData {
    NotFound,
    Did(Did),
    Doc(PartialMiniDoc),
}

impl IdentityVal {
    fn weight(&self) -> usize {
        let wrapping = std::mem::size_of::<Self>();
        let inner = match &self.1 {
            IdentityData::NotFound => 0,
            IdentityData::Did(d) => std::mem::size_of_val(d.as_str()),
            IdentityData::Doc(d) => {
                std::mem::size_of_val(d.unverified_handle.as_str())
                    + std::mem::size_of_val(d.pds.as_str())
                    + std::mem::size_of_val(d.signing_key.as_str())
            }
        };
        wrapping + inner
    }
}

/// partial representation of a com.bad-example.identity mini atproto doc
///
/// partial because the handle is not verified
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialMiniDoc {
    /// an atproto handle (**unverified**)
    ///
    /// the first valid atproto handle from the did doc's aka
    pub unverified_handle: Handle,
    /// the did's atproto pds url (TODO: type this?)
    ///
    /// note: atrium *does* actually parse it into a URI, it just doesn't return
    /// that for some reason
    pub pds: String,
    /// for now we're just pulling this straight from the did doc
    ///
    /// would be nice to type and validate it
    ///
    /// this is the publicKeyMultibase from the did doc.
    /// legacy key encoding not supported.
    /// `id`, `type`, and `controller` must be checked, but aren't stored.
    pub signing_key: String,
}

impl TryFrom<DidDocument> for PartialMiniDoc {
    type Error = String;
    fn try_from(did_doc: DidDocument) -> Result<Self, Self::Error> {
        // must use the first valid handle
        let mut unverified_handle = None;
        let Some(ref doc_akas) = did_doc.also_known_as else {
            return Err("did doc missing `also_known_as`".to_string());
        };
        for aka in doc_akas {
            let Some(maybe_handle) = aka.strip_prefix("at://") else {
                continue;
            };
            let Ok(valid_handle) = Handle::new(maybe_handle.to_lowercase()) else {
                continue;
            };
            unverified_handle = Some(valid_handle);
            break;
        }
        let Some(unverified_handle) = unverified_handle else {
            return Err("no valid atproto handles in `also_known_as`".to_string());
        };

        // atrium seems to get service endpoint getters
        let Some(pds) = did_doc.get_pds_endpoint() else {
            return Err("no valid pds service found".to_string());
        };

        // TODO can't use atrium's get_signing_key() becuase it fails to check type and controller
        // so if we check those and reject it, we might miss a later valid key in the array
        // (todo is to fix atrium)
        // actually: atrium might be flexible for legacy reps. for now we're rejecting legacy rep.

        // must use the first valid signing key
        let mut signing_key = None;
        let Some(verification_methods) = did_doc.verification_method else {
            return Err("no verification methods found".to_string());
        };
        for method in verification_methods {
            if method.id != format!("{}#atproto", did_doc.id) {
                continue;
            }
            if method.r#type != "Multikey" {
                continue;
            }
            if method.controller != did_doc.id {
                continue;
            }
            let Some(key) = method.public_key_multibase else {
                continue;
            };
            signing_key = Some(key);
            break;
        }
        let Some(signing_key) = signing_key else {
            return Err("no valid atproto signing key found in verification methods".to_string());
        };

        Ok(PartialMiniDoc {
            unverified_handle,
            pds,
            signing_key,
        })
    }
}

/// multi-producer *single-consumer* queue structures (wrap in arc-mutex plz)
///
/// the hashset allows testing for presense of items in the queue.
/// this has absolutely no support for multiple queue consumers.
#[derive(Debug, Default)]
struct RefreshQueue {
    queue: VecDeque<IdentityKey>,
    items: HashSet<IdentityKey>,
}

#[derive(Clone)]
pub struct Identity {
    handle_resolver: Arc<AtprotoHandleResolver<HickoryDnsTxtResolver, DefaultHttpClient>>,
    did_resolver: Arc<CommonDidResolver<DefaultHttpClient>>,
    cache: HybridCache<IdentityKey, IdentityVal>,
    /// multi-producer *single consumer* queue
    refresh_queue: Arc<Mutex<RefreshQueue>>,
    /// just a lock to ensure only one refresher (queue consumer) is running (to be improved with a better refresher)
    refresher_task: Arc<Mutex<()>>,
}

impl Identity {
    pub async fn new(
        cache_dir: impl AsRef<Path>,
        memory_mb: usize,
        disk_gb: usize,
    ) -> Result<Self, IdentityError> {
        let http_client = Arc::new(DefaultHttpClient::default());
        let handle_resolver = AtprotoHandleResolver::new(AtprotoHandleResolverConfig {
            dns_txt_resolver: HickoryDnsTxtResolver::new().unwrap(),
            http_client: http_client.clone(),
        });
        let did_resolver = CommonDidResolver::new(CommonDidResolverConfig {
            plc_directory_url: DEFAULT_PLC_DIRECTORY_URL.to_string(),
            http_client: http_client.clone(),
        });

        let device = FsDeviceBuilder::new(cache_dir)
            .with_capacity(disk_gb * 2_usize.pow(30))
            .build()?;
        let engine = BlockEngineConfig::new(device).with_block_size(2_usize.pow(20)); // note: this does limit the max cached item size

        let cache = HybridCacheBuilder::new()
            .with_name("identity")
            .memory(memory_mb * 2_usize.pow(20))
            .with_weighter(|k: &IdentityKey, v: &IdentityVal| k.weight() + v.weight())
            .storage()
            .with_io_engine_config(PsyncIoEngineConfig::default())
            .with_engine_config(engine)
            .build()
            .await?;

        Ok(Self {
            handle_resolver: Arc::new(handle_resolver),
            did_resolver: Arc::new(did_resolver),
            cache,
            refresh_queue: Default::default(),
            refresher_task: Default::default(),
        })
    }

    /// Resolve (and verify!) an atproto handle to a DID
    ///
    /// The result can be stale
    ///
    /// `None` if the handle can't be found or verification fails
    pub async fn handle_to_did(&self, handle: Handle) -> Result<Option<Did>, IdentityError> {
        let Some(did) = self.handle_to_unverified_did(&handle).await? else {
            return Ok(None);
        };
        let Some(doc) = self.did_to_partial_mini_doc(&did).await? else {
            return Ok(None);
        };
        if doc.unverified_handle != handle {
            return Ok(None);
        }
        Ok(Some(did))
    }

    /// Resolve a DID to a pds url
    ///
    /// This *also* incidentally resolves and verifies the handle, which might
    /// make it slower than expected
    pub async fn did_to_pds(&self, did: Did) -> Result<Option<String>, IdentityError> {
        let Some(mini_doc) = self.did_to_partial_mini_doc(&did).await? else {
            return Ok(None);
        };
        Ok(Some(mini_doc.pds))
    }

    /// Resolve (and cache but **not verify**) a handle to a DID
    async fn handle_to_unverified_did(
        &self,
        handle: &Handle,
    ) -> Result<Option<Did>, IdentityError> {
        let key = IdentityKey::Handle(handle.clone());
        metrics::counter!("slingshot_get_handle").increment(1);
        let entry = self
            .cache
            .get_or_fetch(&key, {
                let handle = handle.clone();
                let resolver = self.handle_resolver.clone();
                || async move {
                    let t0 = Instant::now();
                    let (res, success) = match resolver.resolve(&handle).await {
                        Ok(did) => (
                            Ok(IdentityVal(UtcDateTime::now(), IdentityData::Did(did))),
                            "true",
                        ),
                        Err(atrium_identity::Error::NotFound) => (
                            Ok(IdentityVal(UtcDateTime::now(), IdentityData::NotFound)),
                            "false",
                        ),
                        Err(other) => {
                            log::debug!("other error resolving handle: {other:?}");
                            (Err(IdentityError::ResolutionFailed(other)), "false")
                        }
                    };
                    metrics::histogram!("slingshot_fetch_handle", "success" => success)
                        .record(t0.elapsed());
                    res
                }
            })
            .await?;

        let now = UtcDateTime::now();
        let IdentityVal(last_fetch, data) = entry.value();
        match data {
            IdentityData::Doc(_) => {
                log::error!("identity value mixup: got a doc from a handle key (should be a did)");
                Err(IdentityError::IdentityValTypeMixup(handle.to_string()))
            }
            IdentityData::NotFound => {
                if (now - *last_fetch) >= MIN_NOT_FOUND_TTL {
                    metrics::counter!("identity_handle_refresh_queued", "reason" => "ttl", "found" => "false").increment(1);
                    self.queue_refresh(key).await;
                }
                Ok(None)
            }
            IdentityData::Did(did) => {
                if (now - *last_fetch) >= MIN_TTL {
                    metrics::counter!("identity_handle_refresh_queued", "reason" => "ttl", "found" => "true").increment(1);
                    self.queue_refresh(key).await;
                }
                Ok(Some(did.clone()))
            }
        }
    }

    /// Fetch (and cache) a partial mini doc from a did
    pub async fn did_to_partial_mini_doc(
        &self,
        did: &Did,
    ) -> Result<Option<PartialMiniDoc>, IdentityError> {
        let key = IdentityKey::Did(did.clone());
        metrics::counter!("slingshot_get_did_doc").increment(1);
        let entry = self
            .cache
            .get_or_fetch(&key, {
                let did = did.clone();
                let resolver = self.did_resolver.clone();
                || async move {
                    let t0 = Instant::now();
                    let (res, success) = match resolver.resolve(&did).await {
                        Ok(did_doc) if did_doc.id != did.to_string() => (
                            // TODO: fix in atrium: should verify id is did
                            Err(IdentityError::BadDidDoc(
                                "did doc's id did not match did".to_string(),
                            )),
                            "false",
                        ),
                        Ok(did_doc) => match did_doc.try_into() {
                            Ok(mini_doc) => (
                                Ok(IdentityVal(UtcDateTime::now(), IdentityData::Doc(mini_doc))),
                                "true",
                            ),
                            Err(e) => (Err(IdentityError::BadDidDoc(e)), "false"),
                        },
                        Err(atrium_identity::Error::NotFound) => (
                            Ok(IdentityVal(UtcDateTime::now(), IdentityData::NotFound)),
                            "false",
                        ),
                        Err(other) => (Err(IdentityError::ResolutionFailed(other)), "false"),
                    };
                    metrics::histogram!("slingshot_fetch_did_doc", "success" => success)
                        .record(t0.elapsed());
                    res
                }
            })
            .await?;

        let now = UtcDateTime::now();
        let IdentityVal(last_fetch, data) = entry.value();
        match data {
            IdentityData::Did(_) => {
                log::error!("identity value mixup: got a did from a did key (should be a doc)");
                Err(IdentityError::IdentityValTypeMixup(did.to_string()))
            }
            IdentityData::NotFound => {
                if (now - *last_fetch) >= MIN_NOT_FOUND_TTL {
                    metrics::counter!("identity_did_refresh_queued", "reason" => "ttl", "found" => "false").increment(1);
                    self.queue_refresh(key).await;
                }
                Ok(None)
            }
            IdentityData::Doc(mini_did) => {
                if (now - *last_fetch) >= MIN_TTL {
                    metrics::counter!("identity_did_refresh_queued", "reason" => "ttl", "found" => "true").increment(1);
                    self.queue_refresh(key).await;
                }
                Ok(Some(mini_did.clone()))
            }
        }
    }

    /// put a refresh task on the queue
    ///
    /// this can be safely called from multiple concurrent tasks
    pub async fn queue_refresh(&self, key: IdentityKey) {
        // todo: max queue size
        let mut q = self.refresh_queue.lock().await;
        if !q.items.contains(&key) {
            q.items.insert(key.clone());
            q.queue.push_back(key);
        }
    }

    /// find out what's next in the queue. concurrent consumers are not allowed.
    ///
    /// intent is to leave the item in the queue while refreshing, so that a
    /// producer will not re-add it if it's in progress. there's definitely
    /// better ways to do this, but this is ~simple for as far as a single
    /// consumer can take us.
    ///
    /// we could take it from the queue but leave it in the set and remove from
    /// set later, but splitting them apart feels more bug-prone.
    async fn peek_refresh(&self) -> Option<IdentityKey> {
        let q = self.refresh_queue.lock().await;
        q.queue.front().cloned()
    }

    /// call to clear the latest key from the refresh queue. concurrent consumers not allowed.
    ///
    /// must provide the last peeked refresh queue item as a small safety check
    async fn complete_refresh(&self, key: &IdentityKey) -> Result<(), IdentityError> {
        let mut q = self.refresh_queue.lock().await;

        let Some(queue_key) = q.queue.pop_front() else {
            // gone from queue + since we're in an error condition, make sure it's not stuck in items
            // (not toctou because we have the lock)
            // bolder here than below and removing from items because if the queue is *empty*, then we
            // know it hasn't been re-added since losing sync.
            if q.items.remove(key) {
                log::error!("identity refresh: queue de-sync: not in ");
            } else {
                log::warn!(
                    "identity refresh: tried to complete with wrong key. are multiple queue consumers running?"
                );
            }
            return Err(IdentityError::RefreshQueueKeyError("no key in queue"));
        };

        if queue_key != *key {
            // extra weird case here, what's the most defensive behaviour?
            // we have two keys: ours should have been first but isn't. this shouldn't happen, so let's
            // just leave items alone for it. risks unbounded growth but we're in a bad place already.
            // the other key is the one we just popped. we didn't want it, so maybe we should put it
            // back, BUT if we somehow ended up with concurrent consumers, we have bigger problems. take
            // responsibility for taking it instead: remove it from items as well, and just drop it.
            //
            // hope that whoever calls us takes this error seriously.
            if q.items.remove(&queue_key) {
                log::warn!(
                    "identity refresh: queue de-sync + dropping a bystander key without refreshing it!"
                );
            } else {
                // you thought things couldn't get weirder? (i mean hopefully they can't)
                log::error!("identity refresh: queue de-sync + bystander key also de-sync!?");
            }
            return Err(IdentityError::RefreshQueueKeyError(
                "wrong key at front of queue",
            ));
        }

        if q.items.remove(key) {
            Ok(())
        } else {
            log::error!("identity refresh: queue de-sync: key not in items");
            Err(IdentityError::RefreshQueueKeyError("key not in items"))
        }
    }

    /// run the refresh queue consumer
    pub async fn run_refresher(&self, shutdown: CancellationToken) -> Result<(), IdentityError> {
        let _guard = self
            .refresher_task
            .try_lock()
            .expect("there to only be one refresher running");
        loop {
            if shutdown.is_cancelled() {
                log::info!("identity refresher: exiting for shutdown: closing cache...");
                if let Err(e) = self.cache.close().await {
                    log::error!("cache close errored: {e}");
                } else {
                    log::info!("identity cache closed.")
                }
                return Ok(());
            }
            let Some(task_key) = self.peek_refresh().await else {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                continue;
            };
            match task_key {
                IdentityKey::Handle(ref handle) => {
                    log::trace!("refreshing handle {handle:?}");
                    match self.handle_resolver.resolve(handle).await {
                        Ok(did) => {
                            metrics::counter!("identity_handle_refresh", "success" => "true")
                                .increment(1);
                            self.cache.insert(
                                task_key.clone(),
                                IdentityVal(UtcDateTime::now(), IdentityData::Did(did)),
                            );
                        }
                        Err(atrium_identity::Error::NotFound) => {
                            metrics::counter!("identity_handle_refresh", "success" => "false", "reason" => "not found").increment(1);
                            self.cache.insert(
                                task_key.clone(),
                                IdentityVal(UtcDateTime::now(), IdentityData::NotFound),
                            );
                        }
                        Err(err) => {
                            metrics::counter!("identity_handle_refresh", "success" => "false", "reason" => "other").increment(1);
                            log::warn!(
                                "failed to refresh handle: {err:?}. leaving stale (should we eventually do something?)"
                            );
                        }
                    }
                    self.complete_refresh(&task_key).await?; // failures are bugs, so break loop
                }
                IdentityKey::Did(ref did) => {
                    log::trace!("refreshing did doc: {did:?}");

                    match self.did_resolver.resolve(did).await {
                        Ok(did_doc) => {
                            // TODO: fix in atrium: should verify id is did
                            if did_doc.id != did.to_string() {
                                metrics::counter!("identity_did_refresh", "success" => "false", "reason" => "wrong did").increment(1);
                                log::warn!(
                                    "refreshed did doc failed: wrong did doc id. dropping refresh."
                                );
                                continue;
                            }
                            let mini_doc = match did_doc.try_into() {
                                Ok(md) => md,
                                Err(e) => {
                                    metrics::counter!("identity_did_refresh", "success" => "false", "reason" => "bad doc").increment(1);
                                    log::warn!(
                                        "converting mini doc failed: {e:?}. dropping refresh."
                                    );
                                    continue;
                                }
                            };
                            metrics::counter!("identity_did_refresh", "success" => "true")
                                .increment(1);
                            self.cache.insert(
                                task_key.clone(),
                                IdentityVal(UtcDateTime::now(), IdentityData::Doc(mini_doc)),
                            );
                        }
                        Err(atrium_identity::Error::NotFound) => {
                            metrics::counter!("identity_did_refresh", "success" => "false", "reason" => "not found").increment(1);
                            self.cache.insert(
                                task_key.clone(),
                                IdentityVal(UtcDateTime::now(), IdentityData::NotFound),
                            );
                        }
                        Err(err) => {
                            metrics::counter!("identity_did_refresh", "success" => "false", "reason" => "other").increment(1);
                            log::warn!(
                                "failed to refresh did doc: {err:?}. leaving stale (should we eventually do something?)"
                            );
                        }
                    }

                    self.complete_refresh(&task_key).await?; // failures are bugs, so break loop
                }
            }
        }
    }
}

pub struct HickoryDnsTxtResolver(TokioResolver);

impl HickoryDnsTxtResolver {
    fn new() -> Result<Self, ResolveError> {
        Ok(Self(TokioResolver::builder_tokio()?.build()))
    }
}

impl DnsTxtResolver for HickoryDnsTxtResolver {
    async fn resolve(
        &self,
        query: &str,
    ) -> core::result::Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        match self.0.txt_lookup(query).await {
            Ok(r) => {
                metrics::counter!("whoami_resolve_dns_txt", "success" => "true").increment(1);
                Ok(r.iter().map(|r| r.to_string()).collect())
            }
            Err(e) => {
                metrics::counter!("whoami_resolve_dns_txt", "success" => "false").increment(1);
                Err(e.into())
            }
        }
    }
}
