use dashmap::DashMap;
use rand::{Rng, distr::Alphanumeric};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::{JoinHandle, spawn};
use tokio::time::sleep;
use tokio_util::sync::{CancellationToken, DropGuard};

pub struct ExpiringTaskMap<T>(TaskMap<T>);

/// need to manually implement clone because T is allowed to not be clone
impl<T> Clone for ExpiringTaskMap<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: Send + 'static> ExpiringTaskMap<T> {
    pub fn new(expiration: Duration) -> Self {
        let map = TaskMap {
            map: Arc::new(DashMap::new()),
            expiration,
        };
        Self(map)
    }

    pub fn dispatch<F>(&self, task: F, cancel: CancellationToken) -> String
    where
        F: Future<Output = T> + Send + 'static,
    {
        let TaskMap {
            ref map,
            expiration,
        } = self.0;
        let task_key: String = rand::rng()
            .sample_iter(&Alphanumeric)
            .take(24)
            .map(char::from)
            .collect();

        // spawn a tokio task and put the join handle in the map for later retrieval
        map.insert(task_key.clone(), (cancel.clone().drop_guard(), spawn(task)));

        // spawn a second task to clean up the map in case it doesn't get claimed
        let k = task_key.clone();
        let map = map.clone();
        spawn(async move {
            if cancel
                .run_until_cancelled(sleep(expiration))
                .await
                .is_some()
            // the (sleep) task completed first
            {
                map.remove(&k);
                cancel.cancel();
                metrics::counter!("whoami_task_map_completions", "result" => "expired")
                    .increment(1);
            }
        });

        task_key
    }

    pub fn take(&self, key: &str) -> Option<JoinHandle<T>> {
        if let Some((_key, (_guard, handle))) = self.0.map.remove(key) {
            // when the _guard drops, it cancels the token for us
            metrics::counter!("whoami_task_map_completions", "result" => "retrieved").increment(1);
            Some(handle)
        } else {
            metrics::counter!("whoami_task_map_gones").increment(1);
            None
        }
    }
}

struct TaskMap<T> {
    map: Arc<DashMap<String, (DropGuard, JoinHandle<T>)>>,
    expiration: Duration,
}

/// need to manually implement clone because T is allowed to not be clone
impl<T> Clone for TaskMap<T> {
    fn clone(&self) -> Self {
        Self {
            map: self.map.clone(),
            expiration: self.expiration,
        }
    }
}
