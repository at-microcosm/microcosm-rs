use super::{
    LinkReader, LinkStorage, Order, PagedAppendingCollection, PagedOrderedCollection, StorageStats,
};
use crate::{ActionableEvent, CountsByCount, Did, RecordId};
use anyhow::Result;
use links::CollectedLink;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

// hopefully-correct simple hashmap version, intended only for tests to verify disk impl
#[derive(Debug, Clone)]
pub struct MemStorage(Arc<Mutex<MemStorageData>>);

type Linkers = Vec<Option<(Did, RKey)>>; // optional because we replace with None for deleted links to keep cursors stable

#[derive(Debug, Default)]
struct MemStorageData {
    dids: HashMap<Did, bool>,                           // bool: active or nah
    targets: HashMap<Target, HashMap<Source, Linkers>>, // target -> (collection, path) -> (did, rkey)?[]
    links: HashMap<Did, HashMap<RepoId, Vec<(RecordPath, Target)>>>, // did -> collection:rkey -> (path, target)[]
}

impl MemStorage {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(MemStorageData::default())))
    }

    fn add_links(&mut self, record_id: &RecordId, links: &[CollectedLink]) {
        let mut data = self.0.lock().unwrap();
        for link in links {
            data.dids.entry(record_id.did()).or_insert(true); // if they are inserting a link, presumably they are active
            data.targets
                .entry(Target::new(link.target.as_str()))
                .or_default()
                .entry(Source::new(&record_id.collection, &link.path))
                .or_default()
                .push(Some((record_id.did(), RKey(record_id.rkey()))));
            data.links
                .entry(record_id.did())
                .or_default()
                .entry(RepoId::from_record_id(record_id))
                .or_insert(Vec::with_capacity(1))
                .push((
                    RecordPath::new(&link.path),
                    Target::new(link.target.as_str()),
                ))
        }
    }

    fn remove_links(&mut self, record_id: &RecordId) {
        let mut data = self.0.lock().unwrap();
        let repo_id = RepoId::from_record_id(record_id);
        if let Some(Some(link_targets)) = data.links.get(&record_id.did).map(|cr| cr.get(&repo_id))
        {
            let link_targets = link_targets.clone(); // satisfy borrowck
            for (record_path, target) in link_targets {
                data.targets
                    .get_mut(&target)
                    .expect("must have the target if we have a link saved")
                    .get_mut(&Source::new(&record_id.collection, &record_path.0))
                    .expect("must have the target at this path if we have a link to it saved")
                    .iter_mut()
                    .rfind(|d| **d == Some((record_id.did(), RKey(record_id.rkey()))))
                    .expect("must be in dids list if we have a link to it")
                    .take();
            }
        }
        data.links
            .get_mut(&record_id.did)
            .map(|cr| cr.remove(&repo_id));
    }

    fn update_links(&mut self, record_id: &RecordId, new_links: &[CollectedLink]) {
        self.remove_links(record_id);
        self.add_links(record_id, new_links);
    }

    fn set_account(&mut self, did: &Did, active: bool) {
        let mut data = self.0.lock().unwrap();
        if let Some(account) = data.dids.get_mut(did) {
            *account = active;
        }
    }

    fn delete_account(&mut self, did: &Did) {
        let mut data = self.0.lock().unwrap();
        if let Some(links) = data.links.get(did) {
            let links = links.clone();
            for (repo_id, targets) in links {
                let targets = targets.clone();
                for (record_path, target) in targets {
                    data.targets
                        .get_mut(&target)
                        .expect("must have the target if we have a link saved")
                        .get_mut(&Source::new(&repo_id.collection, &record_path.0))
                        .expect("must have the target at this path if we have a link to it saved")
                        .iter_mut()
                        .find(|d| **d == Some((did.clone(), repo_id.rkey.clone())))
                        .expect("lkasjdlfkj")
                        .take();
                }
            }
        }
        data.links.remove(did); // nb: this is removing by a whole prefix in kv context
        data.dids.remove(did);
    }
}

impl Default for MemStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl LinkStorage for MemStorage {
    fn push(&mut self, event: &ActionableEvent, _cursor: u64) -> Result<()> {
        match event {
            ActionableEvent::CreateLinks { record_id, links } => self.add_links(record_id, links),
            ActionableEvent::UpdateLinks {
                record_id,
                new_links,
            } => self.update_links(record_id, new_links),
            ActionableEvent::DeleteRecord(record_id) => self.remove_links(record_id),
            ActionableEvent::ActivateAccount(did) => self.set_account(did, true),
            ActionableEvent::DeactivateAccount(did) => self.set_account(did, false),
            ActionableEvent::DeleteAccount(did) => self.delete_account(did),
        }
        Ok(())
    }

    fn to_readable(&mut self) -> impl LinkReader {
        self.clone()
    }
}

impl LinkReader for MemStorage {
    fn get_many_to_many_counts(
        &self,
        target: &str,
        collection: &str,
        path: &str,
        path_to_other: &str,
        limit: u64,
        after: Option<String>,
        filter_dids: &HashSet<Did>,
        filter_to_targets: &HashSet<String>,
    ) -> Result<PagedOrderedCollection<(String, u64, u64), String>> {
        let data = self.0.lock().unwrap();
        let Some(paths) = data.targets.get(&Target::new(target)) else {
            return Ok(PagedOrderedCollection::empty());
        };
        let Some(linkers) = paths.get(&Source::new(collection, path)) else {
            return Ok(PagedOrderedCollection::empty());
        };

        let path_to_other = RecordPath::new(path_to_other);
        let filter_to_targets: HashSet<Target> =
            HashSet::from_iter(filter_to_targets.iter().map(|s| Target::new(s)));

        let mut grouped_counts: HashMap<Target, (u64, HashSet<Did>)> = HashMap::new();
        for (did, rkey) in linkers.iter().flatten().cloned() {
            if !filter_dids.is_empty() && !filter_dids.contains(&did) {
                continue;
            }
            if let Some(fwd_target) = data
                .links
                .get(&did)
                .unwrap_or(&HashMap::new())
                .get(&RepoId {
                    collection: collection.to_string(),
                    rkey,
                })
                .unwrap_or(&Vec::new())
                .iter()
                .filter_map(|(path, target)| {
                    if *path == path_to_other
                        && (filter_to_targets.is_empty() || filter_to_targets.contains(target))
                    {
                        Some(target)
                    } else {
                        None
                    }
                })
                .take(1)
                .next()
            {
                let e = grouped_counts.entry(fwd_target.clone()).or_default();
                e.0 += 1;
                e.1.insert(did.clone());
            }
        }
        let mut items: Vec<(String, u64, u64)> = grouped_counts
            .iter()
            .map(|(k, (n, u))| (k.0.clone(), *n, u.len() as u64))
            .collect();
        items.sort();
        items = items
            .into_iter()
            .skip_while(|(t, _, _)| after.as_ref().map(|a| t <= a).unwrap_or(false))
            .take(limit as usize)
            .collect();
        let next = if items.len() as u64 >= limit {
            items.last().map(|(t, _, _)| t.clone())
        } else {
            None
        };
        Ok(PagedOrderedCollection { items, next })
    }

    fn get_count(&self, target: &str, collection: &str, path: &str) -> Result<u64> {
        let data = self.0.lock().unwrap();
        let Some(paths) = data.targets.get(&Target::new(target)) else {
            return Ok(0);
        };
        let Some(linkers) = paths.get(&Source::new(collection, path)) else {
            return Ok(0);
        };
        Ok(linkers.iter().flatten().count() as u64)
    }

    fn get_distinct_did_count(&self, target: &str, collection: &str, path: &str) -> Result<u64> {
        let data = self.0.lock().unwrap();
        let Some(paths) = data.targets.get(&Target::new(target)) else {
            return Ok(0);
        };
        let Some(linkers) = paths.get(&Source::new(collection, path)) else {
            return Ok(0);
        };
        Ok(linkers
            .iter()
            .flatten()
            .map(|(did, _)| did)
            .collect::<HashSet<_>>()
            .len() as u64)
    }

    fn get_many_to_many(
        &self,
        target: &str,
        collection: &str,
        path: &str,
        path_to_other: &str,
        limit: u64,
        after: Option<String>,
        filter_dids: &HashSet<Did>,
        filter_to_targets: &HashSet<String>,
    ) -> Result<PagedOrderedCollection<(String, Vec<RecordId>), String>> {
        let empty_res = Ok(PagedOrderedCollection {
            items: Vec::new(),
            next: None,
        });

        // struct MemStorageData {
        //     dids: HashMap<Did, bool>,
        //     targets: HashMap<Target, HashMap<Source, Linkers>>,
        //     links: HashMap<Did, HashMap<RepoId, Vec<(RecordPath, Target)>>>,
        // }
        let data = self.0.lock().unwrap();

        let Some(sources) = data.targets.get(&Target::new(target)) else {
            return empty_res;
        };
        let Some(linkers) = sources.get(&Source::new(collection, path)) else {
            return empty_res;
        };
        let path_to_other = RecordPath::new(path_to_other);

        // Convert filter_to_targets to Target objects for comparison
        let filter_to_target_objs: HashSet<Target> =
            HashSet::from_iter(filter_to_targets.iter().map(|s| Target::new(s)));

        let mut grouped_links: HashMap<Target, Vec<RecordId>> = HashMap::new();
        for (did, rkey) in linkers.iter().flatten().cloned() {
            // Filter by DID if filter is provided
            if !filter_dids.is_empty() && !filter_dids.contains(&did) {
                continue;
            }
            if let Some(fwd_target) = data
                .links
                .get(&did)
                .unwrap_or(&HashMap::new())
                .get(&RepoId {
                    collection: collection.to_string(),
                    rkey: rkey.clone(),
                })
                .unwrap_or(&Vec::new())
                .iter()
                .find_map(|(path, target)| {
                    if *path == path_to_other
                        && (filter_to_target_objs.is_empty()
                            || filter_to_target_objs.contains(target))
                    {
                        Some(target)
                    } else {
                        None
                    }
                })
            {
                let record_ids = grouped_links.entry(fwd_target.clone()).or_default();
                record_ids.push(RecordId {
                    did,
                    collection: collection.to_string(),
                    rkey: rkey.0,
                });
            }
        }

        let mut items = grouped_links
            .into_iter()
            .map(|(t, r)| (t.0, r))
            .collect::<Vec<_>>();

        items.sort_by(|(a, _), (b, _)| a.cmp(b));

        items = items
            .into_iter()
            .skip_while(|(t, _)| after.as_ref().map(|a| t <= a).unwrap_or(false))
            .take(limit as usize)
            .collect();

        let next = if items.len() as u64 >= limit {
            items.last().map(|(t, _)| t.clone())
        } else {
            None
        };

        Ok(PagedOrderedCollection { items, next })
    }

    fn get_links(
        &self,
        target: &str,
        collection: &str,
        path: &str,
        order: Order,
        limit: u64,
        until: Option<u64>, // paged iteration endpoint
        filter_dids: &HashSet<Did>,
    ) -> Result<PagedAppendingCollection<RecordId>> {
        let data = self.0.lock().unwrap();
        let Some(paths) = data.targets.get(&Target::new(target)) else {
            return Ok(PagedAppendingCollection::empty());
        };
        let Some(did_rkeys) = paths.get(&Source::new(collection, path)) else {
            return Ok(PagedAppendingCollection::empty());
        };

        let did_rkeys: Vec<_> = if !filter_dids.is_empty() {
            did_rkeys
                .iter()
                .filter(|m| {
                    Option::<(Did, RKey)>::clone(m)
                        .map(|(did, _)| filter_dids.contains(&did))
                        .unwrap_or(false)
                })
                .cloned()
                .collect()
        } else {
            did_rkeys.to_vec()
        };

        let total = did_rkeys.len() as u64;

        // backlinks are stored oldest-to-newest (ascending index with increasing age)
        let (start, take, next_until) = match order {
            Order::OldestToNewest => {
                let start = until.unwrap_or(0);
                let next = start + limit + 1;
                let next_until = if next < total { Some(next) } else { None };
                (start, limit, next_until)
            }
            Order::NewestToOldest => {
                let until = until.unwrap_or(total);
                match until.checked_sub(limit) {
                    Some(s) if s > 0 => (s, limit, Some(s)),
                    Some(s) => (s, limit, None),
                    None => (0, until, None),
                }
            }
        };

        let alive = did_rkeys.iter().flatten().count() as u64;
        let gone = total - alive;

        let items = did_rkeys
            .iter()
            .skip(start as usize)
            .take(take as usize)
            .flatten()
            .filter(|(did, _)| *data.dids.get(did).expect("did must be in dids"))
            .map(|(did, rkey)| RecordId {
                did: did.clone(),
                rkey: rkey.0.clone(),
                collection: collection.to_string(),
            });

        let items: Vec<_> = match order {
            Order::OldestToNewest => items.collect(), // links are stored oldest first
            Order::NewestToOldest => items.rev().collect(),
        };

        Ok(PagedAppendingCollection {
            version: (total, gone),
            items,
            next: next_until,
            total: alive,
        })
    }

    fn get_distinct_dids(
        &self,
        target: &str,
        collection: &str,
        path: &str,
        limit: u64,
        until: Option<u64>,
    ) -> Result<PagedAppendingCollection<Did>> {
        let data = self.0.lock().unwrap();
        let Some(paths) = data.targets.get(&Target::new(target)) else {
            return Ok(PagedAppendingCollection::empty());
        };
        let Some(did_rkeys) = paths.get(&Source::new(collection, path)) else {
            return Ok(PagedAppendingCollection::empty());
        };

        let dids: Vec<Option<Did>> = {
            let mut seen = HashSet::new();
            did_rkeys
                .iter()
                .map(|o| {
                    o.clone().and_then(|(did, _)| {
                        if seen.contains(&did) {
                            None
                        } else {
                            seen.insert(did.clone());
                            Some(did)
                        }
                    })
                })
                .collect()
        };

        let total = dids.len() as u64;
        let until = until.unwrap_or(total);
        let (start, take, next_until) = match until.checked_sub(limit) {
            Some(s) if s > 0 => (s, limit, Some(s)),
            Some(s) => (s, limit, None),
            None => (0, until, None),
        };

        let alive = dids.iter().flatten().count() as u64;
        let gone = total - alive;

        let items: Vec<Did> = dids
            .iter()
            .skip(start as usize)
            .take(take as usize)
            .rev()
            .flatten()
            .filter(|did| *data.dids.get(did).expect("did must be in dids"))
            .cloned()
            .collect();

        Ok(PagedAppendingCollection {
            version: (total, gone),
            items,
            next: next_until,
            total: alive,
        })
    }

    fn get_all_record_counts(&self, target: &str) -> Result<HashMap<String, HashMap<String, u64>>> {
        let data = self.0.lock().unwrap();
        let mut out: HashMap<String, HashMap<String, u64>> = HashMap::new();
        if let Some(asdf) = data.targets.get(&Target::new(target)) {
            for (Source { collection, path }, linkers) in asdf {
                let count = linkers.iter().flatten().count() as u64;
                out.entry(collection.to_string())
                    .or_default()
                    .insert(path.to_string(), count);
            }
        }
        Ok(out)
    }

    fn get_all_counts(
        &self,
        target: &str,
    ) -> Result<HashMap<String, HashMap<String, CountsByCount>>> {
        let data = self.0.lock().unwrap();
        let mut out: HashMap<String, HashMap<String, CountsByCount>> = HashMap::new();
        if let Some(asdf) = data.targets.get(&Target::new(target)) {
            for (Source { collection, path }, linkers) in asdf {
                let records = linkers.iter().flatten().count() as u64;
                let distinct_dids = linkers
                    .iter()
                    .flatten()
                    .map(|(did, _)| did)
                    .collect::<HashSet<_>>()
                    .len() as u64;
                out.entry(collection.to_string()).or_default().insert(
                    path.to_string(),
                    CountsByCount {
                        records,
                        distinct_dids,
                    },
                );
            }
        }
        Ok(out)
    }

    fn get_stats(&self) -> Result<StorageStats> {
        let data = self.0.lock().unwrap();
        let dids = data.dids.len() as u64;
        let targetables = data
            .targets
            .values()
            .map(|sources| sources.len())
            .sum::<usize>() as u64;
        let linking_records = data.links.values().map(|recs| recs.len()).sum::<usize>() as u64;
        Ok(StorageStats {
            dids,
            targetables,
            linking_records,
            started_at: None,
            other_data: Default::default(),
        })
    }
}

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
struct Target(String);

impl Target {
    fn new(t: &str) -> Self {
        Self(t.into())
    }
}

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
struct Source {
    collection: String,
    path: String,
}

impl Source {
    fn new(collection: &str, path: &str) -> Self {
        Self {
            collection: collection.into(),
            path: path.into(),
        }
    }
}

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
struct RKey(String);

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
struct RepoId {
    collection: String,
    rkey: RKey,
}

impl RepoId {
    fn from_record_id(record_id: &RecordId) -> Self {
        Self {
            collection: record_id.collection.clone(),
            rkey: RKey(record_id.rkey.clone()),
        }
    }
}

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
struct RecordPath(String);

impl RecordPath {
    fn new(rp: &str) -> Self {
        Self(rp.into())
    }
}
