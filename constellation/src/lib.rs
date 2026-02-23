pub mod consumer;
pub mod server;
pub mod storage;

use links::CollectedLink;
use serde::{Deserialize, Serialize};
use std::convert::From;

#[derive(Debug, PartialEq)]
pub enum ActionableEvent {
    CreateLinks {
        record_id: RecordId,
        links: Vec<CollectedLink>,
    },
    UpdateLinks {
        record_id: RecordId,
        new_links: Vec<CollectedLink>,
    },
    DeleteRecord(RecordId),
    ActivateAccount(Did),
    DeactivateAccount(Did),
    DeleteAccount(Did),
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct Did(pub String);

impl<T: Into<String>> From<T> for Did {
    fn from(s: T) -> Self {
        Self(s.into())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct RecordId {
    pub did: Did,
    pub collection: String,
    pub rkey: String,
}

impl RecordId {
    pub fn did(&self) -> Did {
        self.did.clone()
    }
    pub fn collection(&self) -> String {
        self.collection.clone()
    }
    pub fn rkey(&self) -> String {
        self.rkey.clone()
    }
}

/// maybe the worst type in this repo, and there are some bad types
#[derive(Debug, Serialize, PartialEq)]
pub struct CountsByCount {
    pub records: u64,
    pub distinct_dids: u64,
}
