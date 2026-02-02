//! cached record storage

use crate::{Identity, error::RecordError};
use atrium_api::types::string::{Cid, Did, Nsid, RecordKey};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::str::FromStr;
use std::time::Duration;
use url::Url;

#[derive(Debug, Serialize, Deserialize)]
pub struct RawRecord {
    cid: Cid,
    record: String,
}

// TODO: should be able to do typed CID
impl From<(Cid, Box<RawValue>)> for RawRecord {
    fn from((cid, rv): (Cid, Box<RawValue>)) -> Self {
        Self {
            cid,
            record: rv.get().to_string(),
        }
    }
}

/// only for use with stored (validated) values, not general strings
impl From<&RawRecord> for (Cid, Box<RawValue>) {
    fn from(RawRecord { cid, record }: &RawRecord) -> Self {
        (
            cid.clone(),
            RawValue::from_string(record.to_string())
                .expect("stored string from RawValue to be valid"),
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CachedRecord {
    Found(RawRecord),
    Deleted,
}

impl CachedRecord {
    pub(crate) fn weight(&self) -> usize {
        let wrapping = std::mem::size_of::<Self>();
        let inner = match self {
            CachedRecord::Found(RawRecord { record, .. }) => std::mem::size_of_val(record.as_str()),
            _ => 0,
        };
        wrapping + inner
    }
}

//////// upstream record fetching

#[derive(Deserialize)]
struct RecordResponseObject {
    #[allow(dead_code)] // expect it to be there but we ignore it
    uri: String,
    /// CID for this exact version of the record
    ///
    /// this is optional in the spec and that's potentially TODO for slingshot
    cid: Option<String>,
    /// the record itself as JSON
    value: Box<RawValue>,
}

#[derive(Debug, Deserialize)]
pub struct ErrorResponseObject {
    pub error: String,
    pub message: String,
}

#[derive(Clone)]
pub struct Repo {
    identity: Identity,
    client: Client,
}

impl Repo {
    pub fn new(identity: Identity) -> Self {
        let client = Client::builder()
            .user_agent(format!(
                "microcosm slingshot v{} (dev: @bad-example.com)",
                env!("CARGO_PKG_VERSION")
            ))
            .no_proxy()
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap();
        Repo { identity, client }
    }

    pub async fn get_record(
        &self,
        did: &Did,
        collection: &Nsid,
        rkey: &RecordKey,
        cid: &Option<Cid>,
    ) -> Result<CachedRecord, RecordError> {
        let Some(pds) = self.identity.did_to_pds(did.clone()).await? else {
            return Err(RecordError::NotFound("could not get pds for DID"));
        };

        // cid gets set to None for a retry, if it's Some and we got NotFound
        let mut cid = cid;

        let res = loop {
            // TODO: throttle outgoing requests by host probably, generally guard against outgoing requests
            let mut params = vec![
                ("repo", did.to_string()),
                ("collection", collection.to_string()),
                ("rkey", rkey.to_string()),
            ];
            if let Some(cid) = cid {
                params.push(("cid", cid.as_ref().to_string()));
            }
            let mut url = Url::parse_with_params(&pds, &params)?;
            url.set_path("/xrpc/com.atproto.repo.getRecord");

            let res = self
                .client
                .get(url.clone())
                .send()
                .await
                .map_err(RecordError::SendError)?;

            if res.status() == StatusCode::BAD_REQUEST {
                // 1. if we're not able to parse json, it's not something we can handle
                let err = res
                    .json::<ErrorResponseObject>()
                    .await
                    .map_err(RecordError::UpstreamBadBadNotGoodRequest)?;
                // 2. if we are, is it a NotFound? and if so, did we try with a CID?
                // if so, retry with no CID (api handler will reject for mismatch but
                // with a nice error + warm cache)
                if err.error == "NotFound" && cid.is_some() {
                    cid = &None;
                    continue;
                } else {
                    return Err(RecordError::UpstreamBadRequest(err));
                }
            }
            break res;
        };

        let data = res
            .error_for_status()
            .map_err(RecordError::StatusError)? // TODO atproto error handling (think about handling not found)
            .json::<RecordResponseObject>()
            .await
            .map_err(RecordError::ParseJsonError)?; // todo...

        let Some(cid) = data.cid else {
            return Err(RecordError::MissingUpstreamCid);
        };
        let cid = Cid::from_str(&cid).map_err(|e| RecordError::BadUpstreamCid(e.to_string()))?;

        Ok(CachedRecord::Found(RawRecord {
            cid,
            record: data.value.to_string(),
        }))
    }
}
