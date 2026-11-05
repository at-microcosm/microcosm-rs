use crate::ErrorResponseObject;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConsumerError {
    #[error(transparent)]
    JetstreamConnectionError(#[from] jetstream::error::ConnectionError),
    #[error(transparent)]
    JetstreamConfigValidationError(#[from] jetstream::error::ConfigValidationError),
    #[error("jetstream ended")]
    JetstreamEnded,
    #[error("delay queue output dropped")]
    DelayQueueOutputDropped,
}

#[derive(Debug, Error)]
pub enum ServerError {
    #[error("server build error: {0}")]
    AcmeBuildError(std::io::Error),
    #[error("server exited: {0}")]
    ServerExited(std::io::Error),
}

#[derive(Debug, Error)]
pub enum IdentityError {
    #[error("whatever: {0}")]
    WhateverError(String),
    #[error("bad DID: {0}")]
    BadDid(&'static str),
    #[error("identity types got mixed up: {0}")]
    IdentityValTypeMixup(String),
    #[error("foyer error: {0}")]
    FoyerError(#[from] foyer::Error),

    #[error("failed to resolve: {0}")]
    ResolutionFailed(#[from] atrium_identity::Error),
    // #[error("identity resolved but no handle found for user")]
    // NoHandle,
    #[error("found handle {0:?} but it appears invalid: {1}")]
    InvalidHandle(String, &'static str),

    #[error("wrong key for clearing refresh queue: {0}")]
    RefreshQueueKeyError(&'static str),
}

#[derive(Debug, Error)]
pub enum HealthCheckError {
    #[error("failed to send checkin: {0}")]
    HealthCheckError(#[from] reqwest::Error),
}

#[derive(Debug, Error)]
pub enum MainTaskError {
    #[error(transparent)]
    ConsumerTaskError(#[from] ConsumerError),
    #[error(transparent)]
    ServerTaskError(#[from] ServerError),
    #[error(transparent)]
    IdentityTaskError(#[from] IdentityError),
    #[error(transparent)]
    HealthCheckError(#[from] HealthCheckError),
    #[error("firehose cache failed to close: {0}")]
    FirehoseCacheCloseError(foyer::Error),
    #[error("identity cache failed to close: {0}")]
    IdentityCacheCloseError(foyer::Error),
}

#[derive(Debug, Error)]
pub enum RecordError {
    #[error("identity error: {0}")]
    IdentityError(#[from] IdentityError),
    #[error("repo could not be validated as either a DID or an atproto handle")]
    BadRepo,
    #[error("could not get record: {0}")]
    NotFound(&'static str),
    #[error("could nto parse pds url: {0}")]
    UrlParseError(#[from] url::ParseError),
    #[error("reqwest send failed: {0}")]
    SendError(reqwest::Error),
    #[error("reqwest raised for status: {0}")]
    StatusError(reqwest::Error),
    #[error("reqwest failed to parse json: {0}")]
    ParseJsonError(reqwest::Error),
    #[error("upstream getRecord did not include a CID")]
    MissingUpstreamCid,
    #[error("upstream CID was not valid: {0}")]
    BadUpstreamCid(String),
    #[error("upstream atproto-looking bad request")]
    UpstreamBadRequest(ErrorResponseObject),
    #[error("upstream non-atproto bad request")]
    UpstreamBadBadNotGoodRequest(reqwest::Error),
}

#[derive(Debug, Error)]
pub enum ProxyError {
    #[error("failed to parse path: {0}")]
    PathParseError(String),
    #[error(transparent)]
    UrlParseError(#[from] url::ParseError),
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),
    #[error(transparent)]
    InvalidHeader(#[from] reqwest::header::InvalidHeaderValue),
    #[error(transparent)]
    IdentityError(#[from] IdentityError),
    #[error("upstream service could not be resolved")]
    ServiceNotFound,
    #[error("upstream service was found but no services matched")]
    ServiceNotMatched,
}
