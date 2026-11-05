//! Various error types.
use std::io;

use thiserror::Error;

/// Possible errors that can occur when a [JetstreamConfig](crate::JetstreamConfig) that is passed
/// to a [JetstreamConnector](crate::JetstreamConnector) is invalid.
#[derive(Error, Debug)]
pub enum ConfigValidationError {
    #[error("too many wanted collections: {0} > 100")]
    TooManyWantedCollections(usize),
    #[error("too many wanted DIDs: {0} > 10,000")]
    TooManyDids(usize),
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(#[from] url::ParseError),
}

/// Possible errors that can occur in the process of connecting to a Jetstream instance over
/// WebSockets.
///
/// See [JetstreamConnector::connect](crate::JetstreamConnector::connect).
#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("invalid endpoint: {0}")]
    InvalidEndpoint(#[from] url::ParseError),
    #[error("failed to construct url: {0}")]
    InvalidEndpointUri(#[from] tokio_tungstenite::tungstenite::http::uri::InvalidUri),
    #[error("failed to connect to Jetstream instance: {0}")]
    WebSocketFailure(Box<tokio_tungstenite::tungstenite::Error>),
    #[error("the Jetstream config is invalid (this really should not happen here): {0}")]
    InvalidConfig(#[from] ConfigValidationError),
}

impl From<tokio_tungstenite::tungstenite::Error> for ConnectionError {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::WebSocketFailure(Box::new(e))
    }
}

/// Possible errors that can occur when receiving events from a Jetstream instance over WebSockets.
///
/// See [websocket_task](crate::websocket_task).
#[derive(Error, Debug)]
pub enum JetstreamEventError {
    #[error("failed to load built-in zstd dictionary for decoding: {0}")]
    CompressionDictionaryError(io::Error),
    #[error("failed to send ping or pong: {0}")]
    PingPongError(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("no messages received within ttl")]
    NoMessagesReceived,
    #[error("jetstream event receiver closed")]
    ReceiverClosedError,
}
