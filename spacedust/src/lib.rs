pub mod consumer;
pub mod delay;
pub mod error;
pub mod removable_delay_queue;
pub mod server;
pub mod subscriber;

use jetstream::events::CommitEvent;
use microcosm_links::CollectedLink;
use serde::{Deserialize, Serialize};
use server::MultiSubscribeQuery;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug)]
pub struct FilterableProperties {
    /// Full unmodified DID, at-uri, or url
    pub subject: String,
    /// User/identity DID.
    ///
    /// Will match both bare-DIDs and DIDs extracted from at-uris.
    /// `None` for any URL.
    pub subject_did: Option<String>,
    /// Link source -- collection NSID joined with `:` to the record property path.
    pub source: String,
}

/// A serialized message with filterable properties attached
#[derive(Debug)]
pub struct ClientMessage {
    pub message: Message, // always Message::Text
    pub properties: FilterableProperties,
}

impl ClientMessage {
    pub fn new_link(
        link: CollectedLink,
        at_uri: &str,
        commit: &CommitEvent,
    ) -> Result<Self, serde_json::Error> {
        let subject_did = link.target.did();

        let subject = link.target.into_string();

        let undotted = link.path.strip_prefix('.').unwrap_or_else(|| {
            eprintln!("link path did not have expected '.' prefix: {}", link.path);
            ""
        });
        let source = format!("{}:{undotted}", &*commit.collection);

        let client_link_event = ClientLinkEvent {
            operation: "create",
            source: source.clone(),
            source_record: at_uri.to_string(),
            source_rev: commit.rev.to_string(),
            subject: subject.clone(),
        };

        let client_event = ClientEvent {
            kind: "link",
            origin: "live", // TODO: indicate when we're locally replaying jetstream on reconnect?? maybe not.
            link: client_link_event,
        };

        let client_event_json = serde_json::to_string(&client_event)?;

        let message = Message::Text(client_event_json.into());

        let properties = FilterableProperties {
            subject,
            subject_did,
            source,
        };

        Ok(ClientMessage {
            message,
            properties,
        })
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub struct ClientEvent {
    kind: &'static str,   // "link"
    origin: &'static str, // "live", "replay", "backfill"
    link: ClientLinkEvent,
}

#[derive(Debug, Serialize)]
struct ClientLinkEvent {
    operation: &'static str, // "create", "delete" (prob no update, though maybe for rev?)
    source: String,
    source_record: String,
    source_rev: String,
    subject: String,
    // TODO: include the record too? would save clients a level of hydration
    // ^^ no, not for now. until we backfill + support broader deletes at *least*.
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum SubscriberSourcedMessage {
    OptionsUpdate(MultiSubscribeQuery),
}
