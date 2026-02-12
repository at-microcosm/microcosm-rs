use crate::{
    CachedRecord, ErrorResponseObject, Identity, Proxy, Repo,
    error::{RecordError, ServerError},
    proxy::{FullAtUriParts, MatchedRef, extract_links, split_uri},
    record::RawRecord,
};
use atrium_api::types::string::{AtIdentifier, Cid, Did, Handle, Nsid, RecordKey};
use foyer::HybridCache;
use links::at_uri::parse_at_uri as normalize_at_uri;
use serde::Serialize;
use std::{collections::HashMap, path::PathBuf, str::FromStr, sync::Arc, time::Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use poem::{
    Endpoint, EndpointExt, IntoResponse, Route, RouteScheme, Server,
    endpoint::{StaticFileEndpoint, make_sync},
    http::Method,
    listener::{
        Listener, TcpListener,
        acme::{AutoCert, ChallengeType, LETS_ENCRYPT_PRODUCTION, LETS_ENCRYPT_STAGING},
    },
    middleware::{CatchPanic, Cors, Tracing},
};
use poem_openapi::{
    ApiResponse, ContactObject, ExternalDocumentObject, Object, OpenApi, OpenApiService, Tags,
    Union, param::Query, payload::Json, types::Example,
};

fn example_handle() -> String {
    "bad-example.com".to_string()
}
fn example_did() -> String {
    "did:plc:hdhoaan3xa3jiuq4fg4mefid".to_string()
}
fn example_service_did() -> String {
    "did:web:constellation.microcosm.blue".to_string()
}
fn example_collection() -> String {
    "app.bsky.feed.like".to_string()
}
fn example_rkey() -> String {
    "3lv4ouczo2b2a".to_string()
}
fn example_id_fragment() -> String {
    "#constellation".to_string()
}
fn example_uri() -> String {
    format!(
        "at://{}/{}/{}",
        example_did(),
        example_collection(),
        example_rkey()
    )
}
fn example_pds() -> String {
    "https://porcini.us-east.host.bsky.network".to_string()
}
fn example_signing_key() -> String {
    "zQ3shpq1g134o7HGDb86CtQFxnHqzx5pZWknrVX2Waum3fF6j".to_string()
}

#[derive(Debug, Object)]
#[oai(example = true)]
struct XrpcErrorResponseObject {
    /// Should correspond an error `name` in the lexicon errors array
    error: String,
    /// Human-readable description and possibly additonal context
    message: String,
}
impl Example for XrpcErrorResponseObject {
    fn example() -> Self {
        Self {
            error: "RecordNotFound".to_string(),
            message: "This record was deleted".to_string(),
        }
    }
}
type XrpcError = Json<XrpcErrorResponseObject>;
fn xrpc_error(error: impl AsRef<str>, message: impl AsRef<str>) -> XrpcError {
    Json(XrpcErrorResponseObject {
        error: error.as_ref().to_string(),
        message: message.as_ref().to_string(),
    })
}

fn bad_request_handler_get_record(err: poem::Error) -> GetRecordResponse {
    GetRecordResponse::BadRequest(Json(XrpcErrorResponseObject {
        error: "InvalidRequest".to_string(),
        message: format!("Bad request, here's some info that maybe should not be exposed: {err}"),
    }))
}

fn bad_request_handler_resolve_mini(err: poem::Error) -> ResolveMiniDocResponse {
    ResolveMiniDocResponse::BadRequest(Json(XrpcErrorResponseObject {
        error: "InvalidRequest".to_string(),
        message: format!("Bad request, here's some info that maybe should not be exposed: {err}"),
    }))
}

fn bad_request_handler_resolve_service(err: poem::Error) -> ResolveServiceResponse {
    ResolveServiceResponse::BadRequest(Json(XrpcErrorResponseObject {
        error: "InvalidRequest".to_string(),
        message: format!("Bad request, here's some info that maybe should not be exposed: {err}"),
    }))
}

fn bad_request_handler_proxy_query(err: poem::Error) -> ProxyHydrateResponse {
    ProxyHydrateResponse::BadRequest(Json(XrpcErrorResponseObject {
        error: "InvalidRequest".to_string(),
        message: format!("Bad request, here's some info that maybe should not be exposed: {err}"),
    }))
}

fn bad_request_handler_resolve_handle(err: poem::Error) -> JustDidResponse {
    JustDidResponse::BadRequest(Json(XrpcErrorResponseObject {
        error: "InvalidRequest".to_string(),
        message: format!("Bad request, here's some info that maybe should not be exposed: {err}"),
    }))
}

#[derive(Object)]
#[oai(example = true)]
struct FoundRecordResponseObject {
    /// at-uri for this record
    uri: String,
    /// CID for this exact version of the record
    ///
    /// Slingshot will always return the CID, despite it not being a required
    /// response property in the official lexicon.
    ///
    /// TODO: probably actually let it be optional, idk are some pds's weirdly
    /// not returning it?
    cid: Option<String>,
    /// the record itself as JSON
    value: serde_json::Value,
}
impl Example for FoundRecordResponseObject {
    fn example() -> Self {
        Self {
            uri: example_uri(),
            cid: Some("bafyreialv3mzvvxaoyrfrwoer3xmabbmdchvrbyhayd7bga47qjbycy74e".to_string()),
            value: serde_json::json!({
                "$type": "app.bsky.feed.like",
                "createdAt": "2025-07-29T18:02:02.327Z",
                "subject": {
                  "cid": "bafyreia2gy6eyk5qfetgahvshpq35vtbwy6negpy3gnuulcdi723mi7vxy",
                  "uri": "at://did:plc:vwzwgnygau7ed7b7wt5ux7y2/app.bsky.feed.post/3lv4lkb4vgs2k"
                }
            }),
        }
    }
}

#[derive(ApiResponse)]
#[oai(bad_request_handler = "bad_request_handler_get_record")]
enum GetRecordResponse {
    /// Record found
    #[oai(status = 200)]
    Ok(Json<FoundRecordResponseObject>),
    /// Bad request or no record to return
    ///
    /// The only error name in the repo.getRecord lexicon is `RecordNotFound`,
    /// but the [canonical api docs](https://docs.bsky.app/docs/api/com-atproto-repo-get-record)
    /// also list `InvalidRequest`, `ExpiredToken`, and `InvalidToken`. Of
    /// these, slingshot will only generate `RecordNotFound` or `InvalidRequest`,
    /// but may return any proxied error code from the upstream repo.
    #[oai(status = 400)]
    BadRequest(XrpcError),
    /// Server errors
    #[oai(status = 500)]
    ServerError(XrpcError),
}

#[derive(Object)]
#[oai(example = true)]
struct MiniDocResponseObject {
    /// DID, bi-directionally verified if a handle was provided in the query.
    did: String,
    /// The validated handle of the account or `handle.invalid` if the handle
    /// did not bi-directionally match the DID document.
    handle: String,
    /// The identity's PDS URL
    pds: String,
    /// The atproto signing key publicKeyMultibase
    ///
    /// Legacy key encoding not supported. the key is returned directly; `id`,
    /// `type`, and `controller` are omitted.
    signing_key: String,
}
impl Example for MiniDocResponseObject {
    fn example() -> Self {
        Self {
            did: example_did(),
            handle: example_handle(),
            pds: example_pds(),
            signing_key: example_signing_key(),
        }
    }
}

#[derive(ApiResponse)]
#[oai(bad_request_handler = "bad_request_handler_resolve_mini")]
enum ResolveMiniDocResponse {
    /// Identity resolved
    #[oai(status = 200)]
    Ok(Json<MiniDocResponseObject>),
    /// Bad request or identity not resolved
    #[oai(status = 400)]
    BadRequest(XrpcError),
}

#[derive(Object)]
#[oai(example = true)]
struct ServiceResponseObject {
    /// The service endpoint URL, if found
    endpoint: String,
}
impl Example for ServiceResponseObject {
    fn example() -> Self {
        Self {
            endpoint: "https://example.com".to_string(),
        }
    }
}

#[derive(ApiResponse)]
#[oai(bad_request_handler = "bad_request_handler_resolve_service")]
enum ResolveServiceResponse {
    /// Service resolved
    #[oai(status = 200)]
    Ok(Json<ServiceResponseObject>),
    /// Bad request or service not resolved
    #[oai(status = 400)]
    BadRequest(XrpcError),
}

#[derive(Object)]
#[oai(rename_all = "camelCase")]
struct ProxyHydrationError {
    /// Short description of why the hydration failed
    reason: String,
    /// Whether or not it's recommended to retry requesting this item
    should_retry: bool,
    /// URL to follow up at if retrying
    follow_up: String,
}

#[derive(Object)]
#[oai(rename_all = "camelCase")]
struct ProxyHydrationPending {
    /// URL you can request to finish hydrating this item
    follow_up: String,
    /// Why this item couldn't be hydrated: 'deadline' or 'limit'
    ///
    /// - `deadline`: the item fetch didn't complete before the response was
    ///   due, but will continue on slingshot in the background -- `followUp`
    ///   requests are coalesced into the original item fetch to be available as
    ///   early as possible.
    ///
    /// - `limit`: slingshot only attempts to hydrate the first 100 items found
    ///   in a proxied response, with the remaining marked `pending`. You can
    ///   request `followUp` to fetch them.
    ///
    /// In the future, Slingshot may put pending links after `limit` into a low-
    /// priority fetch queue, so that these items become available sooner on
    /// follow-up request as well.
    reason: String,
}

// todo: there's gotta be a supertrait that collects these?
use poem_openapi::types::{IsObjectType, ParseFromJSON, ToJSON, Type};

#[derive(Union)]
#[oai(discriminator_name = "status", rename_all = "camelCase")]
enum Hydration<T: Send + Sync + Type + ToJSON + ParseFromJSON + IsObjectType> {
    Error(ProxyHydrationError),
    Pending(ProxyHydrationPending),
    Found(T),
}

#[derive(Object)]
#[oai(example = true)]
struct ProxyHydrateResponseObject {
    /// The original upstream response content
    output: serde_json::Value,
    /// Any hydrated records
    records: HashMap<String, Hydration<FoundRecordResponseObject>>,
    /// Any hydrated identifiers
    identifiers: HashMap<String, Hydration<MiniDocResponseObject>>,
}
impl Example for ProxyHydrateResponseObject {
    fn example() -> Self {
        Self {
            output: serde_json::json!({}),
            records: HashMap::from([(
                "asdf".into(),
                Hydration::Pending(ProxyHydrationPending {
                    follow_up: "/xrpc/com.atproto.repo.getRecord?...".to_string(),
                    reason: "deadline".to_string(),
                }),
            )]),
            identifiers: HashMap::new(),
        }
    }
}

#[derive(ApiResponse)]
#[oai(bad_request_handler = "bad_request_handler_proxy_query")]
enum ProxyHydrateResponse {
    #[oai(status = 200)]
    Ok(Json<ProxyHydrateResponseObject>),
    #[oai(status = 400)]
    BadRequest(XrpcError),
}

#[derive(Object)]
pub struct HydrationSource {
    /// Record Path syntax for locating fields
    pub path: String,
    /// What to expect at the path: 'strong-ref', 'at-uri', 'at-uri-parts', 'did', 'handle', or 'at-identifier'.
    ///
    /// - `strong-ref`: object in the shape of `com.atproto.repo.strongRef` with `uri` and `cid` keys.
    /// - `at-uri`: string, must have all segments present (identifier, collection, rkey)
    /// - `at-uri-parts`: object with keys (`repo` or `did`), `collection`, `rkey`, and optional `cid`. Other keys may be present and will be ignored.
    /// - `did`: string, `did` format
    /// - `handle`: string, `handle` format
    /// - `at-identifier`: string, `did` or `handle` format
    pub shape: String,
}

#[derive(Object)]
#[oai(example = true)]
struct ProxyQueryPayload {
    /// The NSID of the XRPC you wish to forward
    xrpc: String,
    /// The destination service the request will be forwarded to
    atproto_proxy: String,
    /// An optional auth token to pass on
    ///
    /// the `aud` field must match the upstream atproto_proxy service
    authorization: Option<String>,
    /// An optional set of labelers to request be applied by the upstream
    atproto_accept_labelers: Option<String>,
    /// The `params` for the destination service XRPC endpoint
    ///
    /// Currently this will be passed along unchecked, but a future version of
    /// slingshot may attempt to do lexicon resolution to validate `params`
    /// based on the upstream service
    params: Option<serde_json::Value>,
    /// Paths within the response to look for at-uris that can be hydrated
    hydration_sources: Vec<HydrationSource>,
    // todo: let clients pass a hydration deadline?
}
impl Example for ProxyQueryPayload {
    fn example() -> Self {
        Self {
            xrpc: "app.bsky.feed.getFeedSkeleton".to_string(),
            atproto_proxy: "did:web:blue.mackuba.eu#bsky_fg".to_string(),
            authorization: None,
            atproto_accept_labelers: None,
            params: Some(serde_json::json!({
                "feed": "at://did:plc:oio4hkxaop4ao4wz2pp3f4cr/app.bsky.feed.generator/atproto",
            })),
            hydration_sources: vec![HydrationSource {
                path: "feed[].post".to_string(),
                shape: "at-uri".to_string(),
            }],
        }
    }
}

#[derive(Object)]
#[oai(example = true)]
struct FoundDidResponseObject {
    /// the DID, bi-directionally verified if using Slingshot
    did: String,
}
impl Example for FoundDidResponseObject {
    fn example() -> Self {
        Self { did: example_did() }
    }
}

#[derive(ApiResponse)]
#[oai(bad_request_handler = "bad_request_handler_resolve_handle")]
enum JustDidResponse {
    /// Resolution succeeded
    #[oai(status = 200)]
    Ok(Json<FoundDidResponseObject>),
    /// Bad request, failed to resolve, or failed to verify
    ///
    /// `error` will be one of `InvalidRequest`, `HandleNotFound`.
    #[oai(status = 400)]
    BadRequest(XrpcError),
    /// Something went wrong trying to complete the request
    #[oai(status = 500)]
    ServerError(XrpcError),
}

struct Xrpc {
    base_url: url::Url,
    cache: HybridCache<String, CachedRecord>,
    identity: Identity,
    proxy: Arc<Proxy>,
    repo: Arc<Repo>,
}

#[derive(Tags)]
enum ApiTags {
    /// Core ATProtocol-compatible APIs.
    ///
    /// > [!tip]
    /// > Upstream documentation is available at
    /// > https://docs.bsky.app/docs/category/http-reference
    ///
    /// These queries are usually executed directly against the PDS containing
    /// the data being requested. Slingshot offers a caching view of the same
    /// contents with better expected performance and reliability.
    #[oai(rename = "com.atproto.* queries")]
    ComAtproto,
    /// Additional and improved APIs.
    ///
    /// These APIs offer small tweaks to the core ATProtocol APIs, with more
    /// more convenient [request parameters](#tag/slingshot-specific-queries/GET/xrpc/com.bad-example.repo.getUriRecord)
    /// or [response formats](#tag/slingshot-specific-queries/GET/xrpc/com.bad-example.identity.resolveMiniDoc).
    ///
    /// > [!important]
    /// > At the moment, these are namespaced under the `com.bad-example.*` NSID
    /// > prefix, but as they stabilize they may be migrated to an org namespace
    /// > like `blue.microcosm.*`. Support for asliasing to `com.bad-example.*`
    /// > will be maintained as long as it's in use.
    #[oai(rename = "slingshot-specific queries")]
    Custom,
}

#[OpenApi]
impl Xrpc {
    /// com.atproto.repo.getRecord
    ///
    /// Get a single record from a repository. Does not require auth.
    ///
    /// > [!tip]
    /// > See also the [canonical `com.atproto` XRPC documentation](https://docs.bsky.app/docs/api/com-atproto-repo-get-record)
    /// > that this endpoint aims to be compatible with.
    #[oai(
        path = "/com.atproto.repo.getRecord",
        method = "get",
        tag = "ApiTags::ComAtproto"
    )]
    async fn get_record(
        &self,
        /// The DID or handle of the repo
        #[oai(example = "example_did")]
        Query(repo): Query<String>,
        /// The NSID of the record collection
        #[oai(example = "example_collection")]
        Query(collection): Query<String>,
        /// The Record key
        #[oai(example = "example_rkey")]
        Query(rkey): Query<String>,
        /// Optional: the CID of the version of the record.
        ///
        /// If not specified, then return the most recent version.
        ///
        /// If a stale `CID` is specified and a newer version of the record
        /// exists, Slingshot returns a `NotFound` error. That is: Slingshot
        /// only retains the most recent version of a record.
        Query(cid): Query<Option<String>>,
    ) -> GetRecordResponse {
        self.get_record_impl(&repo, &collection, &rkey, cid.as_deref())
            .await
    }

    /// blue.microcosm.repo.getRecordByUri
    ///
    /// alias of `com.bad-example.repo.getUriRecord` with intention to stabilize under this name
    #[oai(
        path = "/blue.microcosm.repo.getRecordByUri",
        method = "get",
        tag = "ApiTags::Custom"
    )]
    async fn get_record_by_uri(
        &self,
        /// The at-uri of the record
        ///
        /// The identifier can be a DID or an atproto handle, and the collection
        /// and rkey segments must be present.
        #[oai(example = "example_uri")]
        Query(at_uri): Query<String>,
        /// Optional: the CID of the version of the record.
        ///
        /// If not specified, then return the most recent version.
        ///
        /// > [!tip]
        /// > If specified and a newer version of the record exists, returns 404 not
        /// > found. That is: slingshot only retains the most recent version of a
        /// > record.
        Query(cid): Query<Option<String>>,
    ) -> GetRecordResponse {
        self.get_uri_record(Query(at_uri), Query(cid)).await
    }

    /// com.bad-example.repo.getUriRecord
    ///
    /// Ergonomic complement to [`com.atproto.repo.getRecord`](https://docs.bsky.app/docs/api/com-atproto-repo-get-record)
    /// which accepts an `at-uri` instead of individual repo/collection/rkey params
    #[oai(
        path = "/com.bad-example.repo.getUriRecord",
        method = "get",
        tag = "ApiTags::Custom"
    )]
    async fn get_uri_record(
        &self,
        /// The at-uri of the record
        ///
        /// The identifier can be a DID or an atproto handle, and the collection
        /// and rkey segments must be present.
        #[oai(example = "example_uri")]
        Query(at_uri): Query<String>,
        /// Optional: the CID of the version of the record.
        ///
        /// If not specified, then return the most recent version.
        ///
        /// > [!tip]
        /// > If specified and a newer version of the record exists, returns 404 not
        /// > found. That is: slingshot only retains the most recent version of a
        /// > record.
        Query(cid): Query<Option<String>>,
    ) -> GetRecordResponse {
        let bad_at_uri = || {
            GetRecordResponse::BadRequest(xrpc_error(
                "InvalidRequest",
                "at-uri does not appear to be valid",
            ))
        };

        let Some(normalized) = normalize_at_uri(&at_uri) else {
            return bad_at_uri();
        };

        let Some((repo, collection, rkey)) = split_uri(&normalized) else {
            return bad_at_uri();
        };

        self.get_record_impl(
            Into::<String>::into(repo).as_str(),
            collection.as_str(),
            rkey.as_str(),
            cid.as_deref(),
        )
        .await
    }

    /// com.atproto.identity.resolveHandle
    ///
    /// Resolves an atproto [`handle`](https://atproto.com/guides/glossary#handle)
    /// (hostname) to a [`DID`](https://atproto.com/guides/glossary#did-decentralized-id).
    ///
    /// > [!tip]
    /// > Compatibility note: Slingshot will **always bi-directionally verify
    /// > against the DID document**, which is optional according to the
    /// > authoritative lexicon.
    ///
    /// > [!tip]
    /// > See the [canonical `com.atproto` XRPC documentation](https://docs.bsky.app/docs/api/com-atproto-identity-resolve-handle)
    /// > that this endpoint aims to be compatible with.
    #[oai(
        path = "/com.atproto.identity.resolveHandle",
        method = "get",
        tag = "ApiTags::ComAtproto"
    )]
    async fn resolve_handle(
        &self,
        /// The handle to resolve.
        #[oai(example = "example_handle")]
        Query(handle): Query<String>,
    ) -> JustDidResponse {
        let Ok(handle) = Handle::new(handle.to_lowercase()) else {
            return JustDidResponse::BadRequest(xrpc_error("InvalidRequest", "not a valid handle"));
        };

        let Ok(alleged_did) = self.identity.handle_to_did(handle.clone()).await else {
            return JustDidResponse::ServerError(xrpc_error("Failed", "Could not resolve handle"));
        };

        let Some(alleged_did) = alleged_did else {
            return JustDidResponse::BadRequest(xrpc_error(
                "HandleNotFound",
                "Could not resolve handle to a DID",
            ));
        };

        let Ok(partial_doc) = self.identity.did_to_partial_mini_doc(&alleged_did).await else {
            return JustDidResponse::ServerError(xrpc_error("Failed", "Could not fetch DID doc"));
        };

        let Some(partial_doc) = partial_doc else {
            return JustDidResponse::BadRequest(xrpc_error(
                "HandleNotFound",
                "Resolved handle but could not find DID doc for the DID",
            ));
        };

        if partial_doc.unverified_handle != handle {
            return JustDidResponse::BadRequest(xrpc_error(
                "HandleNotFound",
                "Resolved handle failed bi-directional validation",
            ));
        }

        JustDidResponse::Ok(Json(FoundDidResponseObject {
            did: alleged_did.to_string(),
        }))
    }

    /// blue.microcosm.identity.resolveMiniDoc
    ///
    /// alias of `com.bad-example.identity.resolveMiniDoc` with intention to stabilize under this name
    #[oai(
        path = "/blue.microcosm.identity.resolveMiniDoc",
        method = "get",
        tag = "ApiTags::Custom"
    )]
    async fn resolve_mini_doc(
        &self,
        /// Handle or DID to resolve
        #[oai(example = "example_handle")]
        Query(identifier): Query<String>,
    ) -> ResolveMiniDocResponse {
        self.resolve_mini_id(Query(identifier)).await
    }

    /// com.bad-example.identity.resolveMiniDoc
    ///
    /// Like [com.atproto.identity.resolveIdentity](https://docs.bsky.app/docs/api/com-atproto-identity-resolve-identity)
    /// but instead of the full `didDoc` it returns an atproto-relevant subset.
    #[oai(
        path = "/com.bad-example.identity.resolveMiniDoc",
        method = "get",
        tag = "ApiTags::Custom"
    )]
    async fn resolve_mini_id(
        &self,
        /// Handle or DID to resolve
        #[oai(example = "example_handle")]
        Query(identifier): Query<String>,
    ) -> ResolveMiniDocResponse {
        Self::resolve_mini_doc_impl(&identifier, self.identity.clone()).await
    }

    async fn resolve_mini_doc_impl(identifier: &str, identity: Identity) -> ResolveMiniDocResponse {
        let invalid = |reason: &'static str| {
            ResolveMiniDocResponse::BadRequest(xrpc_error("InvalidRequest", reason))
        };

        let mut unverified_handle = None;
        let did = match Did::new(identifier.to_string()) {
            Ok(did) => did,
            Err(_) => {
                let Ok(alleged_handle) = Handle::new(identifier.to_lowercase()) else {
                    return invalid("Identifier was not a valid DID or handle");
                };

                match identity.handle_to_did(alleged_handle.clone()).await {
                    Ok(res) => {
                        if let Some(did) = res {
                            // we did it joe
                            unverified_handle = Some(alleged_handle);
                            did
                        } else {
                            return invalid("Could not resolve handle identifier to a DID");
                        }
                    }
                    Err(e) => {
                        log::debug!("failed to resolve handle: {e}");
                        // TODO: ServerError not BadRequest
                        return invalid("Errored while trying to resolve handle to DID");
                    }
                }
            }
        };
        let Ok(partial_doc) = identity.did_to_partial_mini_doc(&did).await else {
            return invalid("Failed to get DID doc");
        };
        let Some(partial_doc) = partial_doc else {
            return invalid("Failed to find DID doc");
        };

        // ok so here's where we're at:
        // ✅ we have a DID
        // ✅ we have a partial doc
        // 🔶 if we have a handle, it's from the `identifier` (user-input)
        //      -> then we just need to compare to the partial doc to confirm
        //      -> else we need to resolve the DID doc's  to a handle and check
        let handle = if let Some(h) = unverified_handle {
            if h == partial_doc.unverified_handle {
                h.to_string()
            } else {
                "handle.invalid".to_string()
            }
        } else {
            let Ok(handle_did) = identity
                .handle_to_did(partial_doc.unverified_handle.clone())
                .await
            else {
                return invalid("Failed to get DID doc's handle");
            };
            let Some(handle_did) = handle_did else {
                return invalid("Failed to resolve DID doc's handle");
            };
            if handle_did == did {
                partial_doc.unverified_handle.to_string()
            } else {
                "handle.invalid".to_string()
            }
        };

        ResolveMiniDocResponse::Ok(Json(MiniDocResponseObject {
            did: did.to_string(),
            handle,
            pds: partial_doc.pds,
            signing_key: partial_doc.signing_key,
        }))
    }

    /// com.bad-example.identity.resolveService
    ///
    /// resolve an atproto service did + id to its http endpoint
    ///
    /// > [!important]
    /// > this endpoint is experimental and may change
    #[oai(
        path = "/com.bad-example.identity.resolveService",
        method = "get",
        tag = "ApiTags::Custom"
    )]
    async fn resolve_service(
        &self,
        /// the service's did
        #[oai(example = "example_service_did")]
        Query(did): Query<String>,
        /// id fragment, starting with '#'
        ///
        /// must be url-encoded!
        #[oai(example = "example_id_fragment")]
        Query(id): Query<String>,
        /// optionally, the exact service type to filter
        ///
        /// resolving a pds requires matching the type as well as id. service
        /// proxying ignores the type.
        Query(r#type): Query<Option<String>>,
    ) -> ResolveServiceResponse {
        let Ok(did) = Did::new(did) else {
            return ResolveServiceResponse::BadRequest(xrpc_error(
                "InvalidRequest",
                "could not parse 'did' into a DID",
            ));
        };
        let identity = self.identity.clone();
        Self::resolve_service_impl(&did, &id, r#type.as_deref(), identity).await
    }

    async fn resolve_service_impl(
        did: &Did,
        id_fragment: &str,
        service_type: Option<&str>,
        identity: Identity,
    ) -> ResolveServiceResponse {
        let invalid = |reason: &'static str| {
            ResolveServiceResponse::BadRequest(xrpc_error("InvalidRequest", reason))
        };
        let Ok(service_mini_doc) = identity.did_to_mini_service_doc(did).await else {
            return invalid("Failed to get DID doc");
        };
        let Some(service_mini_doc) = service_mini_doc else {
            return invalid("Failed to find DID doc");
        };

        let Some(matching) = service_mini_doc.get(id_fragment, service_type) else {
            return invalid("failed to match identity (and maybe type)");
        };

        ResolveServiceResponse::Ok(Json(ServiceResponseObject {
            endpoint: matching.endpoint.clone(),
        }))
    }

    /// com.bad-example.proxy.hydrateQueryResponse
    ///
    /// > [!important]
    /// > Unstable! This endpoint is experimental and may change.
    ///
    /// Fetch + include records referenced from an upstream xrpc query response
    #[oai(
        path = "/com.bad-example.proxy.hydrateQueryResponse",
        method = "post",
        tag = "ApiTags::Custom"
    )]
    async fn proxy_hydrate_query(
        &self,
        Json(payload): Json<ProxyQueryPayload>,
    ) -> ProxyHydrateResponse {
        let params = if let Some(p) = payload.params {
            let serde_json::Value::Object(map) = p else {
                panic!("params have to be an object");
            };
            Some(map)
        } else {
            None
        };

        let Some((service_did, id_fragment)) = payload.atproto_proxy.rsplit_once("#") else {
            return ProxyHydrateResponse::BadRequest(xrpc_error(
                "BadParameter",
                "atproto_proxy could not be understood",
            ));
        };

        let Ok(service_did) = service_did.parse() else {
            return ProxyHydrateResponse::BadRequest(xrpc_error(
                "BadParameter",
                "atproto_proxy service did could not be parsed",
            ));
        };

        let Ok(xrpc) = payload.xrpc.parse() else {
            return ProxyHydrateResponse::BadRequest(xrpc_error(
                "BadParameter",
                "invalid NSID for xrpc param",
            ));
        };

        match self
            .proxy
            .proxy(
                &service_did,
                &format!("#{id_fragment}"),
                &xrpc,
                payload.authorization.as_deref(),
                payload.atproto_accept_labelers.as_deref(),
                params,
            )
            .await
        {
            Ok(skeleton) => {
                let links = match extract_links(payload.hydration_sources, &skeleton) {
                    Ok(l) => l,
                    Err(e) => {
                        log::warn!("problem extracting: {e:?}");
                        return ProxyHydrateResponse::BadRequest(xrpc_error(
                            "oop",
                            "sorry, error extracting",
                        ));
                    }
                };
                let mut records = HashMap::new();
                let mut identifiers = HashMap::new();

                enum GetThing {
                    Record(String, Hydration<FoundRecordResponseObject>),
                    Identifier(String, Hydration<MiniDocResponseObject>),
                }

                let (tx, mut rx) = mpsc::channel(1);

                let t0 = Instant::now();

                for (i, link) in links.into_iter().enumerate() {
                    match link {
                        MatchedRef::AtUri(parts) => {
                            let non_canonical_url = parts.to_uri();
                            if records.contains_key(&non_canonical_url) {
                                log::warn!("skipping duplicate record without checking cid");
                                continue;
                            }
                            let mut follow_up = self.base_url.clone();
                            follow_up.set_path("/xrpc/com.atproto.repo.getRecord");
                            follow_up
                                .query_pairs_mut()
                                .append_pair("repo", &Into::<String>::into(parts.repo.clone()))
                                .append_pair("collection", parts.collection.as_str())
                                .append_pair("rkey", parts.rkey.as_str());
                            if let Some(ref cid) = parts.cid {
                                follow_up
                                    .query_pairs_mut()
                                    .append_pair("cid", &cid.as_ref().to_string());
                            }

                            if i >= 100 {
                                records.insert(
                                    non_canonical_url.clone(),
                                    Hydration::Pending(ProxyHydrationPending {
                                        reason: "limit".to_string(),
                                        follow_up: follow_up.to_string(),
                                    }),
                                );
                                continue;
                            } else {
                                records.insert(
                                    non_canonical_url.clone(),
                                    Hydration::Pending(ProxyHydrationPending {
                                        reason: "deadline".to_string(),
                                        follow_up: follow_up.to_string(),
                                    }),
                                );
                            }

                            let tx = tx.clone();
                            let identity = self.identity.clone();
                            let repo = self.repo.clone();
                            tokio::task::spawn(async move {
                                let FullAtUriParts {
                                    repo: ident,
                                    collection,
                                    rkey,
                                    cid,
                                } = parts;
                                let did = match ident {
                                    AtIdentifier::Did(did) => did,
                                    AtIdentifier::Handle(handle) => {
                                        let Ok(Some(did)) = identity.handle_to_did(handle).await
                                        else {
                                            let res = Hydration::Error(ProxyHydrationError {
                                                reason: "could not resolve handle".to_string(),
                                                should_retry: true,
                                                follow_up: follow_up.to_string(),
                                            });
                                            return if tx
                                                .send(GetThing::Record(non_canonical_url, res))
                                                .await
                                                .is_ok()
                                            {
                                                metrics::counter!("slingshot_hydrated_one", "type" => "record", "ontime" => "true").increment(1);
                                            } else {
                                                metrics::counter!("slingshot_hydrated_one", "type" => "record", "ontime" => "false").increment(1);
                                            };
                                        };
                                        did
                                    }
                                };

                                let res =
                                    match repo.get_record(&did, &collection, &rkey, &cid).await {
                                        Ok(CachedRecord::Deleted) => {
                                            Hydration::Error(ProxyHydrationError {
                                                reason: "record deleted".to_string(),
                                                should_retry: false,
                                                follow_up: follow_up.to_string(),
                                            })
                                        }
                                        Ok(CachedRecord::Found(RawRecord {
                                            cid: found_cid,
                                            record,
                                        })) => {
                                            if cid
                                                .as_ref()
                                                .map(|expected| *expected != found_cid)
                                                .unwrap_or(false)
                                            {
                                                Hydration::Error(ProxyHydrationError {
                                                    reason: "not found".to_string(),
                                                    should_retry: false,
                                                    follow_up: follow_up.to_string(),
                                                })
                                            } else if let Ok(value) = serde_json::from_str(&record)
                                            {
                                                let canonical_uri = FullAtUriParts {
                                                    repo: AtIdentifier::Did(did),
                                                    collection,
                                                    rkey,
                                                    cid: None, // not used for .to_uri
                                                }
                                                .to_uri();
                                                Hydration::Found(FoundRecordResponseObject {
                                                    cid: Some(found_cid.as_ref().to_string()),
                                                    uri: canonical_uri,
                                                    value,
                                                })
                                            } else {
                                                Hydration::Error(ProxyHydrationError {
                                                    reason: "could not parse upstream response"
                                                        .to_string(),
                                                    should_retry: false,
                                                    follow_up: follow_up.to_string(),
                                                })
                                            }
                                        }
                                        Err(e) => {
                                            log::warn!("finally oop {e:?}");
                                            Hydration::Error(ProxyHydrationError {
                                                reason: "failed to fetch record".to_string(),
                                                should_retry: true, // TODO
                                                follow_up: follow_up.to_string(),
                                            })
                                        }
                                    };
                                if tx
                                    .send(GetThing::Record(non_canonical_url, res))
                                    .await
                                    .is_ok()
                                {
                                    metrics::counter!("slingshot_hydrated_one", "type" => "record", "ontime" => "true").increment(1);
                                } else {
                                    metrics::counter!("slingshot_hydrated_one", "type" => "record", "ontime" => "false").increment(1);
                                }
                            });
                        }
                        MatchedRef::Identifier(id) => {
                            let identifier: String = id.clone().into();
                            if identifiers.contains_key(&identifier) {
                                continue;
                            }

                            let mut follow_up = self.base_url.clone();
                            follow_up.set_path("/xrpc/blue.microcosm.identity.resolveMiniDoc");

                            follow_up
                                .query_pairs_mut()
                                .append_pair("identifier", &identifier);

                            if i >= 100 {
                                identifiers.insert(
                                    identifier.clone(),
                                    Hydration::Pending(ProxyHydrationPending {
                                        reason: "limit".to_string(),
                                        follow_up: follow_up.to_string(),
                                    }),
                                );
                                continue;
                            } else {
                                identifiers.insert(
                                    identifier.clone(),
                                    Hydration::Pending(ProxyHydrationPending {
                                        reason: "deadline".to_string(),
                                        follow_up: follow_up.to_string(),
                                    }),
                                );
                            }

                            let tx = tx.clone();
                            let identity = self.identity.clone();
                            tokio::task::spawn(async move {
                                let res = match Self::resolve_mini_doc_impl(&identifier, identity)
                                    .await
                                {
                                    ResolveMiniDocResponse::Ok(Json(mini_doc)) => {
                                        Hydration::Found(mini_doc)
                                    }
                                    ResolveMiniDocResponse::BadRequest(e) => {
                                        log::warn!("minidoc fail: {:?}", e.0);
                                        Hydration::Error(ProxyHydrationError {
                                            reason: "failed to resolve mini doc".to_string(),
                                            should_retry: false,
                                            follow_up: follow_up.to_string(),
                                        })
                                    }
                                };
                                if tx.send(GetThing::Identifier(identifier, res)).await.is_ok() {
                                    metrics::counter!("slingshot_hydrated_one", "type" => "identity", "ontime" => "true").increment(1);
                                } else {
                                    metrics::counter!("slingshot_hydrated_one", "type" => "identity", "ontime" => "false").increment(1);
                                }
                            });
                        }
                    }
                }
                // so the channel can close when all are completed
                // (we shoudl be doing a timeout...)
                drop(tx);

                let deadline = t0 + std::time::Duration::from_secs_f64(1.6);
                let res = tokio::time::timeout_at(deadline.into(), async {
                    while let Some(hydration) = rx.recv().await {
                        match hydration {
                            GetThing::Record(uri, h) => {
                                if let Some(r) = records.get_mut(&uri) {
                                    match (&r, &h) {
                                        (_, Hydration::Found(_)) => *r = h,   // always replace if found
                                        (Hydration::Pending(_), _) => *r = h, // or if it was pending
                                        _ => {}                               // else leave it
                                    }
                                } else {
                                    records.insert(uri, h);
                                }
                            }
                            GetThing::Identifier(identifier, md) => {
                                identifiers.insert(identifier.to_string(), md);
                            }
                        };
                    }
                })
                .await;

                if res.is_ok() {
                    metrics::histogram!("slingshot_hydration_all_completed").record(t0.elapsed());
                } else {
                    metrics::counter!("slingshot_hydration_cut_off").increment(1);
                }

                ProxyHydrateResponse::Ok(Json(ProxyHydrateResponseObject {
                    output: skeleton,
                    records,
                    identifiers,
                }))
            }
            Err(e) => {
                log::warn!("oh no: {e:?}");
                ProxyHydrateResponse::BadRequest(xrpc_error("oop", "sorry"))
            }
        }
    }

    async fn get_record_impl(
        &self,
        repo: &str,
        collection: &str,
        rkey: &str,
        cid: Option<&str>,
    ) -> GetRecordResponse {
        let did = match Did::new(repo.to_string()) {
            Ok(did) => did,
            Err(_) => {
                let Ok(handle) = Handle::new(repo.to_lowercase()) else {
                    return GetRecordResponse::BadRequest(xrpc_error(
                        "InvalidRequest",
                        "Repo was not a valid DID or handle",
                    ));
                };
                match self.identity.handle_to_did(handle).await {
                    Ok(res) => {
                        if let Some(did) = res {
                            did
                        } else {
                            return GetRecordResponse::BadRequest(xrpc_error(
                                "InvalidRequest",
                                "Could not resolve handle repo to a DID",
                            ));
                        }
                    }
                    Err(e) => {
                        log::debug!("handle resolution failed: {e}");
                        return GetRecordResponse::ServerError(xrpc_error(
                            "ResolutionFailed",
                            "Errored while trying to resolve handle to DID",
                        ));
                    }
                }
            }
        };

        let Ok(collection) = Nsid::new(collection.to_string()) else {
            return GetRecordResponse::BadRequest(xrpc_error(
                "InvalidRequest",
                "Invalid NSID for collection",
            ));
        };

        let Ok(rkey) = RecordKey::new(rkey.to_string()) else {
            return GetRecordResponse::BadRequest(xrpc_error("InvalidRequest", "Invalid rkey"));
        };

        let cid: Option<Cid> = if let Some(cid) = cid {
            let Ok(cid) = Cid::from_str(cid) else {
                return GetRecordResponse::BadRequest(xrpc_error("InvalidRequest", "Invalid CID"));
            };
            Some(cid)
        } else {
            None
        };

        let at_uri = format!("at://{}/{}/{}", &*did, &*collection, &*rkey);

        metrics::counter!("slingshot_get_record").increment(1);
        let fr = self
            .cache
            .get_or_fetch(&at_uri, {
                let cid = cid.clone();
                let repo_api = self.repo.clone();
                || async move {
                    let t0 = Instant::now();
                    let res = repo_api.get_record(&did, &collection, &rkey, &cid).await;
                    let success = if res.is_ok() { "true" } else { "false" };
                    metrics::histogram!("slingshot_fetch_record", "success" => success)
                        .record(t0.elapsed());
                    res
                }
            })
            .await;

        let entry = match fr {
            Ok(e) => e,
            Err(e) if e.kind() == foyer::ErrorKind::External => {
                let record_error = match e.source().map(|s| s.downcast_ref::<RecordError>()) {
                    Some(Some(e)) => e,
                    other => {
                        if other.is_none() {
                            log::error!("external error without a source. wat? {e}");
                        } else {
                            log::error!("downcast to RecordError failed...? {e}");
                        }
                        return GetRecordResponse::ServerError(xrpc_error(
                            "ServerError",
                            "sorry, something went wrong",
                        ));
                    }
                };
                let RecordError::UpstreamBadRequest(ErrorResponseObject {
                    ref error,
                    ref message,
                }) = *record_error
                else {
                    log::error!("RecordError getting cache entry, {record_error:?}");
                    return GetRecordResponse::ServerError(xrpc_error(
                        "ServerError",
                        "sorry, something went wrong",
                    ));
                };

                // all of the noise around here is so that we can ultimately reach this:
                // upstream BadRequest extracted from the foyer result which we can proxy back
                return GetRecordResponse::BadRequest(xrpc_error(
                    error,
                    format!("Upstream bad request: {message}"),
                ));
            }
            Err(e) => {
                log::error!("error (foyer) getting cache entry, {e:?}");
                return GetRecordResponse::ServerError(xrpc_error(
                    "ServerError",
                    "sorry, something went wrong",
                ));
            }
        };

        match *entry {
            CachedRecord::Found(ref raw) => {
                let (found_cid, raw_value) = raw.into();
                if cid.clone().map(|c| c != found_cid).unwrap_or(false) {
                    return GetRecordResponse::BadRequest(Json(XrpcErrorResponseObject {
                        error: "RecordNotFound".to_string(),
                        message: "A record was found but its CID did not match that requested"
                            .to_string(),
                    }));
                }
                // TODO: thank u stellz: https://gist.github.com/stella3d/51e679e55b264adff89d00a1e58d0272
                let value =
                    serde_json::from_str(raw_value.get()).expect("RawValue to be valid json");
                GetRecordResponse::Ok(Json(FoundRecordResponseObject {
                    uri: at_uri,
                    cid: Some(found_cid.as_ref().to_string()),
                    value,
                }))
            }
            CachedRecord::Deleted => GetRecordResponse::BadRequest(Json(XrpcErrorResponseObject {
                error: "RecordNotFound".to_string(),
                message: "This record was deleted".to_string(),
            })),
        }
    }

    // TODO
    // #[oai(path = "/com.atproto.identity.resolveDid", method = "get")]
    // but these are both not specified to do bidirectional validation, which is what we want to offer
    // com.atproto.identity.resolveIdentity seems right, but requires returning the full did-doc
    // would be nice if there were two queries:
    //  did -> verified handle + pds url
    //  handle -> verified did + pds url
    //
    // we could do horrible things and implement resolveIdentity with only a stripped-down fake did doc
    // but this will *definitely* cause problems probably
    //
    // resolveMiniDoc gets most of this well enough.
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct AppViewService {
    id: String,
    r#type: String,
    service_endpoint: String,
}
#[derive(Debug, Clone, Serialize)]
struct AppViewDoc {
    id: String,
    service: [AppViewService; 1],
}
/// Serve a did document for did:web for this to be an xrpc appview
///
/// No slingshot endpoints currently require auth, so it's not necessary to do
/// service proxying, however clients may wish to:
///
/// - PDS proxying offers a level of client IP anonymity from slingshot
/// - slingshot *may* implement more generous per-user rate-limits for proxied requests in the future
fn get_did_doc(domain: &str) -> impl Endpoint + use<> {
    let doc = poem::web::Json(AppViewDoc {
        id: format!("did:web:{domain}"),
        service: [AppViewService {
            id: "#slingshot".to_string(),
            r#type: "SlingshotRecordProxy".to_string(),
            service_endpoint: format!("https://{domain}"),
        }],
    });
    make_sync(move |_| doc.clone())
}

#[allow(clippy::too_many_arguments)]
pub async fn serve(
    cache: HybridCache<String, CachedRecord>,
    identity: Identity,
    repo: Repo,
    proxy: Proxy,
    base_url: url::Url,
    tls_domain: Option<String>,
    tls_certs: Option<PathBuf>,
    tls_ipv6: bool,
    acme_challenge_redirect: Option<String>,
    acme_contact: Option<String>,
    acme_staging: bool,
    shutdown: CancellationToken,
    bind: std::net::SocketAddr,
) -> Result<(), ServerError> {
    let repo = Arc::new(repo);
    let proxy = Arc::new(proxy);
    let api_service = OpenApiService::new(
        Xrpc {
            base_url,
            cache,
            identity,
            proxy,
            repo,
        },
        "Slingshot",
        env!("CARGO_PKG_VERSION"),
    )
    .server(if let Some(ref h) = tls_domain {
        format!("https://{h}")
    } else {
        format!("http://{bind}") // yeah should probably fix this for reverse-proxy scenarios but it's ok for dev for now
    })
    .url_prefix("/xrpc")
    .contact(
        ContactObject::new()
            .name("@microcosm.blue")
            .url("https://bsky.app/profile/microcosm.blue"),
    )
    .description(include_str!("../api-description.md"))
    .external_document(ExternalDocumentObject::new(
        "https://microcosm.blue/slingshot",
    ));

    let app = Route::new()
        .at("/", StaticFileEndpoint::new("./static/index.html"))
        .nest("/openapi", api_service.spec_endpoint())
        .nest("/xrpc/", api_service);

    let cors = Cors::new()
        .allow_origin_regex("*")
        .allow_methods([Method::GET, Method::POST])
        .allow_credentials(false);

    if let Some(domain) = tls_domain {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("alskfjalksdjf");

        let app = app
            .at("/.well-known/did.json", get_did_doc(&domain))
            .with(cors);

        if let Some(contact) = acme_contact {
            let (listener, app) = acmify(app, domain, tls_certs, tls_ipv6, contact, acme_staging)?;
            run(listener, app, shutdown).await
        } else {
            let certs = tls_certs.expect("certs path must be set for non-acme tls");
            let (listener, app) = tlsify(app, domain, certs, tls_ipv6, acme_challenge_redirect)?;
            run(listener, app, shutdown).await
        }
    } else {
        run(TcpListener::bind(bind), app.with(cors), shutdown).await
    }
}

fn acmify(
    app: impl Endpoint + 'static,
    domain: String,
    tls_certs: Option<PathBuf>,
    tls_ipv6: bool,
    acme_contact: String,
    acme_staging: bool,
) -> Result<(impl Listener + 'static, impl Endpoint + 'static), ServerError> {
    let mut auto_cert = AutoCert::builder()
        .contact(acme_contact)
        .directory_url(if acme_staging {
            LETS_ENCRYPT_STAGING
        } else {
            LETS_ENCRYPT_PRODUCTION
        })
        .domain(&domain)
        .challenge_type(ChallengeType::Http01);

    if let Some(path) = tls_certs {
        auto_cert = auto_cert.cache_path(path);
    } else {
        log::warn!(
            "provisioning acme certs without `--tls-certs` folder configured, you might hit letsencrypt rate limits."
        );
    }

    let auto_cert = auto_cert.build().map_err(ServerError::AcmeBuildError)?;

    let app = RouteScheme::new()
        .https(app)
        .http(auto_cert.http_01_endpoint());

    let listener = TcpListener::bind(if tls_ipv6 { "[::]:443" } else { "0.0.0.0:443" })
        .acme(auto_cert)
        .combine(TcpListener::bind(if tls_ipv6 {
            "[::]:80"
        } else {
            "0.0.0.0:80"
        }));

    Ok((listener, app))
}

fn tlsify(
    app: impl Endpoint + 'static,
    domain: String,
    tls_certs: PathBuf,
    tls_ipv6: bool,
    acme_challenge_redirect: Option<String>,
) -> Result<(impl Listener + 'static, impl Endpoint + 'static), ServerError> {
    use poem::listener::{RustlsCertificate, RustlsConfig};
    use std::path::Path;

    fn load_tls_config(f: &Path, domain: &str) -> Result<RustlsConfig, std::io::Error> {
        let cert_contents = std::fs::read(f.join("cert.pem"))
            .inspect_err(|e| log::error!("failed to read cert file in {f:?}: {e}"))?;

        let key_contents = std::fs::read(f.join("key.pem"))
            .inspect_err(|e| log::error!("failed to read key file in {f:?}: {e}"))?;

        let cert = RustlsCertificate::new()
            .cert(cert_contents)
            .key(key_contents);
        Ok(RustlsConfig::new().certificate(domain, cert))
    }

    let listener = TcpListener::bind(if tls_ipv6 { "[::]:443" } else { "0.0.0.0:443" })
        .rustls(async_stream::stream! {
            loop {
                if let Ok(tls_config) = load_tls_config(&tls_certs, &domain) {
                    // TODO: cert reload healthcheck
                    yield tls_config;
                } else {
                    log::warn!("failed to load tls config.");
                }
                tokio::time::sleep(std::time::Duration::from_secs(3600 * 12)).await;
            }
        })
        // TODO: should be allowed to run in tls mode without binding port 80 if not forwarding acme challenges
        .combine(TcpListener::bind(if tls_ipv6 {
            "[::]:80"
        } else {
            "0.0.0.0:80"
        }));

    #[poem::handler]
    async fn https_plz() -> (poem::http::StatusCode, &'static str) {
        (
            poem::http::StatusCode::MISDIRECTED_REQUEST,
            "this service must be accessed with https (port 443).",
        )
    }

    let app = if let Some(redir) = acme_challenge_redirect {
        use poem::web;

        let redirect = poem::endpoint::make_sync(move |req| {
            let token = req.path_params::<String>().unwrap();
            metrics::counter!("http_challenge_redirects").increment(1);
            web::Redirect::temporary(format!("{redir}{token}"))
        });

        RouteScheme::new().https(app).http(
            Route::new()
                .at("/.well-known/acme-challenge/:token", redirect)
                .at("/*any", https_plz)
                .at("/", https_plz),
        )
    } else {
        RouteScheme::new()
            .https(app)
            .http(Route::new().at("/*any", https_plz).at("/", https_plz))
    };

    Ok((listener, app))
}

async fn run<L, A>(listener: L, app: A, shutdown: CancellationToken) -> Result<(), ServerError>
where
    L: Listener + 'static,
    A: Endpoint + 'static,
{
    let app = app
        .with(CatchPanic::new())
        .around(request_counter)
        .with(Tracing);

    Server::new(listener)
        .name("slingshot")
        .run_with_graceful_shutdown(app, shutdown.cancelled(), None)
        .await
        .map_err(ServerError::ServerExited)
        .inspect(|()| log::info!("server ended. goodbye."))
}

async fn request_counter<E: Endpoint>(next: E, req: poem::Request) -> poem::Result<poem::Response> {
    let t0 = std::time::Instant::now();
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let res = next.call(req).await?.into_response();
    metrics::histogram!(
        "server_request",
        "endpoint" => format!("{method} {path}"),
        "status" => res.status().to_string(),
    )
    .record(t0.elapsed());
    Ok(res)
}
