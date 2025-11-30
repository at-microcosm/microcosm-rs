use crate::{
    CachedRecord, ErrorResponseObject, Identity, Repo,
    error::{RecordError, ServerError},
};
use atrium_api::types::string::{Cid, Did, Handle, Nsid, RecordKey};
use foyer::HybridCache;
use links::at_uri::parse_at_uri as normalize_at_uri;
use serde::Serialize;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use poem::{
    Endpoint, EndpointExt, Route, Server,
    endpoint::{StaticFileEndpoint, make_sync},
    http::Method,
    listener::{
        Listener, TcpListener,
        acme::{AutoCert, LETS_ENCRYPT_PRODUCTION},
    },
    middleware::{CatchPanic, Cors, Tracing},
};
use poem_openapi::{
    ApiResponse, ContactObject, ExternalDocumentObject, Object, OpenApi, OpenApiService, Tags,
    param::Query, payload::Json, types::Example,
};

fn example_handle() -> String {
    "bad-example.com".to_string()
}
fn example_did() -> String {
    "did:plc:hdhoaan3xa3jiuq4fg4mefid".to_string()
}
fn example_collection() -> String {
    "app.bsky.feed.like".to_string()
}
fn example_rkey() -> String {
    "3lv4ouczo2b2a".to_string()
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

#[derive(Object)]
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

fn bad_request_handler_resolve_mini(err: poem::Error) -> ResolveMiniIDResponse {
    ResolveMiniIDResponse::BadRequest(Json(XrpcErrorResponseObject {
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
enum ResolveMiniIDResponse {
    /// Identity resolved
    #[oai(status = 200)]
    Ok(Json<MiniDocResponseObject>),
    /// Bad request or identity not resolved
    #[oai(status = 400)]
    BadRequest(XrpcError),
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
    cache: HybridCache<String, CachedRecord>,
    identity: Identity,
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
        self.get_record_impl(repo, collection, rkey, cid).await
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

        // TODO: move this to links
        let Some(rest) = normalized.strip_prefix("at://") else {
            return bad_at_uri();
        };
        let Some((repo, rest)) = rest.split_once('/') else {
            return bad_at_uri();
        };
        let Some((collection, rest)) = rest.split_once('/') else {
            return bad_at_uri();
        };
        let rkey = if let Some((rkey, _rest)) = rest.split_once('?') {
            rkey
        } else {
            rest
        };

        self.get_record_impl(
            repo.to_string(),
            collection.to_string(),
            rkey.to_string(),
            cid,
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
        let Ok(handle) = Handle::new(handle) else {
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
    ) -> ResolveMiniIDResponse {
        let invalid = |reason: &'static str| {
            ResolveMiniIDResponse::BadRequest(xrpc_error("InvalidRequest", reason))
        };

        let mut unverified_handle = None;
        let did = match Did::new(identifier.clone()) {
            Ok(did) => did,
            Err(_) => {
                let Ok(alleged_handle) = Handle::new(identifier) else {
                    return invalid("Identifier was not a valid DID or handle");
                };

                match self.identity.handle_to_did(alleged_handle.clone()).await {
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
        let Ok(partial_doc) = self.identity.did_to_partial_mini_doc(&did).await else {
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
            let Ok(handle_did) = self
                .identity
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

        ResolveMiniIDResponse::Ok(Json(MiniDocResponseObject {
            did: did.to_string(),
            handle,
            pds: partial_doc.pds,
            signing_key: partial_doc.signing_key,
        }))
    }

    async fn get_record_impl(
        &self,
        repo: String,
        collection: String,
        rkey: String,
        cid: Option<String>,
    ) -> GetRecordResponse {
        let did = match Did::new(repo.clone()) {
            Ok(did) => did,
            Err(_) => {
                let Ok(handle) = Handle::new(repo) else {
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

        let Ok(collection) = Nsid::new(collection) else {
            return GetRecordResponse::BadRequest(xrpc_error(
                "InvalidRequest",
                "Invalid NSID for collection",
            ));
        };

        let Ok(rkey) = RecordKey::new(rkey) else {
            return GetRecordResponse::BadRequest(xrpc_error("InvalidRequest", "Invalid rkey"));
        };

        let cid: Option<Cid> = if let Some(cid) = cid {
            let Ok(cid) = Cid::from_str(&cid) else {
                return GetRecordResponse::BadRequest(xrpc_error("InvalidRequest", "Invalid CID"));
            };
            Some(cid)
        } else {
            None
        };

        let at_uri = format!("at://{}/{}/{}", &*did, &*collection, &*rkey);

        let fr = self
            .cache
            .fetch(at_uri.clone(), {
                let cid = cid.clone();
                let repo_api = self.repo.clone();
                || async move {
                    repo_api
                        .get_record(&did, &collection, &rkey, &cid)
                        .await
                        .map_err(|e| foyer::Error::Other(Box::new(e)))
                }
            })
            .await;

        let entry = match fr {
            Ok(e) => e,
            Err(foyer::Error::Other(e)) => {
                let record_error = match e.downcast::<RecordError>() {
                    Ok(e) => e,
                    Err(e) => {
                        log::error!("error (foyer other) getting cache entry, {e:?}");
                        return GetRecordResponse::ServerError(xrpc_error(
                            "ServerError",
                            "sorry, something went wrong",
                        ));
                    }
                };
                let RecordError::UpstreamBadRequest(ErrorResponseObject { error, message }) =
                    *record_error
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
    // #[oai(path = "/com.atproto.identity.resolveHandle", method = "get")]
    // #[oai(path = "/com.atproto.identity.resolveDid", method = "get")]
    // but these are both not specified to do bidirectional validation, which is what we want to offer
    // com.atproto.identity.resolveIdentity seems right, but requires returning the full did-doc
    // would be nice if there were two queries:
    //  did -> verified handle + pds url
    //  handle -> verified did + pds url
    //
    // we could do horrible things and implement resolveIdentity with only a stripped-down fake did doc
    // but this will *definitely* cause problems because eg. we're not currently storing pubkeys and
    // those are a little bit important
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

pub async fn serve(
    cache: HybridCache<String, CachedRecord>,
    identity: Identity,
    repo: Repo,
    domain: Option<String>,
    acme_contact: Option<String>,
    certs: Option<PathBuf>,
    shutdown: CancellationToken,
    bind: std::net::SocketAddr,
) -> Result<(), ServerError> {
    let repo = Arc::new(repo);
    let api_service = OpenApiService::new(
        Xrpc {
            cache,
            identity,
            repo,
        },
        "Slingshot",
        env!("CARGO_PKG_VERSION"),
    )
    .server(if let Some(ref h) = domain {
        format!("https://{h}")
    } else {
        "http://localhost:3000".to_string()
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

    let mut app = Route::new()
        .at("/", StaticFileEndpoint::new("./static/index.html"))
        .nest("/openapi", api_service.spec_endpoint())
        .nest("/xrpc/", api_service);

    if let Some(domain) = domain {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("alskfjalksdjf");

        app = app.at("/.well-known/did.json", get_did_doc(&domain));

        let mut auto_cert = AutoCert::builder()
            .directory_url(LETS_ENCRYPT_PRODUCTION)
            .domain(&domain);
        if let Some(contact) = acme_contact {
            auto_cert = auto_cert.contact(contact);
        }
        if let Some(certs) = certs {
            auto_cert = auto_cert.cache_path(certs);
        }
        let auto_cert = auto_cert.build().map_err(ServerError::AcmeBuildError)?;

        run(
            TcpListener::bind("0.0.0.0:443").acme(auto_cert),
            app,
            shutdown,
        )
        .await
    } else {
        run(TcpListener::bind(bind.to_string()), app, shutdown).await
    }
}

async fn run<L>(listener: L, app: Route, shutdown: CancellationToken) -> Result<(), ServerError>
where
    L: Listener + 'static,
{
    let app = app
        .with(
            Cors::new()
                .allow_origin_regex("*")
                .allow_methods([Method::GET])
                .allow_credentials(false),
        )
        .with(CatchPanic::new())
        .with(Tracing);
    Server::new(listener)
        .name("slingshot")
        .run_with_graceful_shutdown(app, shutdown.cancelled(), None)
        .await
        .map_err(ServerError::ServerExited)
        .inspect(|()| log::info!("server ended. goodbye."))
}
