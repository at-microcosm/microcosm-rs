use crate::ClientMessage;
use crate::error::ServerError;
use crate::subscriber::Subscriber;
use dropshot::{
    ApiDescription, ApiEndpointBodyContentType, Body, ConfigDropshot, ConfigLogging,
    ConfigLoggingLevel, ExtractorMetadata, HttpError, HttpResponse, Query, RequestContext,
    ServerBuilder, ServerContext, SharedExtractor, WebsocketConnection, channel, endpoint,
};
use http::{
    Response, StatusCode,
    header::{ORIGIN, USER_AGENT},
};
use metrics::{counter, histogram};
use std::sync::Arc;

use async_trait::async_trait;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tokio::sync::broadcast;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::protocol::{Role, WebSocketConfig};
use tokio_util::sync::CancellationToken;

const INDEX_HTML: &str = include_str!("../static/index.html");
const FAVICON: &[u8] = include_bytes!("../static/favicon.ico");

pub async fn serve(
    b: broadcast::Sender<Arc<ClientMessage>>,
    d: broadcast::Sender<Arc<ClientMessage>>,
    shutdown: CancellationToken,
    bind: std::net::SocketAddr,
) -> Result<(), ServerError> {
    let config_logging = ConfigLogging::StderrTerminal {
        level: ConfigLoggingLevel::Info,
    };

    let log = config_logging
        .to_logger("example-basic")
        .map_err(ServerError::ConfigLogError)?;

    let mut api = ApiDescription::new();
    api.register(index).unwrap();
    api.register(favicon).unwrap();
    api.register(openapi).unwrap();
    api.register(subscribe).unwrap();

    // TODO: put spec in a once cell / lazy lock thing?
    let spec = Arc::new(
        api.openapi(
            "Spacedust",
            env!("CARGO_PKG_VERSION")
                .parse()
                .inspect_err(|e| {
                    eprintln!("failed to parse cargo package version for openapi: {e:?}")
                })
                .unwrap_or(semver::Version::new(0, 0, 1)),
        )
        .description("A configurable ATProto notifications firehose.")
        .contact_name("part of @microcosm.blue")
        .contact_url("https://microcosm.blue")
        .json()
        .map_err(ServerError::OpenApiJsonFail)?,
    );

    let sub_shutdown = shutdown.clone();
    let ctx = Context {
        spec,
        b,
        d,
        shutdown: sub_shutdown,
    };

    let server = ServerBuilder::new(api, ctx, log)
        .config(ConfigDropshot {
            bind_address: bind,
            ..Default::default()
        })
        .start()?;

    tokio::select! {
        s = server.wait_for_shutdown() => {
            s.map_err(ServerError::ServerExited)?;
            log::info!("server shut down normally.");
        },
        _ = shutdown.cancelled() => {
            log::info!("shutting down: closing server");
            server.close().await.map_err(ServerError::BadClose)?;
        },
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct Context {
    pub spec: Arc<serde_json::Value>,
    pub b: broadcast::Sender<Arc<ClientMessage>>,
    pub d: broadcast::Sender<Arc<ClientMessage>>,
    pub shutdown: CancellationToken,
}

async fn instrument_handler<T, H, R>(ctx: &RequestContext<T>, handler: H) -> Result<R, HttpError>
where
    R: HttpResponse,
    H: Future<Output = Result<R, HttpError>>,
    T: ServerContext,
{
    let start = Instant::now();
    let result = handler.await;
    let latency = start.elapsed();
    let status_code = match &result {
        Ok(response) => response.status_code(),
        Err(e) => e.status_code.as_status(),
    }
    .as_str() // just the number (.to_string()'s Display does eg `200 OK`)
    .to_string();
    let endpoint = ctx.endpoint.operation_id.clone();
    let headers = ctx.request.headers();
    let origin = headers
        .get(ORIGIN)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let ua = headers
        .get(USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .map(|ua| {
            if ua.starts_with("Mozilla/5.0 ") {
                "browser"
            } else {
                ua
            }
        })
        .unwrap_or("")
        .to_string();
    counter!("server_requests_total",
        "endpoint" => endpoint.clone(),
        "origin" => origin,
        "ua" => ua,
        "status_code" => status_code,
    )
    .increment(1);
    histogram!("server_handler_latency", "endpoint" => endpoint).record(latency.as_micros() as f64);
    result
}

use dropshot::{HttpResponseHeaders, HttpResponseOk};

pub type OkCorsResponse<T> = Result<HttpResponseHeaders<HttpResponseOk<T>>, HttpError>;

/// Helper for constructing Ok responses: return OkCors(T).into()
/// (not happy with this yet)
pub struct OkCors<T: Serialize + JsonSchema + Send + Sync>(pub T);

impl<T> From<OkCors<T>> for OkCorsResponse<T>
where
    T: Serialize + JsonSchema + Send + Sync,
{
    fn from(ok: OkCors<T>) -> OkCorsResponse<T> {
        let mut res = HttpResponseHeaders::new_unnamed(HttpResponseOk(ok.0));
        res.headers_mut()
            .insert("access-control-allow-origin", "*".parse().unwrap());
        Ok(res)
    }
}

// TODO: cors for HttpError

/// Serve index page as html
#[endpoint {
    method = GET,
    path = "/",
    /*
     * not useful to have this in openapi
     */
    unpublished = true,
}]
async fn index(ctx: RequestContext<Context>) -> Result<Response<Body>, HttpError> {
    instrument_handler(&ctx, async {
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "text/html")
            .body(INDEX_HTML.into())?)
    })
    .await
}

/// Serve index page as html
#[endpoint {
    method = GET,
    path = "/favicon.ico",
    /*
     * not useful to have this in openapi
     */
    unpublished = true,
}]
async fn favicon(ctx: RequestContext<Context>) -> Result<Response<Body>, HttpError> {
    instrument_handler(&ctx, async {
        Ok(Response::builder()
            .status(StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "image/x-icon")
            .body(FAVICON.to_vec().into())?)
    })
    .await
}

/// Meta: get the openapi spec for this api
#[endpoint {
    method = GET,
    path = "/openapi",
    /*
     * not useful to have this in openapi
     */
    unpublished = true,
}]
async fn openapi(ctx: RequestContext<Context>) -> OkCorsResponse<serde_json::Value> {
    instrument_handler(&ctx, async {
        let spec = (*ctx.context().spec).clone();
        OkCors(spec).into()
    })
    .await
}

/// The real type that gets deserialized
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct MultiSubscribeQuery {
    #[serde(default)]
    pub wanted_subjects: HashSet<String>,
    #[serde(default)]
    pub wanted_subject_dids: HashSet<String>,
    #[serde(default)]
    pub wanted_sources: HashSet<String>,
}
/// The fake corresponding type for docs that dropshot won't freak out about a
/// vec for
#[derive(Deserialize, JsonSchema)]
#[allow(dead_code)]
#[serde(rename_all = "camelCase")]
struct MultiSubscribeQueryForDocs {
    /// One or more at-uris to receive links about
    ///
    /// The at-uri must be url-encoded
    ///
    /// Pass this parameter multiple times to specify multiple collections, like
    /// `wantedSubjects=[...]&wantedSubjects=[...]`
    pub wanted_subjects: String,
    /// One or more DIDs to receive links about
    ///
    /// Pass this parameter multiple times to specify multiple collections
    pub wanted_subject_dids: String,
    /// One or more link sources to receive links about
    ///
    /// TODO: docs about link sources
    ///
    /// eg, a bluesky like's link source: `app.bsky.feed.like:subject.uri`
    ///
    /// Pass this parameter multiple times to specify multiple sources
    pub wanted_sources: String,
}

// The `SharedExtractor` implementation for Query<QueryType> describes how to
// construct an instance of `Query<QueryType>` from an HTTP request: namely, by
// parsing the query string to an instance of `QueryType`.
#[async_trait]
impl SharedExtractor for MultiSubscribeQuery {
    async fn from_request<Context: ServerContext>(
        ctx: &RequestContext<Context>,
    ) -> Result<MultiSubscribeQuery, HttpError> {
        let raw_query = ctx.request.uri().query().unwrap_or("");
        let q = serde_qs::from_str(raw_query).map_err(|e| {
            HttpError::for_bad_request(None, format!("unable to parse query string: {e}"))
        })?;
        Ok(q)
    }

    fn metadata(body_content_type: ApiEndpointBodyContentType) -> ExtractorMetadata {
        // HACK: query type switcheroo: passing MultiSubscribeQuery to
        // `metadata` would "helpfully" panic because dropshot believes we can
        // only have scalar types in a  query.
        //
        // so instead we have a fake second type whose only job is to look the
        // same as MultiSubscribeQuery exept that it has `String` instead of
        // `Vec<String>`, which dropshot will accept, and generate ~close-enough
        // docs for.
        <Query<MultiSubscribeQueryForDocs> as SharedExtractor>::metadata(body_content_type)
    }
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
struct ScalarSubscribeQuery {
    /// Bypass the 21-sec delay buffer
    ///
    /// By default, spacedust holds all firehose links for 21 seconds before
    /// emitting them, to prevent quickly- undone interactions from generating
    /// notifications.
    ///
    /// Setting `instant` to true bypasses this buffer, allowing faster (and
    /// noisier) notification delivery.
    ///
    /// Typically [a little less than 1%](https://bsky.app/profile/bad-example.com/post/3ls32wctsrs2l)
    /// of links links get deleted within 21s of being created.
    #[serde(default)]
    pub instant: bool,
}

#[channel {
    protocol = WEBSOCKETS,
    path = "/subscribe",
}]
async fn subscribe(
    reqctx: RequestContext<Context>,
    query: MultiSubscribeQuery,
    scalar_query: Query<ScalarSubscribeQuery>,
    upgraded: WebsocketConnection,
) -> dropshot::WebsocketChannelResult {
    let ws = tokio_tungstenite::WebSocketStream::from_raw_socket(
        upgraded.into_inner(),
        Role::Server,
        Some(WebSocketConfig::default().max_message_size(
            Some(10 * 2_usize.pow(20)), // 10MiB, matching jetstream
        )),
    )
    .await;

    let Context { b, d, shutdown, .. } = reqctx.context();
    let sub_token = shutdown.child_token();

    let q = scalar_query.into_inner();
    let subscription = if q.instant { b } else { d }.subscribe();
    log::info!("starting subscriber with broadcast: instant={}", q.instant);

    Subscriber::new(query, sub_token)
        .start(ws, subscription)
        .await
        .map_err(|e| format!("boo: {e:?}"))?;

    Ok(())
}
