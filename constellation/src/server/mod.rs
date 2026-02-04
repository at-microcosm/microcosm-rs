use askama::Template;
use axum::{
    extract::{Query, Request},
    http::{self, header},
    middleware::{self, Next},
    response::{IntoResponse, Json, Response},
    routing::get,
    Router,
};
use axum_metrics::{ExtraMetricLabels, MetricLayer};
use bincode::Options;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, UNIX_EPOCH};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::task::spawn_blocking;
use tokio_util::sync::CancellationToken;

use crate::storage::{LinkReader, Order, StorageStats};
use crate::{CountsByCount, Did, RecordId};

mod acceptable;
mod filters;

use acceptable::{acceptable, ExtractAccept};

const DEFAULT_CURSOR_LIMIT: u64 = 100;
const DEFAULT_CURSOR_LIMIT_MAX: u64 = 1000;

fn get_default_cursor_limit() -> u64 {
    DEFAULT_CURSOR_LIMIT
}

fn to500(e: tokio::task::JoinError) -> http::StatusCode {
    eprintln!("handler error: {e}");
    http::StatusCode::INTERNAL_SERVER_ERROR
}

pub async fn serve<S: LinkReader, A: ToSocketAddrs>(
    store: S,
    addr: A,
    did_web_domain: Option<String>,
    stay_alive: CancellationToken,
) -> anyhow::Result<()> {
    let mut app = Router::new();

    if let Some(d) = did_web_domain {
        app = app.route(
            "/.well-known/did.json",
            get({
                let domain = d.clone();
                move || did_web(domain)
            }),
        )
    }

    let app = app
        .route("/robots.txt", get(robots))
        .route(
            "/",
            get({
                let store = store.clone();
                move |accept| async {
                    spawn_blocking(|| hello(accept, store))
                        .await
                        .map_err(to500)?
                }
            }),
        )
        .route(
            "/xrpc/blue.microcosm.links.getManyToManyCounts",
            get({
                let store = store.clone();
                move |accept, query| async {
                    spawn_blocking(|| get_many_to_many_counts(accept, query, store))
                        .await
                        .map_err(to500)?
                }
            }),
        )
        // deprecated
        .route(
            "/links/count",
            get({
                let store = store.clone();
                move |accept, query| async {
                    spawn_blocking(|| count_links(accept, query, store))
                        .await
                        .map_err(to500)?
                }
            }),
        )
        .route(
            "/xrpc/blue.microcosm.links.getBacklinksCount",
            get({
                let store = store.clone();
                move |accept, query| async {
                    spawn_blocking(|| get_backlink_counts(accept, query, store))
                        .await
                        .map_err(to500)?
                }
            }),
        )
        .route(
            "/links/count/distinct-dids",
            get({
                let store = store.clone();
                move |accept, query| async {
                    spawn_blocking(|| count_distinct_dids(accept, query, store))
                        .await
                        .map_err(to500)?
                }
            }),
        )
        .route(
            "/xrpc/blue.microcosm.links.getBacklinks",
            get({
                let store = store.clone();
                move |accept, query| async {
                    spawn_blocking(|| get_backlinks(accept, query, store))
                        .await
                        .map_err(to500)?
                }
            }),
        )
        .route(
            "/links",
            get({
                let store = store.clone();
                move |accept, query| async {
                    spawn_blocking(|| get_links(accept, query, store))
                        .await
                        .map_err(to500)?
                }
            }),
        )
        .route(
            "/links/distinct-dids",
            get({
                let store = store.clone();
                move |accept, query| async {
                    spawn_blocking(|| get_distinct_dids(accept, query, store))
                        .await
                        .map_err(to500)?
                }
            }),
        )
        .route(
            // deprecated
            "/links/all/count",
            get({
                let store = store.clone();
                move |accept, query| async {
                    spawn_blocking(|| count_all_links(accept, query, store))
                        .await
                        .map_err(to500)?
                }
            }),
        )
        .route(
            "/links/all",
            get({
                let store = store.clone();
                move |accept, query| async {
                    spawn_blocking(|| explore_links(accept, query, store))
                        .await
                        .map_err(to500)?
                }
            }),
        )
        .layer(tower_http::cors::CorsLayer::permissive())
        .layer(middleware::from_fn(add_lables))
        .layer(MetricLayer::default());

    let listener = TcpListener::bind(addr).await?;
    println!("api: listening at http://{:?}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { stay_alive.cancelled().await })
        .await?;

    Ok(())
}

async fn add_lables(request: Request, next: Next) -> Response {
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|o| o.to_str().map(|v| v.to_owned()).ok());
    let user_agent = request.headers().get(header::USER_AGENT).and_then(|ua| {
        ua.to_str()
            .map(|v| {
                if v.starts_with("Mozilla/") {
                    "Mozilla/...".into()
                } else {
                    v.to_owned()
                }
            })
            .ok()
    });

    let mut res = next.run(request).await;

    let mut labels = Vec::new();
    if let Some(o) = origin {
        labels.push(metrics::Label::new("origin", o));
    }
    if let Some(ua) = user_agent {
        labels.push(metrics::Label::new("user_agent", ua));
    }
    res.extensions_mut().insert(ExtraMetricLabels(labels));
    res
}

async fn robots() -> &'static str {
    "\
User-agent: *
Disallow: /links
Disallow: /links/
Disallow: /xrpc/
    "
}

async fn did_web(domain: String) -> impl IntoResponse {
    Json(serde_json::json!({
        "id": format!("did:web:{domain}"),
        "service": [{
            "id": "#constellation",
            "type": "ConstellationGraphService",
            "serviceEndpoint": format!("https://{domain}")
        }]
    }))
}

#[derive(Template, Serialize, Deserialize)]
#[template(path = "hello.html.j2")]
struct HelloReponse {
    help: &'static str,
    days_indexed: Option<u64>,
    stats: StorageStats,
}
fn hello(
    accept: ExtractAccept,
    store: impl LinkReader,
) -> Result<impl IntoResponse, http::StatusCode> {
    let stats = store
        .get_stats()
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let days_indexed = stats
        .started_at
        .map(|c| (UNIX_EPOCH + Duration::from_micros(c)).elapsed())
        .transpose()
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?
        .map(|d| d.as_secs() / 86_400);
    Ok(acceptable(accept, HelloReponse {
        help: "open this URL in a web browser (or request with Accept: text/html) for information about this API.",
        days_indexed,
        stats,
    }))
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GetManyToManyCountsQuery {
    subject: String,
    source: String,
    /// path to the secondary link in the linking record
    path_to_other: String,
    /// filter to linking records (join of the m2m) by these DIDs
    #[serde(default)]
    did: Vec<String>,
    /// filter to specific secondary records
    #[serde(default)]
    other_subject: Vec<String>,
    cursor: Option<OpaqueApiCursor>,
    /// Set the max number of links to return per page of results
    #[serde(default = "get_default_cursor_limit")]
    limit: u64,
}
#[derive(Serialize)]
struct OtherSubjectCount {
    subject: String,
    total: u64,
    distinct: u64,
}
#[derive(Template, Serialize)]
#[template(path = "get-many-to-many-counts.html.j2")]
struct GetManyToManyCountsResponse {
    counts_by_other_subject: Vec<OtherSubjectCount>,
    cursor: Option<OpaqueApiCursor>,
    #[serde(skip_serializing)]
    query: GetManyToManyCountsQuery,
}
fn get_many_to_many_counts(
    accept: ExtractAccept,
    query: axum_extra::extract::Query<GetManyToManyCountsQuery>,
    store: impl LinkReader,
) -> Result<impl IntoResponse, http::StatusCode> {
    let cursor_key = query
        .cursor
        .clone()
        .map(|oc| ApiKeyedCursor::try_from(oc).map_err(|_| http::StatusCode::BAD_REQUEST))
        .transpose()?
        .map(|c| c.next);

    let limit = query.limit;
    if limit > DEFAULT_CURSOR_LIMIT_MAX {
        return Err(http::StatusCode::BAD_REQUEST);
    }

    let filter_dids: HashSet<Did> = HashSet::from_iter(
        query
            .did
            .iter()
            .map(|d| d.trim())
            .filter(|d| !d.is_empty())
            .map(|d| Did(d.to_string())),
    );

    let filter_other_subjects: HashSet<String> = HashSet::from_iter(
        query
            .other_subject
            .iter()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
    );

    let Some((collection, path)) = query.source.split_once(':') else {
        return Err(http::StatusCode::BAD_REQUEST);
    };
    let path = format!(".{path}");

    let path_to_other = format!(".{}", query.path_to_other);

    let paged = store
        .get_many_to_many_counts(
            &query.subject,
            collection,
            &path,
            &path_to_other,
            limit,
            cursor_key,
            &filter_dids,
            &filter_other_subjects,
        )
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let cursor = paged.next.map(|next| ApiKeyedCursor { next }.into());

    let items = paged
        .items
        .into_iter()
        .map(|(subject, total, distinct)| OtherSubjectCount {
            subject,
            total,
            distinct,
        })
        .collect();

    Ok(acceptable(
        accept,
        GetManyToManyCountsResponse {
            counts_by_other_subject: items,
            cursor,
            query: (*query).clone(),
        },
    ))
}

#[derive(Clone, Deserialize)]
struct GetLinksCountQuery {
    target: String,
    collection: String,
    path: String,
}
#[derive(Template, Serialize)]
#[template(path = "links-count.html.j2")]
struct GetLinksCountResponse {
    total: u64,
    #[serde(skip_serializing)]
    query: GetLinksCountQuery,
}
fn count_links(
    accept: ExtractAccept,
    query: Query<GetLinksCountQuery>,
    store: impl LinkReader,
) -> Result<impl IntoResponse, http::StatusCode> {
    let total = store
        .get_count(&query.target, &query.collection, &query.path)
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(acceptable(
        accept,
        GetLinksCountResponse {
            total,
            query: (*query).clone(),
        },
    ))
}

#[derive(Clone, Deserialize)]
struct GetItemsCountQuery {
    subject: String,
    source: String,
}
#[derive(Template, Serialize)]
#[template(path = "get-backlinks-count.html.j2")]
struct GetItemsCountResponse {
    total: u64,
    #[serde(skip_serializing)]
    query: GetItemsCountQuery,
}
fn get_backlink_counts(
    accept: ExtractAccept,
    query: axum_extra::extract::Query<GetItemsCountQuery>,
    store: impl LinkReader,
) -> Result<impl IntoResponse, http::StatusCode> {
    let Some((collection, path)) = query.source.split_once(':') else {
        return Err(http::StatusCode::BAD_REQUEST);
    };
    let path = format!(".{path}");
    let total = store
        .get_count(&query.subject, collection, &path)
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(acceptable(
        accept,
        GetItemsCountResponse {
            total,
            query: (*query).clone(),
        },
    ))
}

#[derive(Clone, Deserialize)]
struct GetDidsCountQuery {
    target: String,
    collection: String,
    path: String,
}
#[derive(Template, Serialize)]
#[template(path = "dids-count.html.j2")]
struct GetDidsCountResponse {
    total: u64,
    #[serde(skip_serializing)]
    query: GetDidsCountQuery,
}
fn count_distinct_dids(
    accept: ExtractAccept,
    query: Query<GetDidsCountQuery>,
    store: impl LinkReader,
) -> Result<impl IntoResponse, http::StatusCode> {
    let total = store
        .get_distinct_did_count(&query.target, &query.collection, &query.path)
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(acceptable(
        accept,
        GetDidsCountResponse {
            total,
            query: (*query).clone(),
        },
    ))
}

#[derive(Clone, Deserialize)]
struct GetBacklinksQuery {
    /// The link target
    ///
    /// can be an AT-URI, plain DID, or regular URI
    subject: String,
    /// Filter links only from this link source
    ///
    /// eg.: `app.bsky.feed.like:subject.uri`
    source: String,
    cursor: Option<OpaqueApiCursor>,
    /// Filter links only from these DIDs
    ///
    /// include multiple times to filter by multiple source DIDs
    #[serde(default)]
    did: Vec<String>,
    /// Set the max number of links to return per page of results
    #[serde(default = "get_default_cursor_limit")]
    limit: u64,
    /// Allow returning links in reverse order (default: false)
    #[serde(default)]
    reverse: bool,
}
#[derive(Template, Serialize)]
#[template(path = "get-backlinks.html.j2")]
struct GetBacklinksResponse {
    total: u64,
    records: Vec<RecordId>,
    cursor: Option<OpaqueApiCursor>,
    #[serde(skip_serializing)]
    query: GetBacklinksQuery,
    #[serde(skip_serializing)]
    collection: String,
    #[serde(skip_serializing)]
    path: String,
}
fn get_backlinks(
    accept: ExtractAccept,
    query: axum_extra::extract::Query<GetBacklinksQuery>, // supports multiple param occurrences
    store: impl LinkReader,
) -> Result<impl IntoResponse, http::StatusCode> {
    let until = query
        .cursor
        .clone()
        .map(|oc| ApiCursor::try_from(oc).map_err(|_| http::StatusCode::BAD_REQUEST))
        .transpose()?
        .map(|c| c.next);

    let limit = query.limit;
    if limit > DEFAULT_CURSOR_LIMIT_MAX {
        return Err(http::StatusCode::BAD_REQUEST);
    }

    let filter_dids: HashSet<Did> = HashSet::from_iter(
        query
            .did
            .iter()
            .map(|d| d.trim())
            .filter(|d| !d.is_empty())
            .map(|d| Did(d.to_string())),
    );

    let Some((collection, path)) = query.source.split_once(':') else {
        return Err(http::StatusCode::BAD_REQUEST);
    };
    let path = format!(".{path}");

    let order = if query.reverse {
        Order::OldestToNewest
    } else {
        Order::NewestToOldest
    };

    let paged = store
        .get_links(
            &query.subject,
            collection,
            &path,
            order,
            limit,
            until,
            &filter_dids,
        )
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let cursor = paged.next.map(|next| {
        ApiCursor {
            version: paged.version,
            next,
        }
        .into()
    });

    Ok(acceptable(
        accept,
        GetBacklinksResponse {
            total: paged.total,
            records: paged.items,
            cursor,
            query: (*query).clone(),
            collection: collection.to_string(),
            path,
        },
    ))
}

#[derive(Clone, Deserialize)]
struct GetLinkItemsQuery {
    target: String,
    collection: String,
    path: String,
    cursor: Option<OpaqueApiCursor>,
    /// Filter links only from these DIDs
    ///
    /// include multiple times to filter by multiple source DIDs
    #[serde(default)]
    did: Vec<String>,
    /// [deprecated] Filter links only from these DIDs
    ///
    /// format: comma-separated sequence of DIDs
    ///
    /// errors: if `did` parameter is also present
    ///
    /// deprecated: use `did`, which can be repeated multiple times
    from_dids: Option<String>, // comma separated: gross
    #[serde(default = "get_default_cursor_limit")]
    limit: u64,
}
#[derive(Template, Serialize)]
#[template(path = "links.html.j2")]
struct GetLinkItemsResponse {
    // what does staleness mean?
    // - new links have appeared. would be nice to offer a `since` cursor to fetch these. and/or,
    // - links have been deleted. hmm.
    total: u64,
    linking_records: Vec<RecordId>,
    cursor: Option<OpaqueApiCursor>,
    #[serde(skip_serializing)]
    query: GetLinkItemsQuery,
}
fn get_links(
    accept: ExtractAccept,
    query: axum_extra::extract::Query<GetLinkItemsQuery>, // supports multiple param occurrences
    store: impl LinkReader,
) -> Result<impl IntoResponse, http::StatusCode> {
    let until = query
        .cursor
        .clone()
        .map(|oc| ApiCursor::try_from(oc).map_err(|_| http::StatusCode::BAD_REQUEST))
        .transpose()?
        .map(|c| c.next);

    let limit = query.limit;
    if limit > DEFAULT_CURSOR_LIMIT_MAX {
        return Err(http::StatusCode::BAD_REQUEST);
    }

    let mut filter_dids: HashSet<Did> = HashSet::from_iter(
        query
            .did
            .iter()
            .map(|d| d.trim())
            .filter(|d| !d.is_empty())
            .map(|d| Did(d.to_string())),
    );

    if let Some(comma_joined) = &query.from_dids {
        if !filter_dids.is_empty() {
            return Err(http::StatusCode::BAD_REQUEST);
        }
        for did in comma_joined.split(',') {
            filter_dids.insert(Did(did.to_string()));
        }
    }

    let paged = store
        .get_links(
            &query.target,
            &query.collection,
            &query.path,
            Order::NewestToOldest,
            limit,
            until,
            &filter_dids,
        )
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let cursor = paged.next.map(|next| {
        ApiCursor {
            version: paged.version,
            next,
        }
        .into()
    });

    Ok(acceptable(
        accept,
        GetLinkItemsResponse {
            total: paged.total,
            linking_records: paged.items,
            cursor,
            query: (*query).clone(),
        },
    ))
}

#[derive(Clone, Deserialize)]
struct GetDidItemsQuery {
    target: String,
    collection: String,
    path: String,
    cursor: Option<OpaqueApiCursor>,
    limit: Option<u64>,
    // TODO: allow reverse (er, forward) order as well
}
#[derive(Template, Serialize)]
#[template(path = "dids.html.j2")]
struct GetDidItemsResponse {
    // what does staleness mean?
    // - new links have appeared. would be nice to offer a `since` cursor to fetch these. and/or,
    // - links have been deleted. hmm.
    total: u64,
    linking_dids: Vec<Did>,
    cursor: Option<OpaqueApiCursor>,
    #[serde(skip_serializing)]
    query: GetDidItemsQuery,
}
fn get_distinct_dids(
    accept: ExtractAccept,
    query: Query<GetDidItemsQuery>,
    store: impl LinkReader,
) -> Result<impl IntoResponse, http::StatusCode> {
    let until = query
        .cursor
        .clone()
        .map(|oc| ApiCursor::try_from(oc).map_err(|_| http::StatusCode::BAD_REQUEST))
        .transpose()?
        .map(|c| c.next);

    let limit = query.limit.unwrap_or(DEFAULT_CURSOR_LIMIT);
    if limit > DEFAULT_CURSOR_LIMIT_MAX {
        return Err(http::StatusCode::BAD_REQUEST);
    }

    let paged = store
        .get_distinct_dids(&query.target, &query.collection, &query.path, limit, until)
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let cursor = paged.next.map(|next| {
        ApiCursor {
            version: paged.version,
            next,
        }
        .into()
    });

    Ok(acceptable(
        accept,
        GetDidItemsResponse {
            total: paged.total,
            linking_dids: paged.items,
            cursor,
            query: (*query).clone(),
        },
    ))
}

#[derive(Clone, Deserialize)]
struct GetAllLinksQuery {
    target: String,
}
#[derive(Template, Serialize)]
#[template(path = "links-all-count.html.j2")]
struct GetAllLinksResponse {
    links: HashMap<String, HashMap<String, u64>>,
    #[serde(skip_serializing)]
    query: GetAllLinksQuery,
}
fn count_all_links(
    accept: ExtractAccept,
    query: Query<GetAllLinksQuery>,
    store: impl LinkReader,
) -> Result<impl IntoResponse, http::StatusCode> {
    let links = store
        .get_all_record_counts(&query.target)
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(acceptable(
        accept,
        GetAllLinksResponse {
            links,
            query: (*query).clone(),
        },
    ))
}

#[derive(Clone, Deserialize)]
struct ExploreLinksQuery {
    target: String,
}
#[derive(Template, Serialize)]
#[template(path = "explore-links.html.j2")]
struct ExploreLinksResponse {
    links: HashMap<String, HashMap<String, CountsByCount>>,
    #[serde(skip_serializing)]
    query: ExploreLinksQuery,
}
fn explore_links(
    accept: ExtractAccept,
    query: Query<ExploreLinksQuery>,
    store: impl LinkReader,
) -> Result<impl IntoResponse, http::StatusCode> {
    let links = store
        .get_all_counts(&query.target)
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(acceptable(
        accept,
        ExploreLinksResponse {
            links,
            query: (*query).clone(),
        },
    ))
}

#[serde_as]
#[derive(Clone, Serialize, Deserialize)] // for json
struct OpaqueApiCursor(#[serde_as(as = "serde_with::hex::Hex")] Vec<u8>);

#[derive(Serialize, Deserialize)] // for bincode
struct ApiCursor {
    version: (u64, u64), // (collection length, deleted item count)
    next: u64,
}

impl TryFrom<OpaqueApiCursor> for ApiCursor {
    type Error = bincode::Error;

    fn try_from(item: OpaqueApiCursor) -> Result<Self, Self::Error> {
        bincode::DefaultOptions::new().deserialize(&item.0)
    }
}

impl From<ApiCursor> for OpaqueApiCursor {
    fn from(item: ApiCursor) -> Self {
        OpaqueApiCursor(bincode::DefaultOptions::new().serialize(&item).unwrap())
    }
}

#[derive(Serialize, Deserialize)] // for bincode
struct ApiKeyedCursor {
    next: String, // the key
}

impl TryFrom<OpaqueApiCursor> for ApiKeyedCursor {
    type Error = bincode::Error;

    fn try_from(item: OpaqueApiCursor) -> Result<Self, Self::Error> {
        bincode::DefaultOptions::new().deserialize(&item.0)
    }
}

impl From<ApiKeyedCursor> for OpaqueApiCursor {
    fn from(item: ApiKeyedCursor) -> Self {
        OpaqueApiCursor(bincode::DefaultOptions::new().serialize(&item).unwrap())
    }
}
