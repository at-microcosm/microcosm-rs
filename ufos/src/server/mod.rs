mod collections_query;
mod cors;

use crate::index_html::INDEX_HTML;
use crate::storage::StoreReader;
use crate::store_types::{HourTruncatedCursor, WeekTruncatedCursor};
use crate::{
    ConsumerInfo, Cursor, JustCount, Nsid, NsidCount, NsidPrefix, OrderCollectionsBy, PrefixChild,
    UFOsRecord,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use collections_query::MultiCollectionQuery;
use cors::{OkCors, OkCorsResponse};
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::Body;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::HttpError;
use dropshot::HttpResponse;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::ServerBuilder;
use dropshot::ServerContext;
use http::{
    header::{ORIGIN, USER_AGENT},
    Response, StatusCode,
};
use metrics::{counter, describe_counter, describe_histogram, histogram, Unit};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn describe_metrics() {
    describe_counter!(
        "server_requests_total",
        Unit::Count,
        "total requests handled"
    );
    describe_histogram!(
        "server_handler_latency",
        Unit::Microseconds,
        "time to respond to a request in microseconds, excluding dropshot overhead"
    );
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
        Err(ref e) => e.status_code.as_status(),
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

struct Context {
    pub spec: Arc<serde_json::Value>,
    storage: Box<dyn StoreReader>,
}

fn dt_to_cursor(dt: DateTime<Utc>) -> Result<HourTruncatedCursor, HttpError> {
    let t = dt.timestamp_micros();
    if t < 0 {
        Err(HttpError::for_bad_request(None, "timestamp too old".into()))
    } else {
        let t = t as u64;
        let t_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        const ONE_HOUR: u64 = 60 * 60 * 1_000_000;
        if t > t_now && (t - t_now > 2 * ONE_HOUR) {
            Err(HttpError::for_bad_request(None, "future timestamp".into()))
        } else {
            Ok(HourTruncatedCursor::truncate_raw_u64(t))
        }
    }
}

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

/// Meta: get the openapi spec for this api
#[endpoint {
    method = GET,
    path = "/openapi",
    /*
     * not useful to have this in openapi
     */
    unpublished = true,
}]
async fn get_openapi(ctx: RequestContext<Context>) -> OkCorsResponse<serde_json::Value> {
    instrument_handler(&ctx, async {
        let spec = (*ctx.context().spec).clone();
        OkCors(spec).into()
    })
    .await
}

#[derive(Debug, Serialize, JsonSchema)]
struct MetaInfo {
    storage_name: String,
    storage: serde_json::Value,
    consumer: ConsumerInfo,
}
/// UFOs meta-info
#[endpoint {
    method = GET,
    path = "/meta"
}]
async fn get_meta_info(ctx: RequestContext<Context>) -> OkCorsResponse<MetaInfo> {
    let Context { storage, .. } = ctx.context();
    let failed_to_get =
        |what| move |e| HttpError::for_internal_error(format!("failed to get {what}: {e:?}"));

    instrument_handler(&ctx, async {
        let storage_info = storage
            .get_storage_stats()
            .await
            .map_err(failed_to_get("storage info"))?;

        let consumer = storage
            .get_consumer_info()
            .await
            .map_err(failed_to_get("consumer info"))?;

        OkCors(MetaInfo {
            storage_name: storage.name(),
            storage: storage_info,
            consumer,
        })
        .into()
    })
    .await
}

// TODO: replace with normal (🙃) multi-qs value somehow
fn to_multiple_nsids(s: &str) -> Result<HashSet<Nsid>, String> {
    let mut out = HashSet::new();
    for collection in s.split(',') {
        let Ok(nsid) = Nsid::new(collection.to_string()) else {
            return Err(format!("collection {collection:?} was not a valid NSID"));
        };
        out.insert(nsid);
    }
    Ok(out)
}

#[derive(Debug, Deserialize, JsonSchema)]
struct RecordsCollectionsQuery {
    collection: Option<String>, // JsonSchema not implemented for Nsid :(
}
#[derive(Debug, Serialize, JsonSchema)]
struct ApiRecord {
    did: String,
    collection: String,
    rkey: String,
    record: Box<serde_json::value::RawValue>,
    time_us: u64,
}
impl From<UFOsRecord> for ApiRecord {
    fn from(ufo: UFOsRecord) -> Self {
        Self {
            did: ufo.did.to_string(),
            collection: ufo.collection.to_string(),
            rkey: ufo.rkey.to_string(),
            record: ufo.record,
            time_us: ufo.cursor.to_raw_u64(),
        }
    }
}
/// Record samples
///
/// Get most recent records seen in the firehose, by collection NSID
///
/// Multiple collections are supported. They will be delivered in one big array with no
/// specified order.
#[endpoint {
    method = GET,
    path = "/records",
}]
async fn get_records_by_collections(
    ctx: RequestContext<Context>,
    collection_query: Query<RecordsCollectionsQuery>,
) -> OkCorsResponse<Vec<ApiRecord>> {
    let Context { storage, .. } = ctx.context();
    instrument_handler(&ctx, async {
        let mut limit = 42;
        let query = collection_query.into_inner();
        let collections = if let Some(provided_collection) = query.collection {
            to_multiple_nsids(&provided_collection)
                .map_err(|reason| HttpError::for_bad_request(None, reason))?
        } else {
            limit = 12;
            let min_time_ago = SystemTime::now() - Duration::from_secs(86_400 * 3); // we want at least 3 days of data
            let since: WeekTruncatedCursor = Cursor::at(min_time_ago).into();
            let (collections, _) = storage
                .get_collections(
                    1000,
                    Default::default(),
                    Some(since.try_as().unwrap()),
                    None,
                )
                .await
                .map_err(|e| HttpError::for_internal_error(e.to_string()))?;
            collections
                .into_iter()
                .map(|c| Nsid::new(c.nsid).unwrap())
                .collect()
        };

        let records = storage
            .get_records_by_collections(collections, limit, true)
            .await
            .map_err(|e| HttpError::for_internal_error(e.to_string()))?
            .into_iter()
            .map(|r| r.into())
            .collect();

        OkCors(records).into()
    })
    .await
}

#[derive(Debug, Deserialize, JsonSchema)]
struct CollectionsStatsQuery {
    /// Limit stats to those seen after this UTC datetime
    ///
    /// default: 1 week ago
    since: Option<DateTime<Utc>>,
    /// Limit stats to those seen before this UTC datetime
    ///
    /// default: now
    until: Option<DateTime<Utc>>,
}
/// Collection stats
///
/// Get record statistics for collections during a specific time period.
///
/// Note: the statistics are "rolled up" into hourly buckets in the background,
/// so the data here can be as stale as that background task is behind. See the
/// meta info endpoint to find out how up-to-date the rollup currently is. (In
/// general it sholud be pretty close to live)
#[endpoint {
    method = GET,
    path = "/collections/stats"
}]
async fn get_collection_stats(
    ctx: RequestContext<Context>,
    collections_query: MultiCollectionQuery,
    query: Query<CollectionsStatsQuery>,
) -> OkCorsResponse<HashMap<String, JustCount>> {
    let Context { storage, .. } = ctx.context();

    instrument_handler(&ctx, async {
        let q = query.into_inner();
        let collections: HashSet<Nsid> = collections_query.try_into()?;

        let since = q.since.map(dt_to_cursor).transpose()?.unwrap_or_else(|| {
            let week_ago_secs = 7 * 86_400;
            let week_ago = SystemTime::now() - Duration::from_secs(week_ago_secs);
            Cursor::at(week_ago).into()
        });

        let until = q.until.map(dt_to_cursor).transpose()?;

        let mut seen_by_collection = HashMap::with_capacity(collections.len());

        for collection in &collections {
            let counts = storage
                .get_collection_counts(collection, since, until)
                .await
                .map_err(|e| HttpError::for_internal_error(format!("boooo: {e:?}")))?;

            seen_by_collection.insert(collection.to_string(), counts);
        }

        OkCors(seen_by_collection).into()
    })
    .await
}

#[derive(Debug, Serialize, JsonSchema)]
struct CollectionsResponse {
    /// Each known collection and its associated statistics
    ///
    /// The order is unspecified.
    collections: Vec<NsidCount>,
    /// Include in a follow-up request to get the next page of results, if more are available
    cursor: Option<String>,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum CollectionsQueryOrder {
    RecordsCreated,
    DidsEstimate,
}
impl From<&CollectionsQueryOrder> for OrderCollectionsBy {
    fn from(q: &CollectionsQueryOrder) -> Self {
        match q {
            CollectionsQueryOrder::RecordsCreated => OrderCollectionsBy::RecordsCreated,
            CollectionsQueryOrder::DidsEstimate => OrderCollectionsBy::DidsEstimate,
        }
    }
}
#[derive(Debug, Deserialize, JsonSchema)]
struct CollectionsQuery {
    /// The maximum number of collections to return in one request.
    ///
    /// Default: `100` normally, `32` if `order` is specified.
    #[schemars(range(min = 1, max = 200))]
    limit: Option<usize>,
    /// Get a paginated response with more collections.
    ///
    /// Always omit the cursor for the first request. If more collections than the limit are available, the response will contain a non-null `cursor` to include with the next request.
    ///
    /// `cursor` is mutually exclusive with `order`.
    cursor: Option<String>,
    /// Limit collections and statistics to those seen after this UTC datetime
    since: Option<DateTime<Utc>>,
    /// Limit collections and statistics to those seen before this UTC datetime
    until: Option<DateTime<Utc>>,
    /// Get a limited, sorted list
    ///
    /// Mutually exclusive with `cursor` -- sorted results cannot be paged.
    order: Option<CollectionsQueryOrder>,
}

/// List collections
///
/// With statistics.
///
/// ## To fetch a full list:
///
/// Omit the `order` parameter and page through the results using the `cursor`. There have been a lot of collections seen in the ATmosphere, well over 400 at time of writing, so you *will* need to make a series of paginaged requests with `cursor`s to get them all.
///
/// The set of collections across multiple requests is not guaranteed to be a perfectly consistent snapshot:
///
/// - all collection NSIDs observed before the first request will be included in the results
///
/// - *new* NSIDs observed in the firehose *while paging* might be included or excluded from the final set
///
/// - no duplicate NSIDs will occur in the combined results
///
/// In practice this is close enough for most use-cases to not worry about.
///
/// ## To fetch the top collection NSIDs:
///
/// Specify the `order` parameter (must be either `records-created` or `did-estimate`). Note that ordered results cannot be paged.
///
/// All statistics are bucketed hourly, so the most granular effecitve time boundary for `since` and `until` is one hour.
#[endpoint {
    method = GET,
    path = "/collections"
}]
async fn get_collections(
    ctx: RequestContext<Context>,
    query: Query<CollectionsQuery>,
) -> OkCorsResponse<CollectionsResponse> {
    let Context { storage, .. } = ctx.context();
    let q = query.into_inner();

    instrument_handler(&ctx, async {
        if q.cursor.is_some() && q.order.is_some() {
            let msg =
                "`cursor` is mutually exclusive with `order`. ordered results cannot be paged.";
            return Err(HttpError::for_bad_request(None, msg.to_string()));
        }

        let order = if let Some(ref o) = q.order {
            o.into()
        } else {
            let cursor = q
                .cursor
                .and_then(|c| if c.is_empty() { None } else { Some(c) })
                .map(|c| URL_SAFE_NO_PAD.decode(&c))
                .transpose()
                .map_err(|e| HttpError::for_bad_request(None, format!("invalid cursor: {e:?}")))?;
            OrderCollectionsBy::Lexi { cursor }
        };

        let limit = match (q.limit, q.order) {
            (Some(limit), _) => limit,
            (None, Some(_)) => 32,
            (None, None) => 100,
        };

        if !(1..=200).contains(&limit) {
            let msg = format!("limit not in 1..=200: {limit}");
            return Err(HttpError::for_bad_request(None, msg));
        }

        let since = q.since.map(dt_to_cursor).transpose()?;
        let until = q.until.map(dt_to_cursor).transpose()?;

        let (collections, next_cursor) = storage
            .get_collections(limit, order, since, until)
            .await
            .map_err(|e| HttpError::for_internal_error(format!("oh shoot: {e:?}")))?;

        let next_cursor = next_cursor.map(|c| URL_SAFE_NO_PAD.encode(c));

        OkCors(CollectionsResponse {
            collections,
            cursor: next_cursor,
        })
        .into()
    })
    .await
}

#[derive(Debug, Serialize, JsonSchema)]
struct PrefixResponse {
    /// Note that total may not include counts beyond the current page (TODO)
    total: JustCount,
    children: Vec<PrefixChild>,
    /// Include in a follow-up request to get the next page of results, if more are available
    cursor: Option<String>,
}
#[derive(Debug, Deserialize, JsonSchema)]
struct PrefixQuery {
    ///
    /// The final segment of a collection NSID is the `name`, and everything before it is called its `group`. eg:
    ///
    /// - `app.bsky.feed.post` and `app.bsky.feed.like` are both in the _lexicon group_ "`app.bsky.feed`".
    ///
    prefix: String,
    /// The maximum number of collections to return in one request.
    ///
    /// The number of items actually returned may be less than the limit. If paginating, this does **not** indicate that no
    /// more items are available! Check if the `cursor` in the response is `null` to determine the end of items.
    ///
    /// Default: `100` normally, `32` if `order` is specified.
    #[schemars(range(min = 1, max = 200))]
    limit: Option<usize>,
    /// Get a paginated response with more collections.
    ///
    /// Always omit the cursor for the first request. If more collections than the limit are available, the response will contain a non-null `cursor` to include with the next request.
    ///
    /// `cursor` is mutually exclusive with `order`.
    cursor: Option<String>,
    /// Limit collections and statistics to those seen after this UTC datetime
    ///
    /// Default: all-time
    since: Option<DateTime<Utc>>,
    /// Limit collections and statistics to those seen before this UTC datetime
    ///
    /// Default: now
    until: Option<DateTime<Utc>>,
    /// Get a limited, sorted list
    ///
    /// Mutually exclusive with `cursor` -- sorted results cannot be paged.
    order: Option<CollectionsQueryOrder>,
}
/// Prefix-filter collections list
///
/// This endpoint enumerates all collection NSIDs for a lexicon group.
///
/// ## To fetch a full list:
///
/// Omit the `order` parameter and page through the results using the `cursor`. There have been a lot of collections seen in the ATmosphere, well over 400 at time of writing, so you *will* need to make a series of paginaged requests with `cursor`s to get them all.
///
/// The set of collections across multiple requests is not guaranteed to be a perfectly consistent snapshot:
///
/// - all collection NSIDs observed before the first request will be included in the results
///
/// - *new* NSIDs observed in the firehose *while paging* might be included or excluded from the final set
///
/// - no duplicate NSIDs will occur in the combined results
///
/// In practice this is close enough for most use-cases to not worry about.
///
/// ## To fetch the top collection NSIDs:
///
/// Specify the `order` parameter (must be either `records-created` or `did-estimate`). Note that ordered results cannot be paged.
///
/// All statistics are bucketed hourly, so the most granular effecitve time boundary for `since` and `until` is one hour.
#[endpoint {
    method = GET,
    path = "/prefix"
}]
async fn get_prefix(
    ctx: RequestContext<Context>,
    query: Query<PrefixQuery>,
) -> OkCorsResponse<PrefixResponse> {
    let Context { storage, .. } = ctx.context();
    let q = query.into_inner();

    instrument_handler(&ctx, async {
        let prefix = NsidPrefix::new(&q.prefix).map_err(|e| {
            HttpError::for_bad_request(
                None,
                format!("{:?} was not a valid NSID prefix: {e:?}", q.prefix),
            )
        })?;

        if q.cursor.is_some() && q.order.is_some() {
            let msg =
                "`cursor` is mutually exclusive with `order`. ordered results cannot be paged.";
            return Err(HttpError::for_bad_request(None, msg.to_string()));
        }

        let order = if let Some(ref o) = q.order {
            o.into()
        } else {
            let cursor = q
                .cursor
                .and_then(|c| if c.is_empty() { None } else { Some(c) })
                .map(|c| URL_SAFE_NO_PAD.decode(&c))
                .transpose()
                .map_err(|e| HttpError::for_bad_request(None, format!("invalid cursor: {e:?}")))?;
            OrderCollectionsBy::Lexi { cursor }
        };

        let limit = match (q.limit, q.order) {
            (Some(limit), _) => limit,
            (None, Some(_)) => 32,
            (None, None) => 100,
        };

        if !(1..=200).contains(&limit) {
            let msg = format!("limit not in 1..=200: {limit}");
            return Err(HttpError::for_bad_request(None, msg));
        }

        let since = q.since.map(dt_to_cursor).transpose()?;
        let until = q.until.map(dt_to_cursor).transpose()?;

        let (total, children, next_cursor) = storage
            .get_prefix(prefix, limit, order, since, until)
            .await
            .map_err(|e| HttpError::for_internal_error(format!("oh shoot: {e:?}")))?;

        let next_cursor = next_cursor.map(|c| URL_SAFE_NO_PAD.encode(c));

        OkCors(PrefixResponse {
            total,
            children,
            cursor: next_cursor,
        })
        .into()
    })
    .await
}

#[derive(Debug, Deserialize, JsonSchema)]
struct CollectionTimeseriesQuery {
    collection: String, // JsonSchema not implemented for Nsid :(
    /// Limit collections and statistics to those seen after this UTC datetime
    ///
    /// default: 1 week ago
    since: Option<DateTime<Utc>>,
    /// Limit collections and statistics to those seen before this UTC datetime
    ///
    /// default: now
    until: Option<DateTime<Utc>>,
    /// time steps between data, in seconds
    ///
    /// the step will be rounded down to the nearest hour
    ///
    /// default: 86400 (24hrs)
    #[schemars(range(min = 3600))]
    step: Option<u64>,
    // todo: rolling averages
}
#[derive(Debug, Serialize, JsonSchema)]
struct CollectionTimeseriesResponse {
    range: Vec<DateTime<Utc>>,
    series: HashMap<String, Vec<JustCount>>,
}
/// Collection timeseries stats
#[endpoint {
    method = GET,
    path = "/timeseries"
}]
async fn get_timeseries(
    ctx: RequestContext<Context>,
    query: Query<CollectionTimeseriesQuery>,
) -> OkCorsResponse<CollectionTimeseriesResponse> {
    let Context { storage, .. } = ctx.context();
    let q = query.into_inner();

    instrument_handler(&ctx, async {
        let since = q.since.map(dt_to_cursor).transpose()?.unwrap_or_else(|| {
            let week_ago_secs = 7 * 86_400;
            let week_ago = SystemTime::now() - Duration::from_secs(week_ago_secs);
            Cursor::at(week_ago).into()
        });

        let until = q.until.map(dt_to_cursor).transpose()?;

        let step = if let Some(secs) = q.step {
            if secs < 3600 {
                let msg = format!("step is too small: {secs}");
                Err(HttpError::for_bad_request(None, msg))?;
            }
            (secs / 3600) * 3600 // trucate to hour
        } else {
            86_400
        };

        let nsid = Nsid::new(q.collection).map_err(|e| {
            HttpError::for_bad_request(None, format!("collection was not a valid NSID: {e:?}"))
        })?;

        let (range_cursors, series) = storage
            .get_timeseries(vec![nsid], since, until, step)
            .await
            .map_err(|e| HttpError::for_internal_error(format!("oh shoot: {e:?}")))?;

        let range = range_cursors
            .into_iter()
            .map(|c| DateTime::<Utc>::from_timestamp_micros(c.to_raw_u64() as i64).unwrap())
            .collect();

        let series = series
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.iter().map(Into::into).collect()))
            .collect();

        OkCors(CollectionTimeseriesResponse { range, series }).into()
    })
    .await
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SearchQuery {
    /// Query
    ///
    /// at least two alphanumeric (+hyphen) characters must be present
    q: String,
}
#[derive(Debug, Serialize, JsonSchema)]
struct SearchResponse {
    matches: Vec<NsidCount>,
}
/// Search lexicons
#[endpoint {
    method = GET,
    path = "/search"
}]
async fn search_collections(
    ctx: RequestContext<Context>,
    query: Query<SearchQuery>,
) -> OkCorsResponse<SearchResponse> {
    let Context { storage, .. } = ctx.context();
    let q = query.into_inner();
    instrument_handler(&ctx, async {
        // TODO: query validation
        // TODO: also handle multi-space stuff (ufos-app tries to on client)
        let terms: Vec<String> = q.q.split(' ').map(Into::into).collect();
        let matches = storage
            .search_collections(terms)
            .await
            .map_err(|e| HttpError::for_internal_error(format!("oh ugh: {e:?}")))?;
        OkCors(SearchResponse { matches }).into()
    })
    .await
}

pub async fn serve(storage: impl StoreReader + 'static, bind: std::net::SocketAddr) -> Result<(), String> {
    describe_metrics();
    let log = ConfigLogging::StderrTerminal {
        level: ConfigLoggingLevel::Warn,
    }
    .to_logger("server")
    .map_err(|e| e.to_string())?;

    let mut api = ApiDescription::new();

    api.register(index).unwrap();
    api.register(get_openapi).unwrap();
    api.register(get_meta_info).unwrap();
    api.register(get_records_by_collections).unwrap();
    api.register(get_collection_stats).unwrap();
    api.register(get_collections).unwrap();
    api.register(get_prefix).unwrap();
    api.register(get_timeseries).unwrap();
    api.register(search_collections).unwrap();

    let context = Context {
        spec: Arc::new(
            api.openapi(
                "UFOs API: Every lexicon in the ATmosphere",
                env!("CARGO_PKG_VERSION")
                    .parse()
                    .inspect_err(|e| {
                        log::warn!("failed to parse cargo package version for openapi: {e:?}")
                    })
                    .unwrap_or(semver::Version::new(0, 0, 1)),
            )
            .description("Samples and statistics of atproto records by their collection NSID")
            .contact_name("part of @microcosm.blue")
            .contact_url("https://microcosm.blue")
            .json()
            .map_err(|e| e.to_string())?,
        ),
        storage: Box::new(storage),
    };

    ServerBuilder::new(api, context, log)
        .config(ConfigDropshot {
            bind_address: bind,
            ..Default::default()
        })
        .start()
        .map_err(|error| format!("failed to start server: {error}"))?
        .await
}
