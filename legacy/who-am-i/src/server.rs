use atrium_api::types::string::Did;
use atrium_oauth::OAuthClientMetadata;
use axum::{
    Router,
    extract::{FromRef, Json as ExtractJson, Query, State},
    http::{
        StatusCode,
        header::{CONTENT_SECURITY_POLICY, CONTENT_TYPE, HeaderMap, ORIGIN, REFERER},
    },
    response::{IntoResponse, Json, Redirect, Response},
    routing::{get, post},
};
use axum_extra::extract::cookie::{Cookie, Expiration, Key, SameSite, SignedCookieJar};
use axum_template::{RenderHtml, engine::Engine};
use handlebars::{Handlebars, handlebars_helper};
use jose_jwk::JwkSet;
use std::path::PathBuf;

use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use url::Url;

use crate::{
    ExpiringTaskMap, OAuth, OAuthCallbackParams, OAuthCompleteError, ResolveHandleError, Tokens,
};

const FAVICON: &[u8] = include_bytes!("../static/favicon.ico");
const STYLE_CSS: &str = include_str!("../static/style.css");

const HELLO_COOKIE_KEY: &str = "hello-who-am-i";
const DID_COOKIE_KEY: &str = "did";

const COOKIE_EXPIRATION: Duration = Duration::from_secs(30 * 86_400);

type AppEngine = Engine<Handlebars<'static>>;

#[derive(Clone)]
struct AppState {
    pub key: Key,
    pub allowed_hosts: Arc<HashSet<String>>,
    pub engine: AppEngine,
    pub oauth: Arc<OAuth>,
    pub resolve_handles: ExpiringTaskMap<Result<String, ResolveHandleError>>,
    pub shutdown: CancellationToken,
    pub tokens: Arc<Tokens>,
}

impl FromRef<AppState> for Key {
    fn from_ref(state: &AppState) -> Self {
        state.key.clone()
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn serve(
    shutdown: CancellationToken,
    app_secret: String,
    oauth_private_key: Option<PathBuf>,
    tokens: Tokens,
    base: String,
    bind: String,
    allowed_hosts: Vec<String>,
    dev: bool,
) {
    let mut hbs = Handlebars::new();
    hbs.set_dev_mode(dev);
    hbs.register_templates_directory("templates", Default::default())
        .unwrap();

    handlebars_helper!(json: |v: Value| serde_json::to_string(&v).unwrap());
    hbs.register_helper("json", Box::new(json));

    // clients have to pick up their identity-resolving tasks within this period
    let task_pickup_expiration = Duration::from_secs(15);

    let oauth = OAuth::new(oauth_private_key, base).unwrap();

    let state = AppState {
        engine: Engine::new(hbs),
        key: Key::from(app_secret.as_bytes()), // TODO: via config
        allowed_hosts: Arc::new(HashSet::from_iter(allowed_hosts)),
        oauth: Arc::new(oauth),
        resolve_handles: ExpiringTaskMap::new(task_pickup_expiration),
        shutdown: shutdown.clone(),
        tokens: Arc::new(tokens),
    };

    let app = Router::new()
        .route("/", get(hello))
        .route("/favicon.ico", get(favicon)) // todo MIME
        .route("/style.css", get(css))
        .route("/prompt", get(prompt))
        .route("/user-info", post(user_info))
        .route("/client-metadata.json", get(client_metadata))
        .route("/auth", get(start_oauth))
        .route("/authorized", get(complete_oauth))
        .route("/disconnect", post(disconnect))
        .route("/.well-known/jwks.json", get(jwks))
        .with_state(state);

    eprintln!("starting server at http://{bind}");
    let listener = TcpListener::bind(bind)
        .await
        .expect("listener binding to work");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.cancelled().await })
        .await
        .unwrap();
}

#[derive(Debug, Deserialize)]
struct HelloQuery {
    auth_reload: Option<String>,
    auth_failed: Option<String>,
}
async fn hello(
    State(AppState {
        engine,
        resolve_handles,
        shutdown,
        oauth,
        ..
    }): State<AppState>,
    Query(params): Query<HelloQuery>,
    mut jar: SignedCookieJar,
) -> Response {
    let is_auth_reload = params.auth_reload.is_some();
    let auth_failed = params.auth_failed.is_some();
    let no_cookie = jar.get(HELLO_COOKIE_KEY).is_none();
    jar = jar.add(hello_cookie());

    let info = if let Some(did) = jar.get(DID_COOKIE_KEY) {
        if let Ok(did) = Did::new(did.value_trimmed().to_string()) {
            // push cookie expiry
            jar = jar.add(cookie(&did));
            let fetch_key = resolve_handles.dispatch(
                {
                    let oauth = oauth.clone();
                    let did = did.clone();
                    async move { oauth.resolve_handle(did.clone()).await }
                },
                shutdown.child_token(),
            );
            json!({
                "did": did,
                "fetch_key": fetch_key,
                "is_auth_reload": is_auth_reload,
                "auth_failed": auth_failed,
                "no_cookie": no_cookie,
            })
        } else {
            jar = jar.remove(DID_COOKIE_KEY);
            json!({
                "is_auth_reload": is_auth_reload,
                "auth_failed": auth_failed,
                "no_cookie": no_cookie,
            })
        }
    } else {
        json!({
            "is_auth_reload": is_auth_reload,
            "auth_failed": auth_failed,
            "no_cookie": no_cookie,
        })
    };
    let frame_headers = [(CONTENT_SECURITY_POLICY, "frame-ancestors 'none'")];
    (frame_headers, jar, RenderHtml("hello", engine, info)).into_response()
}

async fn css() -> impl IntoResponse {
    let headers = [
        (CONTENT_TYPE, "text/css"),
        // (CACHE_CONTROL, "") // TODO
    ];
    (headers, STYLE_CSS)
}

async fn favicon() -> impl IntoResponse {
    ([(CONTENT_TYPE, "image/x-icon")], FAVICON)
}

fn hello_cookie() -> Cookie<'static> {
    Cookie::build((HELLO_COOKIE_KEY, "hiiii"))
        .http_only(true)
        .secure(true)
        .same_site(SameSite::None)
        .expires(Expiration::DateTime(
            (SystemTime::now() + COOKIE_EXPIRATION).into(),
        )) // wtf safari needs this to not be a session cookie??
        .max_age(COOKIE_EXPIRATION.try_into().unwrap())
        .path("/")
        .into()
}

fn cookie(did: &Did) -> Cookie<'static> {
    Cookie::build((DID_COOKIE_KEY, did.to_string()))
        .http_only(true)
        .secure(true)
        .same_site(SameSite::None)
        .expires(Expiration::DateTime(
            (SystemTime::now() + COOKIE_EXPIRATION).into(),
        )) // wtf safari needs this to not be a session cookie??
        .max_age(COOKIE_EXPIRATION.try_into().unwrap())
        .path("/")
        .into()
}

#[derive(Debug, Deserialize)]
struct PromptQuery {
    // this must *ONLY* be used for the postmessage target origin
    app: Option<String>,
}
async fn prompt(
    State(AppState {
        allowed_hosts,
        engine,
        oauth,
        resolve_handles,
        shutdown,
        tokens,
        ..
    }): State<AppState>,
    Query(params): Query<PromptQuery>,
    jar: SignedCookieJar,
    headers: HeaderMap,
) -> impl IntoResponse {
    let err = |reason, check_frame, detail| {
        metrics::counter!("whoami_auth_prompt", "ok" => "false", "reason" => reason).increment(1);
        let info = json!({
            "reason": reason,
            "check_frame": check_frame,
            "detail": detail,
        });
        let html = RenderHtml("prompt-error", engine.clone(), info);
        (StatusCode::BAD_REQUEST, html).into_response()
    };

    let Some(parent) = headers.get(ORIGIN).or_else(|| {
        eprintln!("referrer fallback");
        // TODO: referer should only be used for localhost??
        headers.get(REFERER)
    }) else {
        return err("Missing origin and no referrer for fallback", true, None);
    };
    let Ok(parent) = parent.to_str() else {
        return err("Unreadable origin or referrer", true, None);
    };
    eprintln!(
        "rolling with parent: {parent:?} (from origin? {})",
        headers.get(ORIGIN).is_some()
    );
    let Ok(url) = Url::parse(parent) else {
        return err("Bad origin or referrer", true, None);
    };
    let Some(parent_host) = url.host_str() else {
        return err("Origin or referrer missing host", true, None);
    };
    if !allowed_hosts.contains(parent_host) {
        return err(
            "Login is not allowed on this page",
            false,
            Some(parent_host),
        );
    }
    if let Some(ref app) = params.app
        && !allowed_hosts.contains(app)
    {
        return err("Login is not allowed for this app", false, Some(app));
    }
    let parent_origin = url.origin().ascii_serialization();
    if parent_origin == "null" {
        return err("Origin or referrer header value is opaque", true, None);
    }

    let all_allowed = allowed_hosts
        .iter()
        .map(|h| format!("https://{h}"))
        .collect::<Vec<_>>()
        .join(" ");
    let csp = format!("frame-ancestors 'self' {parent_origin} {all_allowed}");
    let frame_headers = [(CONTENT_SECURITY_POLICY, &csp)];

    if let Some(did) = jar.get(DID_COOKIE_KEY) {
        let Ok(did) = Did::new(did.value_trimmed().to_string()) else {
            return err("Bad cookie", false, None);
        };

        // push cookie expiry
        let jar = jar.add(cookie(&did));

        let token = match tokens.mint(&*did) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("failed to create JWT: {e:?}");
                return err("failed to create JWT", false, None);
            }
        };

        let fetch_key = resolve_handles.dispatch(
            {
                let oauth = oauth.clone();
                let did = did.clone();
                async move { oauth.resolve_handle(did.clone()).await }
            },
            shutdown.child_token(),
        );

        metrics::counter!("whoami_auth_prompt", "ok" => "true", "known" => "true").increment(1);
        let info = json!({
            "did": did,
            "token": token,
            "fetch_key": fetch_key,
            "parent_host": parent_host,
            "parent_origin": parent_origin,
            "parent_target": params.app.map(|h| format!("https://{h}")),
        });
        (frame_headers, jar, RenderHtml("prompt", engine, info)).into_response()
    } else {
        metrics::counter!("whoami_auth_prompt", "ok" => "true", "known" => "false").increment(1);
        let info = json!({
            "parent_host": parent_host,
            "parent_origin": parent_origin,
        });
        (frame_headers, RenderHtml("prompt", engine, info)).into_response()
    }
}

#[derive(Debug, Deserialize)]
struct UserInfoParams {
    fetch_key: String,
}
async fn user_info(
    State(AppState {
        resolve_handles, ..
    }): State<AppState>,
    ExtractJson(params): ExtractJson<UserInfoParams>,
) -> impl IntoResponse {
    let err = |status, reason: &str| {
        metrics::counter!("whoami_user_info", "found" => "false", "reason" => reason.to_string())
            .increment(1);
        (status, Json(json!({ "reason": reason }))).into_response()
    };

    let Some(task_handle) = resolve_handles.take(&params.fetch_key) else {
        return err(StatusCode::NOT_FOUND, "fetch key does not exist or expired");
    };

    match task_handle.await {
        Err(task_err) => {
            eprintln!("task join error? {task_err:?}");
            err(StatusCode::INTERNAL_SERVER_ERROR, "server errored")
        }
        Ok(Err(ResolveHandleError::ResolutionFailed(atrium_identity::Error::NotFound))) => {
            err(StatusCode::NOT_FOUND, "handle not found")
        }
        Ok(Err(ResolveHandleError::ResolutionFailed(e))) => {
            eprintln!("handle resolution failed: {e:?}");
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "handle resolution failed",
            )
        }
        Ok(Err(ResolveHandleError::NoHandle)) => err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "resolved identity but did not find a handle",
        ),
        Ok(Err(ResolveHandleError::InvalidHandle(_h, reason))) => err(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("handle appears invalid: {reason}"),
        ),
        Ok(Ok(handle)) => {
            metrics::counter!("whoami_user_info", "found" => "true").increment(1);
            Json(json!({ "handle": handle })).into_response()
        }
    }
}

async fn client_metadata(
    State(AppState { oauth, .. }): State<AppState>,
) -> Json<OAuthClientMetadata> {
    Json(oauth.client_metadata())
}

#[derive(Debug, Deserialize)]
struct BeginOauthParams {
    handle: String,
}
async fn start_oauth(
    State(AppState { oauth, engine, .. }): State<AppState>,
    Query(params): Query<BeginOauthParams>,
    jar: SignedCookieJar,
) -> Response {
    // if any existing session was active, clear it first
    // ...this might help a confusion attack w multiple sign-in flows or smth
    let jar = jar.remove(DID_COOKIE_KEY);

    use atrium_identity::Error as IdError;
    use atrium_oauth::Error as OAuthError;

    let err = |code, reason: &str| {
        metrics::counter!("whoami_auth_start", "ok" => "false", "reason" => reason.to_string())
            .increment(1);
        let info = json!({
            "result": "fail",
            "reason": reason,
        });
        (code, RenderHtml("auth-fail", engine.clone(), info)).into_response()
    };

    match oauth.begin(&params.handle).await {
        Err(OAuthError::Identity(
            IdError::NotFound | IdError::HttpStatus(StatusCode::NOT_FOUND),
        )) => err(StatusCode::NOT_FOUND, "handle not found"),
        Err(OAuthError::Identity(IdError::AtIdentifier(r))) => err(StatusCode::BAD_REQUEST, &r),
        Err(e) => {
            eprintln!("begin auth failed: {e:?}");
            err(StatusCode::INTERNAL_SERVER_ERROR, "unknown")
        }
        Ok(auth_url) => {
            metrics::counter!("whoami_auth_start", "ok" => "true").increment(1);
            (jar, Redirect::to(&auth_url)).into_response()
        }
    }
}

async fn complete_oauth(
    State(AppState {
        engine,
        resolve_handles,
        oauth,
        shutdown,
        tokens,
        ..
    }): State<AppState>,
    Query(params): Query<OAuthCallbackParams>,
    jar: SignedCookieJar,
) -> Response {
    let err = |code, result, reason: &str| {
        metrics::counter!("whoami_auth_complete", "ok" => "false", "reason" => reason.to_string())
            .increment(1);
        let info = json!({
            "result": result,
            "reason": reason,
        });
        (code, RenderHtml("auth-fail", engine.clone(), info)).into_response()
    };

    let did = match oauth.complete(params).await {
        Ok(did) => did,
        Err(e) => {
            return match e {
                OAuthCompleteError::Denied { description, .. } => {
                    let desc = description.unwrap_or("permission to share was denied".to_string());
                    err(StatusCode::FORBIDDEN, "deny", desc.as_str())
                }
                OAuthCompleteError::Failed { .. } => {
                    eprintln!("auth completion failed: {e:?}");
                    err(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "fail",
                        "failed to complete",
                    )
                }
                OAuthCompleteError::CallbackFailed(e) => {
                    eprintln!("auth callback failed: {e:?}");
                    err(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "fail",
                        "failed to complete callback",
                    )
                }
                OAuthCompleteError::NoDid => err(StatusCode::BAD_REQUEST, "fail", "no DID found"),
            };
        }
    };

    let jar = jar.add(cookie(&did));

    let token = match tokens.mint(&*did) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("failed to create JWT: {e:?}");
            return err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "fail",
                "failed to create JWT",
            );
        }
    };

    let fetch_key = resolve_handles.dispatch(
        {
            let oauth = oauth.clone();
            let did = did.clone();
            async move { oauth.resolve_handle(did.clone()).await }
        },
        shutdown.child_token(),
    );

    metrics::counter!("whoami_auth_complete", "ok" => "true").increment(1);
    let info = json!({
        "did": did,
        "token": token,
        "fetch_key": fetch_key,
    });
    (jar, RenderHtml("authorized", engine, info)).into_response()
}

async fn disconnect(jar: SignedCookieJar) -> impl IntoResponse {
    metrics::counter!("whoami_disconnect").increment(1);
    let jar = jar.remove(DID_COOKIE_KEY);
    (jar, Json(json!({ "ok": true })))
}

async fn jwks(State(AppState { oauth, tokens, .. }): State<AppState>) -> Json<JwkSet> {
    let mut jwks = oauth.jwks();
    jwks.keys.push(tokens.jwk());
    Json(jwks)
}
