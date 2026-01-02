use crate::{Storage, TokenVerifier, VerifyError};
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
    ApiResponse, ContactObject, ExternalDocumentObject, Object, OpenApi, OpenApiService,
    SecurityScheme, Tags,
    auth::Bearer,
    payload::{Json, PlainText},
    types::Example,
};
use serde::Serialize;
use serde_json::{Value, json};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Debug, SecurityScheme)]
#[oai(ty = "bearer")]
struct XrpcAuth(Bearer);

#[derive(Tags)]
enum ApiTags {
    /// Custom pocket APIs
    #[oai(rename = "Pocket APIs")]
    Pocket,
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
            error: "PreferencesNotFound".to_string(),
            message: "No preferences were found for this user".to_string(),
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

#[derive(Debug, Object)]
#[oai(example = true)]
struct PrefsObject {
    preferences: Value,
}
impl Example for PrefsObject {
    fn example() -> Self {
        Self {
            preferences: json!({
                "hello": "world",
            }),
        }
    }
}

#[derive(ApiResponse)]
enum GetPrefsResponse {
    /// Record found
    #[oai(status = 200)]
    Ok(Json<PrefsObject>),
    /// Bad request or no preferences to return
    #[oai(status = 400)]
    BadRequest(XrpcError),
    /// Server errors
    #[oai(status = 500)]
    ServerError(XrpcError),
}

#[derive(ApiResponse)]
enum PutPrefsResponse {
    /// Record found
    #[oai(status = 200)]
    Ok(PlainText<String>),
    /// Bad request or no preferences to return
    #[oai(status = 400)]
    BadRequest(XrpcError),
    /// Server errors
    #[oai(status = 500)]
    ServerError(XrpcError),
}

struct Xrpc {
    aud: String,
    verifier: TokenVerifier,
    storage: Arc<Mutex<Storage>>,
}

#[OpenApi]
impl Xrpc {
    /// getPreferences
    ///
    /// get stored preferences
    ///
    /// TODO: don't hardcode nucleus
    #[oai(
        path = "/net.at-app.pet.ptr.nucleus.getPreferences",
        method = "get",
        tag = "ApiTags::Pocket"
    )]
    async fn pocket_get_prefs(&self, XrpcAuth(auth): XrpcAuth) -> GetPrefsResponse {
        let did = match self
            .verifier
            .verify(
                "net.at-app.pet.ptr.nucleus.getPreferences",
                &self.aud,
                &auth.token,
            )
            .await
        {
            Ok(d) => d,
            Err(VerifyError::BadToken(reason)) => {
                return GetPrefsResponse::BadRequest(xrpc_error("BadToken", reason));
            }
            Err(VerifyError::ProbablyBadIdentity) => {
                return GetPrefsResponse::BadRequest(xrpc_error(
                    "BadIdentity",
                    "could not resolve the user identity",
                ));
            }
            Err(VerifyError::FailedToVerify(reason)) => {
                return GetPrefsResponse::ServerError(xrpc_error("FailedToVerify", reason));
            }
        };
        log::info!("verified did: {did}");

        let storage = self.storage.clone();

        let Ok(Ok(res)) = tokio::task::spawn_blocking(move || {
            storage
                .lock()
                .unwrap()
                .get(&did, "net.at-app.pet.ptr.nucleus")
                .inspect_err(|e| log::error!("failed to get prefs: {e}"))
        })
        .await
        else {
            return GetPrefsResponse::ServerError(xrpc_error(
                "InternalError",
                "failed to get prefs from db",
            ));
        };

        let Some(serialized) = res else {
            return GetPrefsResponse::BadRequest(xrpc_error(
                "NotFound",
                "could not find prefs for this identity",
            ));
        };

        let preferences = match serde_json::from_str(&serialized) {
            Ok(v) => v,
            Err(e) => {
                log::error!("failed to deserialize prefs: {e}");
                return GetPrefsResponse::ServerError(xrpc_error(
                    "InternalError",
                    "failed to deserialize prefs",
                ));
            }
        };

        GetPrefsResponse::Ok(Json(PrefsObject { preferences }))
    }

    /// putPreferences
    ///
    /// store preferences
    ///
    /// TODO: don't hardcode nucleus
    #[oai(
        path = "/net.at-app.pet.ptr.nucleus.putPreferences",
        method = "post",
        tag = "ApiTags::Pocket"
    )]
    async fn pocket_put_prefs(
        &self,
        XrpcAuth(auth): XrpcAuth,
        Json(prefs): Json<PrefsObject>,
    ) -> PutPrefsResponse {
        let did = match self
            .verifier
            .verify(
                "com.bad-example.pocket.putPreferences",
                &self.aud,
                &auth.token,
            )
            .await
        {
            Ok(d) => d,
            Err(VerifyError::BadToken(reason)) => {
                return PutPrefsResponse::BadRequest(xrpc_error("BadToken", reason));
            }
            Err(VerifyError::ProbablyBadIdentity) => {
                return PutPrefsResponse::BadRequest(xrpc_error(
                    "BadIdentity",
                    "could not resolve the user identity",
                ));
            }
            Err(VerifyError::FailedToVerify(reason)) => {
                return PutPrefsResponse::ServerError(xrpc_error("FailedToVerify", reason));
            }
        };
        log::info!("verified did: {did}");
        log::warn!("received prefs: {prefs:?}");

        let storage = self.storage.clone();
        let serialized = prefs.preferences.to_string();

        let Ok(Ok(())) = tokio::task::spawn_blocking(move || {
            storage
                .lock()
                .unwrap()
                .put(&did, "net.at-app.pet.ptr.nucleus", &serialized)
                .inspect_err(|e| log::error!("failed to insert prefs: {e}"))
        })
        .await
        else {
            return PutPrefsResponse::ServerError(xrpc_error(
                "InternalError",
                "failed to put prefs to db",
            ));
        };

        PutPrefsResponse::Ok(PlainText("saved.".to_string()))
    }
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
fn get_did_doc(domain: &str) -> impl Endpoint + use<> {
    let doc = poem::web::Json(AppViewDoc {
        id: format!("did:web:{domain}"),
        service: [AppViewService {
            id: "#pocket_prefs".to_string(),
            r#type: "PocketPreferences".to_string(),
            service_endpoint: format!("https://{domain}"),
        }],
    });
    make_sync(move |_| doc.clone())
}

pub async fn serve(domain: &str, storage: Storage, certs_path: PathBuf) -> () {
    let verifier = TokenVerifier::default();
    let api_service = OpenApiService::new(
        Xrpc {
            aud: domain.to_string(),
            verifier,
            storage: Arc::new(Mutex::new(storage)),
        },
        "Pocket",
        env!("CARGO_PKG_VERSION"),
    )
    .server(domain)
    .url_prefix("/xrpc")
    .contact(
        ContactObject::new()
            .name("@microcosm.blue")
            .url("https://bsky.app/profile/microcosm.blue"),
    )
    .description(include_str!("../api-description.md"))
    .external_document(ExternalDocumentObject::new("https://microcosm.blue/pocket"));

    let app = Route::new()
        .nest("/openapi", api_service.spec_endpoint())
        .nest("/xrpc/", api_service)
        .at("/.well-known/did.json", get_did_doc(domain))
        .at("/", StaticFileEndpoint::new("./static/index.html"))
        .with(
            Cors::new()
                .allow_method(Method::GET)
                .allow_method(Method::POST),
        )
        .with(CatchPanic::new())
        .with(Tracing);

    // set up letsencrypt
    // (TODO: make letsencrypt optional)
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("alskfjalksdjf");

    let auto_cert = AutoCert::builder()
        .directory_url(LETS_ENCRYPT_PRODUCTION)
        .domain(domain)
        .cache_path(certs_path)
        .build()
        .unwrap();

    let listener = TcpListener::bind("0.0.0.0:443").acme(auto_cert);
    Server::new(listener).name("pocket").run(app).await.unwrap();
}
