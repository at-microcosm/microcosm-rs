use clap::Parser;
use poem::{
    EndpointExt, Response, Route, Server, get, handler,
    http::StatusCode,
    listener::TcpListener,
    middleware::{AddData, Tracing},
    web::{Data, Json, Query, TypedHeader, headers::Host},
};
use serde::{Deserialize, Serialize};

#[handler]
fn hello() -> String {
    "ɹoʇɔǝʅⅎǝɹ".to_string()
}

#[derive(Debug, Serialize)]
struct DidDoc {
    id: String,
    service: [DidService; 1],
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct DidService {
    id: String,
    r#type: String,
    service_endpoint: String,
}

#[handler]
fn did_doc(TypedHeader(host): TypedHeader<Host>, service: Data<&DidService>) -> Json<DidDoc> {
    Json(DidDoc {
        id: format!("did:web:{}", host.hostname()),
        service: [service.clone()],
    })
}

#[derive(Deserialize)]
struct AskQuery {
    domain: String,
}
#[handler]
fn ask_caddy(
    Data(parent): Data<&Option<String>>,
    Query(AskQuery { domain }): Query<AskQuery>,
) -> Response {
    if let Some(parent) = parent
        && let Some(prefix) = domain.strip_suffix(&format!(".{parent}"))
        && !prefix.contains('.')
    {
        // no sub-sub-domains allowed
        return Response::builder().body("ok");
    };
    Response::builder()
        .status(StatusCode::FORBIDDEN)
        .body("nope")
}

/// Slingshot record edge cache
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    /// The DID document service ID to serve
    ///
    /// must start with a '#', like `#bsky_appview'
    #[arg(long)]
    id: String,
    /// Service type
    ///
    /// Not sure exactly what its requirements are. 'BlueskyAppview' for example
    #[arg(long)]
    r#type: String,
    /// The HTTPS endpoint for the service
    #[arg(long)]
    service_endpoint: String,
    /// The parent domain; requests should come from subdomains of this
    #[arg(long)]
    domain: Option<String>,
}

impl From<Args> for DidService {
    fn from(a: Args) -> Self {
        Self {
            id: a.id,
            r#type: a.r#type,
            service_endpoint: a.service_endpoint,
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::fmt::init();
    log::info!("ɹoʇɔǝʅⅎǝɹ");

    let args = Args::parse();
    let domain = args.domain.clone();
    let service: DidService = args.into();

    Server::new(TcpListener::bind("0.0.0.0:3001"))
        .run(
            Route::new()
                .at("/", get(hello))
                .at("/.well-known/did.json", get(did_doc))
                .at("/ask", get(ask_caddy))
                .with(AddData::new(service))
                .with(AddData::new(domain))
                .with(Tracing),
        )
        .await
        .unwrap()
}
