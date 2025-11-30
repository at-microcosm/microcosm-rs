// use foyer::HybridCache;
// use foyer::{Engine, DirectFsDeviceOptions, HybridCacheBuilder};
use metrics_exporter_prometheus::PrometheusBuilder;
use slingshot::{
    Identity, Repo, consume, error::MainTaskError, firehose_cache, healthcheck, serve,
};
use std::path::PathBuf;

use clap::Parser;
use tokio_util::sync::CancellationToken;

/// Slingshot record edge cache
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    /// Jetstream server to connect to (exclusive with --fixture). Provide either a wss:// URL, or a shorhand value:
    /// 'us-east-1', 'us-east-2', 'us-west-1', or 'us-west-2'
    #[arg(long)]
    jetstream: String,
    /// don't request zstd-compressed jetstream events
    ///
    /// reduces CPU at the expense of more ingress bandwidth
    #[arg(long, action)]
    jetstream_no_zstd: bool,
    /// where to keep disk caches
    #[arg(long)]
    cache_dir: PathBuf,
    /// the domain pointing to this server
    ///
    /// if present:
    /// - a did:web document will be served at /.well-known/did.json
    /// - an HTTPS certs will be automatically configured with Acme/letsencrypt
    /// - TODO: a rate-limiter will be installed
    #[arg(long)]
    domain: Option<String>,
    /// email address for letsencrypt contact
    ///
    /// recommended in production, i guess?
    #[arg(long)]
    acme_contact: Option<String>,
    /// a location to cache acme https certs
    ///
    /// only used if --host is specified. omitting requires re-requesting certs
    /// on every restart, and letsencrypt has rate limits that are easy to hit.
    ///
    /// recommended in production, but mind the file permissions.
    #[arg(long)]
    certs: Option<PathBuf>,
    /// an web address to send healtcheck pings to every ~51s or so
    #[arg(long)]
    healthcheck: Option<String>,
    /// slingshot server's listen address
    #[arg(long, default_value = "0.0.0.0:8080")]
    bind: std::net::SocketAddr,
    /// metrics server's listen address
    #[arg(long, default_value = "0.0.0.0:8765")]
    bind_metrics: std::net::SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    tracing_subscriber::fmt::init();

    let shutdown = CancellationToken::new();

    let ctrlc_shutdown = shutdown.clone();
    ctrlc::set_handler(move || ctrlc_shutdown.cancel()).expect("failed to set ctrl-c handler");

    let args = Args::parse();

    if let Err(e) = install_metrics_server(args.bind_metrics) {
        log::error!("failed to install metrics server: {e:?}");
    } else {
        log::info!("metrics listening at http://{}", args.bind_metrics);
    }

    std::fs::create_dir_all(&args.cache_dir).map_err(|e| {
        format!(
            "failed to ensure cache parent dir: {e:?} (dir: {:?})",
            args.cache_dir
        )
    })?;
    let cache_dir = args.cache_dir.canonicalize().map_err(|e| {
        format!(
            "failed to canonicalize cache_dir: {e:?} (dir: {:?})",
            args.cache_dir
        )
    })?;
    log::info!("cache dir ready at at {cache_dir:?}.");

    log::info!("setting up firehose cache...");
    let cache = firehose_cache(cache_dir.join("./firehose")).await?;
    log::info!("firehose cache ready.");

    let mut tasks: tokio::task::JoinSet<Result<(), MainTaskError>> = tokio::task::JoinSet::new();

    log::info!("starting identity service...");
    let identity = Identity::new(cache_dir.join("./identity"))
        .await
        .map_err(|e| format!("identity setup failed: {e:?}"))?;
    log::info!("identity service ready.");
    let identity_refresher = identity.clone();
    let identity_shutdown = shutdown.clone();
    tasks.spawn(async move {
        identity_refresher.run_refresher(identity_shutdown).await?;
        Ok(())
    });

    let repo = Repo::new(identity.clone());

    let server_shutdown = shutdown.clone();
    let server_cache_handle = cache.clone();
    let bind = args.bind;
    tasks.spawn(async move {
        serve(
            server_cache_handle,
            identity,
            repo,
            args.domain,
            args.acme_contact,
            args.certs,
            server_shutdown,
            bind,
        )
        .await?;
        Ok(())
    });

    let consumer_shutdown = shutdown.clone();
    let consumer_cache = cache.clone();
    tasks.spawn(async move {
        consume(
            args.jetstream,
            None,
            args.jetstream_no_zstd,
            consumer_shutdown,
            consumer_cache,
        )
        .await?;
        Ok(())
    });

    if let Some(hc) = args.healthcheck {
        let healthcheck_shutdown = shutdown.clone();
        tasks.spawn(async move {
            healthcheck(hc, healthcheck_shutdown).await?;
            Ok(())
        });
    }

    tokio::select! {
        _ = shutdown.cancelled() => log::warn!("shutdown requested"),
        Some(r) = tasks.join_next() => {
            log::warn!("a task exited, shutting down: {r:?}");
            shutdown.cancel();
        }
    }

    tasks.spawn(async move {
        cache
            .close()
            .await
            .map_err(MainTaskError::FirehoseCacheCloseError)
    });

    tokio::select! {
        _ = async {
            while let Some(completed) = tasks.join_next().await {
                log::info!("shutdown: task completed: {completed:?}");
            }
        } => {},
        _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
            log::info!("shutdown: not all tasks completed on time. aborting...");
            tasks.shutdown().await;
        },
    }

    log::info!("bye!");

    Ok(())
}

fn install_metrics_server(bind_metrics: std::net::SocketAddr) -> Result<(), metrics_exporter_prometheus::BuildError> {
    log::info!("installing metrics server...");
    PrometheusBuilder::new()
        .set_quantiles(&[0.5, 0.9, 0.99, 1.0])?
        .set_bucket_duration(std::time::Duration::from_secs(300))?
        .set_bucket_count(std::num::NonZero::new(12).unwrap()) // count * duration = 60 mins. stuff doesn't happen that fast here.
        .set_enable_unit_suffix(false) // this seemed buggy for constellation (sometimes wouldn't engage)
        .with_http_listener(bind_metrics)
        .install()?;
    log::info!(
        "metrics server installed! listening on http://{}",
        bind_metrics
    );
    Ok(())
}
