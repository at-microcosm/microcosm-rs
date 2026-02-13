use metrics_exporter_prometheus::PrometheusBuilder;
use slingshot::{
    Identity, Proxy, Repo, consume, error::MainTaskError, firehose_cache, healthcheck, serve,
};
use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use tokio_util::sync::CancellationToken;
use url::Url;

/// Slingshot record edge cache
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    /// Jetstream server to connect to (exclusive with --fixture). Provide either a wss:// URL, or a shorhand value:
    /// 'us-east-1', 'us-east-2', 'us-west-1', or 'us-west-2'
    #[arg(long, env = "SLINGSHOT_JETSTREAM")]
    jetstream: String,
    /// don't request zstd-compressed jetstream events
    ///
    /// reduces CPU at the expense of more ingress bandwidth
    #[arg(long, action, env = "SLINGSHOT_JETSTREAM_NO_ZSTD")]
    jetstream_no_zstd: bool,
    /// where to keep disk caches
    #[arg(long, env = "SLINGSHOT_CACHE_DIR")]
    cache_dir: PathBuf,
    /// where to listen for incomming requests
    ///
    /// cannot be used with acme -- if you need ipv6 see --acme-ipv6
    #[arg(long, env = "SLINGSHOT_BIND")]
    #[clap(default_value = "0.0.0.0:8080")]
    bind: SocketAddr,
    /// memory cache size in megabytes for records
    #[arg(long, env = "SLINGSHOT_RECORD_CACHE_MEMORY_MB")]
    #[clap(default_value_t = 64)]
    record_cache_memory_mb: usize,
    /// disk cache size in gigabytes for records
    #[arg(long, env = "SLINGSHOT_RECORD_CACHE_DISK_GB")]
    #[clap(default_value_t = 1)]
    record_cache_disk_gb: usize,
    /// memory cache size in megabytes for identities
    #[arg(long, env = "SLINGSHOT_IDENTITY_CACHE_MEMORY_MB")]
    #[clap(default_value_t = 64)]
    identity_cache_memory_mb: usize,
    /// disk cache size in gigabytes for identities
    #[arg(long, env = "SLINGSHOT_IDENTITY_CACHE_DISK_GB")]
    #[clap(default_value_t = 1)]
    identity_cache_disk_gb: usize,
    /// the address of this server
    ///
    /// used if --acme-domain is not set, defaulting to `--bind`
    #[arg(long, conflicts_with("tls_domain"), env = "SLINGSHOT_PUBLIC_HOST")]
    base_url: Option<Url>,
    /// the domain pointing to this server
    ///
    /// if present:
    /// - a did:web document will be served at /.well-known/did.json
    /// - the server will bind on port 443
    /// - if `--acme-contact` is present, the server will bind port 80 for http
    ///   challenges and attempt to auto-provision certs for `--tls-domain`
    /// - if `--acme-contact is absent, the server will load certs from the
    ///   `--tls-certs` folder, and try to reload them twice daily, guarded by
    ///   a lock file called `.cert-lock` in the `--tls-certs` folder.
    /// - TODO: a rate-limiter will be installed
    #[arg(
        long,
        conflicts_with("bind"),
        requires("tls_certs"),
        env = "SLINGSHOT_TLS_DOMAIN"
    )]
    tls_domain: Option<String>,
    /// a location to find/cache acme or other tls certs
    ///
    /// recommended in production, mind the file permissions.
    #[arg(long, env = "SLINGSHOT_TLS_CERTS_PATH")]
    tls_certs: Option<PathBuf>,
    /// listen for ipv6 when using acme or other tls
    ///
    /// you must also configure the relevant DNS records for this to work
    #[arg(long, action, requires("tls_domain"), env = "SLINGSHOT_TLS_IPV6")]
    tls_ipv6: bool,
    /// with tls configured on 443, say "please use https" on port 80
    ///
    /// note: configuring tls always binds port 80 as well, 443-only is TODO
    #[arg(long, action, requires("tls_domain"), env = "SLINGSHOT_WARN_INSECURE")]
    warn_insecure: bool,
    /// redirect acme http-01 challenges to this url
    ///
    /// useful if you're setting up a second instance that synchronizes its
    /// certs from a main instance doing acme.
    #[arg(
        long,
        conflicts_with("acme_contact"),
        requires("tls_domain"),
        env = "SLINGSHOT_ACME_CHALLENGE_REDIRECT"
    )]
    acme_challenge_redirect: Option<String>,
    /// email address for letsencrypt contact
    ///
    /// recommended in production, i guess?
    #[arg(long, requires("tls_domain"), env = "SLINGSHOT_ACME_CONTACT")]
    acme_contact: Option<String>,
    /// use the staging environment for letsencrypt
    ///
    /// recommended to initially test out new deployments with this to avoid
    /// letsencrypt rate limit problems.
    #[arg(long, action, requires("acme_contact"), env = "SLINGSHOT_ACME_STAGING")]
    acme_staging: bool,
    /// an web address to send healtcheck pings to every ~51s or so
    #[arg(long, env = "SLINGSHOT_HEALTHCHECK")]
    healthcheck: Option<String>,
    /// enable metrics collection and serving
    #[arg(long, action, env = "SLINGSHOT_COLLECT_METRICS")]
    collect_metrics: bool,
    /// metrics server's listen address
    #[arg(long, requires("collect_metrics"), env = "SLINGSHOT_BIND_METRICS")]
    #[clap(default_value = "[::]:8765")]
    bind_metrics: std::net::SocketAddr,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    tracing_subscriber::fmt::init();

    let shutdown = CancellationToken::new();

    let ctrlc_shutdown = shutdown.clone();
    ctrlc::set_handler(move || ctrlc_shutdown.cancel()).expect("failed to set ctrl-c handler");

    let args = Args::parse();

    let base_url: Url = args
        .base_url
        .or_else(|| {
            args.tls_domain
                .as_ref()
                .map(|d| Url::parse(&format!("https://{d}")).unwrap())
        })
        .unwrap_or_else(|| Url::parse(&format!("http://{}", args.bind)).unwrap());

    if args.collect_metrics {
        log::trace!("installing metrics server...");
        if let Err(e) = install_metrics_server(args.bind_metrics) {
            log::error!("failed to install metrics server: {e:?}");
        } else {
            log::info!("metrics listening at http://{}", args.bind_metrics);
        }
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
    let cache = firehose_cache(
        cache_dir.join("./firehose"),
        args.record_cache_memory_mb,
        args.record_cache_disk_gb,
    )
    .await?;
    log::info!("firehose cache ready.");

    let mut tasks: tokio::task::JoinSet<Result<(), MainTaskError>> = tokio::task::JoinSet::new();

    log::info!("starting identity service...");
    let identity = Identity::new(
        cache_dir.join("./identity"),
        args.identity_cache_memory_mb,
        args.identity_cache_disk_gb,
    )
    .await
    .map_err(|e| format!("identity setup failed: {e:?}"))?;
    let identity_refresher = identity.clone();
    let identity_shutdown = shutdown.clone();
    tasks.spawn(async move {
        identity_refresher.run_refresher(identity_shutdown).await?;
        Ok(())
    });
    log::info!("identity service ready.");

    let repo = Repo::new(identity.clone());
    let proxy = Proxy::new(identity.clone());

    let identity_for_server = identity.clone();
    let server_shutdown = shutdown.clone();
    let server_cache_handle = cache.clone();
    let bind = args.bind;
    tasks.spawn(async move {
        serve(
            server_cache_handle,
            identity_for_server,
            repo,
            proxy,
            base_url,
            args.tls_domain,
            args.tls_certs,
            args.tls_ipv6,
            args.warn_insecure,
            args.acme_challenge_redirect,
            args.acme_contact,
            args.acme_staging,
            server_shutdown,
            bind,
        )
        .await?;
        Ok(())
    });

    let identity_refreshable = identity.clone();
    let consumer_shutdown = shutdown.clone();
    let consumer_cache = cache.clone();
    tasks.spawn(async move {
        consume(
            args.jetstream,
            None,
            args.jetstream_no_zstd,
            identity_refreshable,
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

fn install_metrics_server(
    bind_metrics: std::net::SocketAddr,
) -> Result<(), metrics_exporter_prometheus::BuildError> {
    log::info!("installing metrics server...");
    PrometheusBuilder::new()
        .set_buckets(&[0.001, 0.006, 0.036, 0.216, 1.296, 7.776, 45.656])?
        .set_bucket_duration(std::time::Duration::from_secs(15))?
        .set_bucket_count(std::num::NonZero::new(4).unwrap()) // count * duration = bucket lifetime
        .set_enable_unit_suffix(false) // this seemed buggy for constellation (sometimes wouldn't engage)
        .with_http_listener(bind_metrics)
        .install()?;
    log::info!(
        "metrics server installed! listening on http://{}",
        bind_metrics
    );
    Ok(())
}
