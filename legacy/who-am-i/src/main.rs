use clap::{ArgAction, Parser};
use metrics_exporter_prometheus::{BuildError as PromBuildError, PrometheusBuilder};
use std::path::PathBuf;
use tokio_util::sync::CancellationToken;
use who_am_i::{Tokens, serve};

/// Aggregate links in the at-mosphere
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    /// secret key from which the cookie-signing key is derived
    ///
    /// must have at least 512 bits (64 bytes) of randomness
    ///
    /// eg: `cat /dev/urandom | head -c 64 | base64`
    #[arg(long, env)]
    app_secret: String,
    /// path to at-oauth private key (PEM pk8 format)
    ///
    /// generate with:
    ///
    ///     openssl ecparam -genkey -noout -name prime256v1 \
    ///         | openssl pkcs8 -topk8 -nocrypt -out <PATH-TO-PRIV-KEY>.pem
    #[arg(long, env)]
    oauth_private_key: Option<PathBuf>,
    /// path to jwt private key (PEM pk8 format)
    ///
    /// generate with:
    ///
    ///     openssl ecparam -genkey -noout -name prime256v1 \
    ///         | openssl pkcs8 -topk8 -nocrypt -out <PATH-TO-PRIV-KEY>.pem
    #[arg(long)]
    jwt_private_key: PathBuf,
    /// this server's client-reachable base url, for oauth redirect + jwt check
    ///
    /// required unless running in localhost mode with --dev
    #[arg(long, env)]
    base_url: Option<String>,
    /// host:port to bind to on startup
    #[arg(long, env, default_value = "127.0.0.1:9997")]
    bind: String,
    /// Enable dev mode
    ///
    /// enables automatic template reloading, uses localhost oauth config, etc
    #[arg(long, action)]
    dev: bool,
    /// Hosts who are allowed to one-click auth
    ///
    /// Pass this argument multiple times to allow multiple hosts
    #[arg(long = "allow_host", short = 'a', action = ArgAction::Append)]
    allowed_hosts: Vec<String>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let shutdown = CancellationToken::new();

    let ctrlc_shutdown = shutdown.clone();
    ctrlc::set_handler(move || ctrlc_shutdown.cancel()).expect("failed to set ctrl-c handler");

    let args = Args::parse();

    // let bind = args.bind.to_socket_addrs().expect("--bind must be ToSocketAddrs");

    let base = args.base_url.unwrap_or_else(|| {
        if args.dev {
            format!("http://{}", args.bind)
        } else {
            panic!("not in --dev mode so --base-url is required")
        }
    });

    if !args.dev && args.oauth_private_key.is_none() {
        panic!("--at-oauth-key is required except in --dev");
    } else if args.dev && args.oauth_private_key.is_some() {
        eprintln!("warn: --at-oauth-key is ignored in dev (localhost config)");
    }

    if args.allowed_hosts.is_empty() {
        panic!("at least one --allowed-host host must be set");
    }

    println!("starting with allowed_hosts hosts:");
    for host in &args.allowed_hosts {
        println!(" - {host}");
    }

    let tokens = Tokens::from_files(args.jwt_private_key).unwrap();

    if let Err(e) = install_metrics_server() {
        eprintln!("failed to install metrics server: {e:?}");
    };

    serve(
        shutdown,
        args.app_secret,
        args.oauth_private_key,
        tokens,
        base,
        args.bind,
        args.allowed_hosts,
        args.dev,
    )
    .await;
}

fn install_metrics_server() -> Result<(), PromBuildError> {
    println!("installing metrics server...");
    let host = [0, 0, 0, 0];
    let port = 8765;
    PrometheusBuilder::new()
        .set_enable_unit_suffix(false)
        .with_http_listener((host, port))
        .install()?;
    println!(
        "metrics server installed! listening on http://{}.{}.{}.{}:{port}",
        host[0], host[1], host[2], host[3]
    );
    Ok(())
}
