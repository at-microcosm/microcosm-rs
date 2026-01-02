use clap::Parser;
use pocket::{Storage, serve};
use std::path::PathBuf;
use tokio::fs::create_dir_all;

/// Pocket: personal private preferences storage
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    /// path to the sqlite db file
    #[arg(long, default_value = "pocket-prefs.sqlite3")]
    db: PathBuf,
    /// just initialize the db and exit
    #[arg(long, action)]
    init_db: bool,
    /// the domain for tls and serving a did doc
    #[arg(long, default_value = "at-app.net")]
    domain: String,
    /// path to cache tls certs
    #[arg(long, default_value = "./certs")]
    certs_path: PathBuf,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    log::info!("👖 hi");
    let args = Args::parse();
    let Args {
        db,
        init_db,
        domain,
        certs_path,
    } = args;
    if init_db {
        Storage::init(&db).unwrap();
        log::info!("👖 initialized db at {db:?}. bye")
    } else {
        let storage = Storage::connect(db).unwrap();

        log::info!("configuring acme for https at {domain:?}...");
        create_dir_all(&certs_path).await.unwrap();

        serve(&domain, storage, certs_path).await
    }
}
