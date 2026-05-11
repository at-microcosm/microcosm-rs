use clap::Parser;
use pocket::{Storage, serve};
use std::path::PathBuf;

/// Slingshot record edge cache
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    /// path to the sqlite db file
    #[arg(long)]
    db: Option<PathBuf>,
    /// just initialize the db and exit
    #[arg(long, action)]
    init_db: bool,
    /// the domain for serving a did doc (unused if running behind reflector)
    #[arg(long)]
    domain: Option<String>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    log::info!("👖 hi");
    let args = Args::parse();
    let domain = args.domain.unwrap_or("bad-example.com".into());
    let db_path = args.db.unwrap_or("prefs.sqlite3".into());
    if args.init_db {
        Storage::init(&db_path).unwrap();
        log::info!("👖 initialized db at {db_path:?}. bye")
    } else {
        let storage = Storage::connect(db_path).unwrap();
        serve(&domain, storage).await
    }
}
