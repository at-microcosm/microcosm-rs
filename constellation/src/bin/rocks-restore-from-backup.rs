use crate::time::Instant;
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use std::time;
use tracing::{error, info};

#[cfg(feature = "rocks")]
use rocksdb::backup::{BackupEngine, BackupEngineOptions, RestoreOptions};

/// Restore a rocksdb database
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// the directory to restore the database into
    dest: PathBuf,
    /// restore from a rocksdb backup folder
    #[arg(long, conflicts_with_all = ["endpoint", "prefix", "concurrency"])]
    fs_dir: Option<PathBuf>,
    /// restore from public object storage
    #[arg(long, default_value = "https://constellation.t3.storage.dev")]
    endpoint: String,
    /// specific backup to restore (default: latest)
    #[arg(long)]
    backup: Option<u32>,
    /// object-store key prefix
    #[arg(long, default_value = "")]
    prefix: String,
    /// concurrency limit for object storage operations
    ///
    /// TODO: wire this through for the filesystem mode as well.
    #[arg(long)]
    concurrency: Option<usize>,
}

#[cfg(feature = "rocks")]
fn main() -> Result<()> {
    use tracing_subscriber::EnvFilter;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let t0 = Instant::now();

    let result = if let Some(fs_dir) = args.fs_dir {
        info!(
            ?fs_dir,
            ?args.dest,
            backup = describe_backup(args.backup),
            mode = "rocksdb fs",
            "restoring rocksdb backup..."
        );
        restore_from_dir(&fs_dir, &args.dest, args.backup)
    } else {
        info!(
            args.endpoint,
            args.prefix,
            ?args.dest,
            backup = describe_backup(args.backup),
            mode = "eat-rocks object storage",
            "restoring rocksdb backup..."
        );
        restore_from_object_store(
            &args.endpoint,
            &args.prefix,
            &args.dest,
            args.backup,
            args.concurrency,
        )
    };

    match result {
        Ok(()) => info!(
            elapsed = ?t0.elapsed(),
            "backup restored."
        ),
        Err(err) => error!(
            ?err,
            elapsed = ?t0.elapsed(),
            "failed to restore backup."
        ),
    }
    Ok(())
}

fn describe_backup(backup: Option<u32>) -> String {
    match backup {
        Some(id) => format!("#{id}"),
        None => "(latest)".to_string(),
    }
}

#[cfg(feature = "rocks")]
fn restore_from_dir(
    fs_dir: &std::path::Path,
    dest: &std::path::Path,
    backup_id: Option<u32>,
) -> Result<()> {
    let mut engine = BackupEngine::open(&BackupEngineOptions::new(fs_dir)?, &rocksdb::Env::new()?)?;
    let opts = RestoreOptions::default();
    match backup_id {
        Some(id) => engine.restore_from_backup(dest, dest, &opts, id)?,
        None => engine.restore_from_latest_backup(dest, dest, &opts)?,
    }
    Ok(())
}

#[cfg(feature = "rocks")]
fn restore_from_object_store(
    endpoint: &str,
    prefix: &str,
    dest: &std::path::Path,
    backup_id: Option<u32>,
    concurrency: Option<usize>,
) -> Result<()> {
    use eat_rocks::{public_bucket, restore, RestoreOptions};
    use tokio::runtime::Runtime;

    let rt = Runtime::new()?;
    rt.block_on(async {
        let store = public_bucket(endpoint)?;
        restore(
            store,
            prefix,
            dest,
            RestoreOptions {
                backup_id: backup_id.map(u64::from),
                concurrency: concurrency.unwrap_or(RestoreOptions::default().concurrency),
                ..Default::default()
            },
        )
        .await
    })?;
    Ok(())
}
