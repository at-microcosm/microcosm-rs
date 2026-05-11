use clap::Parser;
use fjall::{Config, PartitionCreateOptions};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser)]
#[command(about = "Run a major compaction over every ufos partition")]
struct Cli {
    /// path to the fjall data directory
    ///
    /// WARNING: MUST NOT RUN WHILE ANOTHER UFOS PROCESS IS USING IT
    data: PathBuf,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    eprintln!("opening db at {:?}...", cli.data);
    let keyspace = Config::new(&cli.data).open()?;

    for name in ["global", "feeds", "records", "rollups", "queues"] {
        let partition = keyspace.open_partition(name, PartitionCreateOptions::default())?;
        let size0 = partition.disk_space();
        eprintln!("beginning major compaction for {name} (original size: {size0})");
        let t0 = Instant::now();
        partition.major_compact()?;
        let dt = t0.elapsed();
        let sizef = partition.disk_space();
        let dsize = (sizef as i64) - (size0 as i64);
        eprintln!("completed compaction for {name} in {dt:?} (new size: {sizef}, {dsize})");
    }

    Ok(())
}
