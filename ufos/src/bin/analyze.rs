use cardinality_estimator_safe::Sketch;
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use fjall::{Config, PartitionCreateOptions, PartitionHandle};
use std::collections::BTreeMap;
use std::path::PathBuf;
use ufos::db_types::{db_complete, DbBytes};
use ufos::store_types::{
    AllTimeRollupKey, AllTimeRollupStaticPrefix, CountsValue, WeekTruncatedCursor, WeeklyRollupKey,
    WeeklyRollupStaticPrefix, WithCollection,
};

#[derive(Parser)]
#[command(about = "One-off data analysis of ufos rollup data")]
struct Cli {
    /// Path to the fjall data directory
    data: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Total estimated distinct users across all time and every group
    TotalUsers,
    /// Weekly estimated distinct users (excluding app.bsky.*/chat.bsky.*)
    WeeklyUsers,
    /// Weekly count of groups with >10 estimated distinct users (excluding app.bsky.*/chat.bsky.*)
    WeeklyGroups,
    /// Like weekly-groups but with the last NSID segment removed
    WeeklyParents,
}

fn week_label(week: WeekTruncatedCursor) -> String {
    let us = week.to_raw_u64();
    let secs = (us / 1_000_000) as i64;
    let dt = DateTime::<Utc>::from_timestamp(secs, 0).unwrap();
    dt.format("%Y-%m-%d").to_string()
}

fn is_excluded(nsid: &str) -> bool {
    nsid.starts_with("app.bsky.") || nsid.starts_with("chat.bsky.")
}

fn parent_prefix(nsid: &str) -> &str {
    let Some((pre, _)) = nsid.rsplit_once('.') else {
        eprintln!("no segments in nsid? nsid={nsid}");
        return nsid;
    };
    pre
}

fn total_users(rollups: &PartitionHandle) -> anyhow::Result<()> {
    eprintln!("scanning all-time rollups...");
    let mut global_sketch = Sketch::<14>::default();
    let mut all_time_count = 0u64;
    let prefix_bytes = AllTimeRollupStaticPrefix::default().to_db_bytes()?;
    for kv in rollups.prefix(prefix_bytes) {
        let (key_bytes, val_bytes) = kv?;
        let _key = db_complete::<AllTimeRollupKey>(&key_bytes)?;
        let val = db_complete::<CountsValue>(&val_bytes)?;
        global_sketch.merge(val.dids());
        all_time_count += 1;
    }
    println!("groups scanned: {all_time_count}");
    println!("estimated distinct users: {}", global_sketch.estimate());
    Ok(())
}

/// Scan weekly rollups once, returning week -> (merged sketch, per-group entries)
/// Only non-excluded groups are included.
#[expect(clippy::type_complexity)]
fn scan_weekly(
    rollups: &PartitionHandle,
) -> anyhow::Result<BTreeMap<u64, (Sketch<14>, Vec<(String, u64)>)>> {
    eprintln!("scanning weekly rollups...");
    let mut weekly_data: BTreeMap<u64, (Sketch<14>, Vec<(String, u64)>)> = BTreeMap::new();
    let prefix_bytes = WeeklyRollupStaticPrefix::default().to_db_bytes()?;
    let mut scanned = 0u64;
    for kv in rollups.prefix(prefix_bytes) {
        let (key_bytes, val_bytes) = kv?;
        let key = db_complete::<WeeklyRollupKey>(&key_bytes)?;
        let val = db_complete::<CountsValue>(&val_bytes)?;
        let week_us = key.cursor().to_raw_u64();
        let nsid_str = key.collection().to_string();
        let estimate = val.dids().estimate() as u64;

        let entry = weekly_data
            .entry(week_us)
            .or_insert_with(|| (Sketch::<14>::default(), Vec::new()));

        if !is_excluded(&nsid_str) {
            entry.0.merge(val.dids());
            entry.1.push((nsid_str, estimate));
        }

        scanned += 1;
        if scanned.is_multiple_of(500_000) {
            eprintln!("  ...scanned {scanned} weekly entries");
        }
    }
    eprintln!("  total weekly entries scanned: {scanned}");
    Ok(weekly_data)
}

fn weekly_users(rollups: &PartitionHandle) -> anyhow::Result<()> {
    let weekly_data = scan_weekly(rollups)?;
    println!("week\test_users");
    for (&week_us, (sketch, _)) in &weekly_data {
        let week = WeekTruncatedCursor::try_from_raw_u64(week_us)?;
        println!("{}\t{}", week_label(week), sketch.estimate());
    }
    Ok(())
}

fn weekly_groups(rollups: &PartitionHandle) -> anyhow::Result<()> {
    let weekly_data = scan_weekly(rollups)?;
    println!("week\tgroups");
    for (&week_us, (_, entries)) in &weekly_data {
        let week = WeekTruncatedCursor::try_from_raw_u64(week_us)?;
        let count = entries.iter().filter(|(_, est)| *est > 10).count();
        println!("{}\t{}", week_label(week), count);
    }
    Ok(())
}

fn weekly_parents(rollups: &PartitionHandle) -> anyhow::Result<()> {
    let weekly_data = scan_weekly(rollups)?;
    println!("week\tparents\ttop parent prefixes");
    for (&week_us, (_, entries)) in &weekly_data {
        let week = WeekTruncatedCursor::try_from_raw_u64(week_us)?;
        let mut parent_counts: BTreeMap<&str, usize> = BTreeMap::new();
        for (nsid, est) in entries {
            if *est > 10 {
                let parent = parent_prefix(nsid);
                *parent_counts.entry(parent).or_default() += 1;
            }
        }
        let total_parents = parent_counts.len();
        let mut sorted: Vec<_> = parent_counts.into_iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(&a.1));
        let top: Vec<String> = sorted
            .iter()
            .take(5)
            .map(|(prefix, count)| format!("{prefix}({count})"))
            .collect();
        println!(
            "{}\t{}\t{}",
            week_label(week),
            total_parents,
            top.join(", ")
        );
    }
    Ok(())
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    eprintln!("opening db at {:?}...", cli.data);
    let keyspace = Config::new(&cli.data).open()?;
    let rollups = keyspace.open_partition("rollups", PartitionCreateOptions::default())?;

    match cli.command {
        Command::TotalUsers => total_users(&rollups),
        Command::WeeklyUsers => weekly_users(&rollups),
        Command::WeeklyGroups => weekly_groups(&rollups),
        Command::WeeklyParents => weekly_parents(&rollups),
    }
}
