use clap::Parser;
use jetstream::events::Cursor;
use metrics::{describe_gauge, gauge, Unit};
use metrics_exporter_prometheus::PrometheusBuilder;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};
use tokio::task::JoinSet;
use ufos::consumer;
use ufos::file_consumer;
use ufos::server;
use ufos::storage::{StorageWhatever, StoreBackground, StoreReader, StoreWriter};
use ufos::storage_fjall::FjallStorage;
use ufos::store_types::SketchSecretPrefix;
use ufos::{nice_duration, ConsumerInfo};

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

/// Aggregate links in the at-mosphere
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
struct Args {
    /// Jetstream server to connect to (exclusive with --fixture). Provide either a wss:// URL, or a shorhand value:
    /// 'us-east-1', 'us-east-2', 'us-west-1', or 'us-west-2'
    #[arg(long)]
    jetstream: String,
    /// allow changing jetstream endpoints
    #[arg(long, action)]
    jetstream_force: bool,
    /// don't request zstd-compressed jetstream events
    ///
    /// reduces CPU at the expense of more ingress bandwidth
    #[arg(long, action)]
    jetstream_no_zstd: bool,
    /// Location to store persist data to disk
    #[arg(long)]
    data: PathBuf,
    /// DEBUG: don't start the jetstream consumer or its write loop
    #[arg(long, action)]
    pause_writer: bool,
    /// Adjust runtime settings like background task intervals for efficient backfill
    #[arg(long, action)]
    backfill: bool,
    /// DEBUG: force the rw loop to fall behind  by pausing it
    /// todo: restore this
    #[arg(long, action)]
    pause_rw: bool,
    /// reset the rollup cursor, scrape through missed things in the past (backfill)
    #[arg(long, action)]
    reroll: bool,
    /// DEBUG: interpret jetstream as a file fixture
    #[arg(long, action)]
    jetstream_fixture: bool,
    /// ufos server's listen address
    #[arg(long, default_value = "0.0.0.0:9990")]
    bind: std::net::SocketAddr,
    /// metrics server's listen address
    #[arg(long, default_value = "0.0.0.0:8765")]
    bind_metrics: std::net::SocketAddr,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();
    let jetstream = args.jetstream.clone();
    let (read_store, write_store, cursor, sketch_secret) = FjallStorage::init(
        args.data.clone(),
        jetstream,
        args.jetstream_force,
        Default::default(),
    )?;
    go(args, read_store, write_store, cursor, sketch_secret).await?;
    Ok(())
}

async fn go<B: StoreBackground + 'static>(
    args: Args,
    read_store: impl StoreReader + 'static + Clone,
    mut write_store: impl StoreWriter<B> + 'static,
    cursor: Option<Cursor>,
    sketch_secret: SketchSecretPrefix,
) -> anyhow::Result<()> {
    let mut whatever_tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();
    let mut consumer_tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

    println!("starting server with storage...");
    let serving = server::serve(read_store.clone(), args.bind);
    whatever_tasks.spawn(async move {
        serving.await.map_err(|e| {
            log::warn!("server ended: {e}");
            anyhow::anyhow!(e)
        })
    });

    if args.pause_writer {
        log::info!("not starting jetstream or the write loop.");
        for t in whatever_tasks.join_all().await {
            if let Err(e) = t {
                return Err(anyhow::anyhow!(e));
            }
        }
        return Ok(());
    }

    let batches = if args.jetstream_fixture {
        log::info!("starting with jestream file fixture: {:?}", args.jetstream);
        file_consumer::consume(args.jetstream.into(), sketch_secret, cursor).await?
    } else {
        log::info!(
            "starting consumer with cursor: {cursor:?} from {:?} ago",
            cursor.map(|c| c.elapsed())
        );
        consumer::consume(&args.jetstream, cursor, false, sketch_secret).await?
    };

    let rolling = write_store
        .background_tasks(args.reroll)?
        .run(args.backfill);
    whatever_tasks.spawn(async move {
        rolling
            .await
            .inspect_err(|e| log::warn!("rollup ended: {e}"))?;
        Ok(())
    });

    consumer_tasks.spawn(async move {
        write_store
            .receive_batches(batches)
            .await
            .inspect_err(|e| log::warn!("consumer ended: {e}"))?;
        Ok(())
    });

    whatever_tasks.spawn(async move {
        do_update_stuff(read_store).await;
        log::warn!("status task ended");
        Ok(())
    });

    install_metrics_server(args.bind_metrics)?;

    for (i, t) in consumer_tasks.join_all().await.iter().enumerate() {
        log::warn!("task {i} done: {t:?}");
    }

    println!("consumer tasks all completed, killing the others");
    whatever_tasks.shutdown().await;

    println!("bye!");

    Ok(())
}

fn install_metrics_server(bind_metrics: std::net::SocketAddr) -> anyhow::Result<()> {
    log::info!("installing metrics server...");
    PrometheusBuilder::new()
        .set_quantiles(&[0.5, 0.9, 0.99, 1.0])?
        .set_bucket_duration(Duration::from_secs(60))?
        .set_bucket_count(std::num::NonZero::new(10).unwrap()) // count * duration = 10 mins. stuff doesn't happen that fast here.
        .set_enable_unit_suffix(false) // this seemed buggy for constellation (sometimes wouldn't engage)
        .with_http_listener(bind_metrics)
        .install()?;
    log::info!(
        "metrics server installed! listening on http://{}",
        bind_metrics
    );
    Ok(())
}

async fn do_update_stuff(read_store: impl StoreReader) {
    describe_gauge!(
        "persisted_cursor_age",
        Unit::Microseconds,
        "microseconds between our clock and the latest persisted event's cursor"
    );
    describe_gauge!(
        "rollup_cursor_age",
        Unit::Microseconds,
        "microseconds between our clock and the latest rollup cursor"
    );
    let started_at = std::time::SystemTime::now();
    let mut first_cursor = None;
    let mut first_rollup = None;
    let mut last_at = std::time::SystemTime::now();
    let mut last_cursor = None;
    let mut last_rollup = None;
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(4));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        interval.tick().await;
        read_store.update_metrics();
        match read_store.get_consumer_info().await {
            Err(e) => log::warn!("failed to get jetstream consumer info: {e:?}"),
            Ok(ConsumerInfo::Jetstream {
                latest_cursor,
                rollup_cursor,
                ..
            }) => {
                let now = std::time::SystemTime::now();
                let latest_cursor = latest_cursor.map(Cursor::from_raw_u64);
                let rollup_cursor = rollup_cursor.map(Cursor::from_raw_u64);
                backfill_info(
                    latest_cursor,
                    rollup_cursor,
                    last_cursor,
                    last_rollup,
                    last_at,
                    first_cursor,
                    first_rollup,
                    started_at,
                    now,
                );
                first_cursor = first_cursor.or(latest_cursor);
                first_rollup = first_rollup.or(rollup_cursor);
                last_cursor = latest_cursor;
                last_rollup = rollup_cursor;
                last_at = now;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn backfill_info(
    latest_cursor: Option<Cursor>,
    rollup_cursor: Option<Cursor>,
    last_cursor: Option<Cursor>,
    last_rollup: Option<Cursor>,
    last_at: SystemTime,
    first_cursor: Option<Cursor>,
    first_rollup: Option<Cursor>,
    started_at: SystemTime,
    now: SystemTime,
) {
    if let Some(cursor) = latest_cursor {
        gauge!("persisted_cursor_age").set(cursor.elapsed_micros_f64());
    }
    if let Some(cursor) = rollup_cursor {
        gauge!("rollup_cursor_age").set(cursor.elapsed_micros_f64());
    }

    let nice_dt_two_maybes = |earlier: Option<Cursor>, later: Option<Cursor>| match (earlier, later)
    {
        (Some(earlier), Some(later)) => match later.duration_since(&earlier) {
            Ok(dt) => nice_duration(dt),
            Err(e) => {
                let rev_dt = e.duration();
                format!("+{}", nice_duration(rev_dt))
            }
        },
        _ => "unknown".to_string(),
    };

    let rate = |mlatest: Option<Cursor>, msince: Option<Cursor>, real: Duration| {
        mlatest
            .zip(msince)
            .map(|(latest, since)| {
                latest
                    .duration_since(&since)
                    .unwrap_or(Duration::from_millis(1))
            })
            .map(|dtc| format!("{:.2}", dtc.as_secs_f64() / real.as_secs_f64()))
            .unwrap_or("??".into())
    };

    let dt_real = now
        .duration_since(last_at)
        .unwrap_or(Duration::from_millis(1));

    let dt_real_total = now
        .duration_since(started_at)
        .unwrap_or(Duration::from_millis(1));

    let cursor_rate = rate(latest_cursor, last_cursor, dt_real);
    let cursor_avg = rate(latest_cursor, first_cursor, dt_real_total);

    let rollup_rate = rate(rollup_cursor, last_rollup, dt_real);
    let rollup_avg = rate(rollup_cursor, first_rollup, dt_real_total);

    log::trace!(
        "cursor: {} behind (→{}, {cursor_rate}x, {cursor_avg}x avg). rollup: {} behind (→{}, {rollup_rate}x, {rollup_avg}x avg).",
        latest_cursor.map(|c| c.elapsed().map(nice_duration).unwrap_or("++".to_string())).unwrap_or("?".to_string()),
        nice_dt_two_maybes(last_cursor, latest_cursor),
        rollup_cursor.map(|c| c.elapsed().map(nice_duration).unwrap_or("++".to_string())).unwrap_or("?".to_string()),
        nice_dt_two_maybes(last_rollup, rollup_cursor),
    );
}
