use crate::CachedRecord;
use foyer::{
    BlockEngineConfig, DeviceBuilder, FsDeviceBuilder, HybridCache, HybridCacheBuilder,
    PsyncIoEngineConfig,
};
use std::path::Path;

pub async fn firehose_cache(
    cache_dir: impl AsRef<Path>,
    memory_mb: usize,
    disk_gb: usize,
) -> Result<HybridCache<String, CachedRecord>, String> {
    let device = FsDeviceBuilder::new(cache_dir)
        .with_capacity(disk_gb * 2_usize.pow(30))
        .build()
        .map_err(|e| format!("foyer device setup error: {e}"))?;

    let engine = BlockEngineConfig::new(device).with_block_size(16 * 2_usize.pow(20)); // note: this does limit the max cached item size

    let cache = HybridCacheBuilder::new()
        .with_name("firehose")
        .memory(memory_mb * 2_usize.pow(20))
        .with_weighter(|k: &String, v: &CachedRecord| {
            std::mem::size_of_val(k.as_str()) + v.weight()
        })
        .storage()
        .with_io_engine_config(PsyncIoEngineConfig::default())
        .with_engine_config(engine)
        .build()
        .await
        .map_err(|e| format!("foyer setup error: {e:?}"))?;

    Ok(cache)
}
