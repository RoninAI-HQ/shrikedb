use mimalloc::MiMalloc;
use std::sync::Arc;

use falcon_facade::listener::Listener;
use falcon_persistence::snapshot;
use falcon_server::engine_shard_set::EngineShardSet;
use falcon_server::main_service;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let port = std::env::var("FALCONDB_PORT").unwrap_or_else(|_| "6379".to_string());
    let addr = format!("127.0.0.1:{}", port);

    let num_shards: u32 = std::env::var("FALCONDB_SHARDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get() as u32)
                .unwrap_or(4)
        })
        .max(1);

    tracing::info!(
        "FalconDB v0.1.0 starting on {} with {} shards (co-located)",
        addr,
        num_shards
    );

    let registry = Arc::new(main_service::build_command_registry());
    let shard_set = Arc::new(EngineShardSet::new(num_shards, Arc::clone(&registry)));

    // Runtime for the listener + startup tasks.
    // Uses multi-thread to ensure the accept loop and channel sends don't block each other.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("failed to create listener runtime");

    rt.block_on(async {
        // Load RDB on startup
        let rdb_path = snapshot::default_rdb_path();
        if rdb_path.exists() {
            tracing::info!("Loading RDB from {}", rdb_path.display());
            match snapshot::load_rdb(&rdb_path) {
                Ok(entries) => {
                    let count = entries.len();
                    shard_set.load_entries(entries).await;
                    tracing::info!("Loaded {} keys from RDB", count);
                }
                Err(e) => {
                    tracing::error!("Failed to load RDB: {}", e);
                }
            }
        }

        // Collect connection senders from shard handles
        let conn_senders: Vec<_> = shard_set
            .all_shards()
            .iter()
            .map(|h| h.conn_tx.clone())
            .collect();

        // Run the lightweight listener (just accepts + distributes)
        let listener = Listener::new(&addr);
        listener.run_distributed(conn_senders).await.unwrap();
    });
}
