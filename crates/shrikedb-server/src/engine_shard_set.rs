use std::sync::Arc;
use std::thread;

use shrikedb_core::compact_obj::PrimeValue;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

use bytes::BytesMut;
use shrikedb_facade::resp_parser::RespExpr;

use crate::command_registry::CommandRegistry;
use crate::shard_thread::{self, CrossShardTask, ShardControl};

/// Snapshot data from a single shard.
pub struct ShardSnapshotData {
    pub entries: Vec<(u16, Vec<u8>, PrimeValue, Option<u64>)>,
}

/// Entry to load into a shard.
pub struct LoadEntry {
    pub db_index: u16,
    pub key: Vec<u8>,
    pub value: PrimeValue,
    pub expire_ms: Option<u64>,
}

/// Handle to communicate with a shard thread.
#[derive(Clone)]
pub struct ShardHandle {
    /// Send new TCP connections to this shard.
    pub conn_tx: mpsc::Sender<TcpStream>,
    /// Send cross-shard command tasks.
    pub cross_tx: mpsc::Sender<CrossShardTask>,
    /// Send control messages (snapshot, load).
    pub control_tx: mpsc::Sender<ShardControl>,
}

/// Reply from a shard command dispatch (for MainService compatibility).
pub struct ShardReply {
    pub data: BytesMut,
    pub keep_alive: bool,
}

impl ShardHandle {
    /// Dispatch a command to this shard via cross-shard channel.
    /// Used by MainService for SAVE/BGSAVE and global commands.
    pub async fn dispatch(
        &self,
        args: Vec<RespExpr>,
        db_index: u16,
    ) -> Result<ShardReply, ()> {
        let (tx, rx) = oneshot::channel();
        let task = CrossShardTask {
            args,
            db_index,
            reply_tx: tx,
        };
        self.cross_tx.send(task).await.map_err(|_| ())?;
        let reply = rx.await.map_err(|_| ())?;
        Ok(ShardReply {
            data: reply.data,
            keep_alive: true,
        })
    }

    /// Request a snapshot from this shard.
    pub async fn snapshot(&self) -> Result<ShardSnapshotData, ()> {
        let (tx, rx) = oneshot::channel();
        self.control_tx
            .send(ShardControl::Snapshot(tx))
            .await
            .map_err(|_| ())?;
        rx.await.map_err(|_| ())
    }

    /// Load entries into this shard.
    pub async fn load(&self, entries: Vec<LoadEntry>) -> Result<(), ()> {
        self.control_tx
            .send(ShardControl::Load(entries))
            .await
            .map_err(|_| ())
    }
}

/// Manages N shard threads with co-located connections.
pub struct EngineShardSet {
    handles: Vec<ShardHandle>,
    num_shards: u32,
}

impl EngineShardSet {
    /// Spawn shard threads with co-located connection handling.
    pub fn new(num_shards: u32, registry: Arc<CommandRegistry>) -> Self {
        // Create channels for each shard
        let mut conn_txs = Vec::new();
        let mut conn_rxs = Vec::new();
        let mut cross_txs = Vec::new();
        let mut cross_rxs = Vec::new();
        let mut control_txs = Vec::new();
        let mut control_rxs = Vec::new();

        for _ in 0..num_shards {
            let (ctx, crx) = mpsc::channel::<TcpStream>(256);
            conn_txs.push(ctx);
            conn_rxs.push(Some(crx));

            let (xtx, xrx) = mpsc::channel::<CrossShardTask>(4096);
            cross_txs.push(xtx);
            cross_rxs.push(Some(xrx));

            let (stx, srx) = mpsc::channel::<ShardControl>(64);
            control_txs.push(stx);
            control_rxs.push(Some(srx));
        }

        // Build handles
        let handles: Vec<ShardHandle> = (0..num_shards as usize)
            .map(|i| ShardHandle {
                conn_tx: conn_txs[i].clone(),
                cross_tx: cross_txs[i].clone(),
                control_tx: control_txs[i].clone(),
            })
            .collect();

        // Spawn shard threads
        for shard_id in 0..num_shards {
            let i = shard_id as usize;
            let registry = Arc::clone(&registry);
            let conn_rx = conn_rxs[i].take().unwrap();
            let cross_rx = cross_rxs[i].take().unwrap();
            let control_rx = control_rxs[i].take().unwrap();
            // Each shard gets senders to ALL shards (for cross-shard dispatch)
            let all_cross_txs: Vec<mpsc::Sender<CrossShardTask>> = cross_txs.clone();

            thread::Builder::new()
                .name(format!("shard-{}", shard_id))
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to create shard runtime");

                    let local = tokio::task::LocalSet::new();
                    rt.block_on(local.run_until(shard_thread::run_shard_thread(
                        shard_id,
                        num_shards,
                        registry,
                        conn_rx,
                        cross_rx,
                        control_rx,
                        all_cross_txs,
                    )));
                })
                .expect("failed to spawn shard thread");
        }

        tracing::info!("Started {} co-located shard threads", num_shards);

        EngineShardSet {
            handles,
            num_shards,
        }
    }

    pub fn num_shards(&self) -> u32 {
        self.num_shards
    }

    pub fn shard(&self, shard_id: u32) -> &ShardHandle {
        &self.handles[shard_id as usize]
    }

    pub fn all_shards(&self) -> &[ShardHandle] {
        &self.handles
    }

    /// Collect snapshots from all shards (for SAVE/BGSAVE).
    pub async fn collect_snapshots(&self) -> Result<Vec<ShardSnapshotData>, ()> {
        let mut futures = Vec::new();
        for handle in &self.handles {
            let h = handle.clone();
            futures.push(tokio::spawn(async move { h.snapshot().await }));
        }
        let mut snapshots = Vec::with_capacity(self.num_shards as usize);
        for fut in futures {
            match fut.await {
                Ok(Ok(snap)) => snapshots.push(snap),
                _ => return Err(()),
            }
        }
        Ok(snapshots)
    }

    /// Load RDB entries, distributing each to the correct shard.
    pub async fn load_entries(
        &self,
        entries: Vec<shrikedb_persistence::rdb_load::RdbEntry>,
    ) {
        use crate::sharding;

        let mut shard_entries: Vec<Vec<LoadEntry>> =
            (0..self.num_shards).map(|_| Vec::new()).collect();

        for entry in entries {
            let sid = sharding::shard(&entry.key, self.num_shards);
            shard_entries[sid as usize].push(LoadEntry {
                db_index: entry.db_index as u16,
                key: entry.key,
                value: entry.value,
                expire_ms: entry.expire_ms,
            });
        }

        let mut futures = Vec::new();
        for (sid, entries) in shard_entries.into_iter().enumerate() {
            if !entries.is_empty() {
                let h = self.handles[sid].clone();
                futures.push(tokio::spawn(async move { h.load(entries).await }));
            }
        }
        for fut in futures {
            let _ = fut.await;
        }
    }
}
