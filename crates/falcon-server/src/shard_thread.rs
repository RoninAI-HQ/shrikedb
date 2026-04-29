use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};

use falcon_facade::conn_context::ConnContext;
use falcon_facade::reply_builder::ReplyBuilder;
use falcon_facade::resp_parser::{ParseResult, RespExpr, RespParser};

use crate::command_registry::CommandRegistry;
use crate::engine_shard::EngineShard;
use crate::engine_shard_set::{LoadEntry, ShardSnapshotData};
use crate::main_service::{self, CommandType};
use crate::sharding;

/// A task sent from one shard to another for cross-shard command execution.
pub struct CrossShardTask {
    pub args: Vec<RespExpr>,
    pub db_index: u16,
    pub reply_tx: oneshot::Sender<CrossShardReply>,
}

pub struct CrossShardReply {
    pub data: BytesMut,
}

/// Control messages sent to shard threads (for snapshot, load, etc.)
pub enum ShardControl {
    Snapshot(oneshot::Sender<ShardSnapshotData>),
    Load(Vec<LoadEntry>),
}

/// Run a shard thread. This is the main entry point called from EngineShardSet.
/// Each shard thread owns an EngineShard, handles its own connections, and
/// communicates with other shards only for cross-shard commands.
pub async fn run_shard_thread(
    shard_id: u32,
    num_shards: u32,
    registry: Arc<CommandRegistry>,
    mut conn_rx: mpsc::Receiver<TcpStream>,
    mut cross_rx: mpsc::Receiver<CrossShardTask>,
    mut control_rx: mpsc::Receiver<ShardControl>,
    cross_txs: Vec<mpsc::Sender<CrossShardTask>>,
) {
    let shard = Rc::new(RefCell::new(EngineShard::new()));
    shard.borrow_mut().update_time(main_service::now_ms());

    tracing::debug!("Shard {} co-located event loop started", shard_id);

    // Spawn a background task for periodic time updates and expiry sweeps.
    // This runs as a spawned local task, so it cooperates with connection tasks.
    {
        let shard_rc = Rc::clone(&shard);
        tokio::task::spawn_local(async move {
            let mut time_interval = tokio::time::interval(std::time::Duration::from_millis(1));
            let mut expiry_interval = tokio::time::interval(std::time::Duration::from_millis(100));
            loop {
                tokio::select! {
                    _ = time_interval.tick() => {
                        shard_rc.borrow_mut().update_time(main_service::now_ms());
                    }
                    _ = expiry_interval.tick() => {
                        let mut s = shard_rc.borrow_mut();
                        s.update_time(main_service::now_ms());
                        for db_idx in 0..16u16 {
                            if s.db_slice.db_size(db_idx) > 0 {
                                s.db_slice.expire_sweep(db_idx, 20);
                            }
                        }
                    }
                }
            }
        });
    }

    // Main loop: accept connections, process cross-shard commands, handle control messages.
    loop {
        tokio::select! {
            biased;

            // Process cross-shard commands (high priority)
            Some(task) = cross_rx.recv() => {
                let mut s = shard.borrow_mut();
                s.update_time(main_service::now_ms());
                let mut reply_buf = BytesMut::with_capacity(128);
                let mut ctx = ConnContext { db_index: task.db_index };
                registry.dispatch(&task.args, &mut ctx, &mut s, &mut reply_buf);
                let _ = task.reply_tx.send(CrossShardReply { data: reply_buf });
            }

            // Control messages (snapshot, load)
            Some(ctrl) = control_rx.recv() => {
                match ctrl {
                    ShardControl::Snapshot(reply_tx) => {
                        let s = shard.borrow();
                        let snap = collect_snapshot(&s);
                        let _ = reply_tx.send(snap);
                    }
                    ShardControl::Load(entries) => {
                        let mut s = shard.borrow_mut();
                        load_entries(&mut s, entries);
                    }
                }
            }

            // Accept new connections
            Some(stream) = conn_rx.recv() => {
                let shard_rc = Rc::clone(&shard);
                let registry = Arc::clone(&registry);
                let cross_txs = cross_txs.clone();
                tokio::task::spawn_local(async move {
                    handle_connection(
                        stream, shard_rc, shard_id, num_shards,
                        registry, cross_txs,
                    ).await;
                });
            }
        }
    }
}

/// Handle a single connection on this shard thread.
/// Fast path: commands targeting this shard execute directly.
/// Slow path: commands targeting other shards go via cross-shard channels.
async fn handle_connection(
    mut stream: TcpStream,
    shard: Rc<RefCell<EngineShard>>,
    shard_id: u32,
    num_shards: u32,
    registry: Arc<CommandRegistry>,
    cross_txs: Vec<mpsc::Sender<CrossShardTask>>,
) {
    let mut parser = RespParser::new();
    let mut read_buf = BytesMut::with_capacity(16384);
    let mut write_buf = BytesMut::with_capacity(4096);
    let mut ctx = ConnContext::new();

    loop {
        // Parse and dispatch all complete commands
        loop {
            match parser.parse(&mut read_buf) {
                ParseResult::Complete(args) => {
                    let keep_alive = dispatch_command(
                        &args, &mut ctx, &mut write_buf,
                        &shard, shard_id, num_shards,
                        &registry, &cross_txs,
                    ).await;
                    if !keep_alive {
                        let _ = stream.write_all(&write_buf).await;
                        return;
                    }
                }
                ParseResult::Incomplete => break,
                ParseResult::Error(e) => {
                    ReplyBuilder::new(&mut write_buf)
                        .send_err(&format!("Protocol error: {}", e));
                    let _ = stream.write_all(&write_buf).await;
                    return;
                }
            }
        }

        // Flush pending replies
        if !write_buf.is_empty() {
            if stream.write_all(&write_buf).await.is_err() {
                return;
            }
            write_buf.clear();
        }

        // Read more data
        match stream.read_buf(&mut read_buf).await {
            Ok(0) => return,
            Ok(_) => {}
            Err(_) => return,
        }
    }
}

/// Dispatch a single command. Returns false if connection should close (QUIT).
async fn dispatch_command(
    args: &[RespExpr],
    conn_ctx: &mut ConnContext,
    reply_buf: &mut BytesMut,
    shard: &Rc<RefCell<EngineShard>>,
    shard_id: u32,
    num_shards: u32,
    registry: &CommandRegistry,
    cross_txs: &[mpsc::Sender<CrossShardTask>],
) -> bool {
    if args.is_empty() {
        ReplyBuilder::new(reply_buf).send_err("empty command");
        return true;
    }

    let cmd_bytes = match args[0].as_bytes() {
        Some(b) => b.to_ascii_uppercase(),
        None => {
            ReplyBuilder::new(reply_buf).send_err("invalid command");
            return true;
        }
    };
    let cmd_str = match std::str::from_utf8(&cmd_bytes) {
        Ok(s) => s,
        Err(_) => {
            ReplyBuilder::new(reply_buf).send_err("invalid command encoding");
            return true;
        }
    };

    // QUIT: close connection
    if cmd_str == "QUIT" {
        ReplyBuilder::new(reply_buf).send_ok();
        return false;
    }

    // SELECT: connection-local
    if cmd_str == "SELECT" {
        return handle_select(args, conn_ctx, reply_buf);
    }

    // SAVE/BGSAVE: handled at the EngineShardSet level, not here.
    // The connection sends these via cross-shard to shard 0 which coordinates.
    // For now, we execute SAVE/BGSAVE as a no-key command on the local shard
    // (the real implementation goes through the control channel from main).
    if cmd_str == "SAVE" || cmd_str == "BGSAVE" {
        ReplyBuilder::new(reply_buf)
            .send_err("SAVE/BGSAVE must be issued via the control interface");
        return true;
    }

    let entry = match registry.find(cmd_str) {
        Some(e) => e,
        None => {
            ReplyBuilder::new(reply_buf).send_err(&format!(
                "unknown command '{}', with args beginning with: ",
                cmd_str.to_lowercase()
            ));
            return true;
        }
    };

    // Arity check
    let argc = args.len() as i16;
    if (entry.arity > 0 && argc != entry.arity) || (entry.arity < 0 && argc < -entry.arity) {
        ReplyBuilder::new(reply_buf).send_err(&format!(
            "wrong number of arguments for '{}' command",
            cmd_str.to_lowercase()
        ));
        return true;
    }

    match main_service::classify_command(cmd_str, entry) {
        CommandType::NoKeys => {
            // PING, ECHO, COMMAND, INFO: execute locally
            let mut s = shard.borrow_mut();
            s.update_time(main_service::now_ms());
            registry.dispatch(args, conn_ctx, &mut s, reply_buf);
        }
        CommandType::SingleKey => {
            let key = args[entry.first_key as usize].as_bytes().unwrap_or(b"");
            let target = sharding::shard(key, num_shards);
            if target == shard_id {
                // FAST PATH: execute locally
                let mut s = shard.borrow_mut();
                s.update_time(main_service::now_ms());
                registry.dispatch(args, conn_ctx, &mut s, reply_buf);
            } else {
                // SLOW PATH: send to target shard
                dispatch_remote(args, conn_ctx.db_index, target, cross_txs, reply_buf).await;
            }
        }
        CommandType::MultiKeySameHandler => {
            dispatch_multi_key_sum(
                args, conn_ctx, reply_buf, shard, shard_id, num_shards, registry, cross_txs,
            ).await;
        }
        CommandType::MGet => {
            dispatch_mget(
                args, conn_ctx, reply_buf, shard, shard_id, num_shards, registry, cross_txs,
            ).await;
        }
        CommandType::MSet => {
            dispatch_mset(
                args, cmd_str, conn_ctx, reply_buf, shard, shard_id, num_shards, registry, cross_txs,
            ).await;
        }
        CommandType::Rename => {
            let from = args[1].as_bytes().unwrap_or(b"");
            let to = args[2].as_bytes().unwrap_or(b"");
            let s1 = sharding::shard(from, num_shards);
            let s2 = sharding::shard(to, num_shards);
            if s1 != s2 {
                ReplyBuilder::new(reply_buf)
                    .send_err("CROSSSLOT Keys in request don't hash to the same slot");
            } else if s1 == shard_id {
                let mut s = shard.borrow_mut();
                s.update_time(main_service::now_ms());
                registry.dispatch(args, conn_ctx, &mut s, reply_buf);
            } else {
                dispatch_remote(args, conn_ctx.db_index, s1, cross_txs, reply_buf).await;
            }
        }
        CommandType::Global => {
            dispatch_global(
                args, cmd_str, conn_ctx, reply_buf, shard, shard_id, num_shards, registry, cross_txs,
            ).await;
        }
    }
    true
}

/// Send a command to a remote shard and write the reply into reply_buf.
async fn dispatch_remote(
    args: &[RespExpr],
    db_index: u16,
    target: u32,
    cross_txs: &[mpsc::Sender<CrossShardTask>],
    reply_buf: &mut BytesMut,
) {
    let (tx, rx) = oneshot::channel();
    let task = CrossShardTask {
        args: args.to_vec(),
        db_index,
        reply_tx: tx,
    };
    if cross_txs[target as usize].send(task).await.is_err() {
        ReplyBuilder::new(reply_buf).send_err("shard unavailable");
        return;
    }
    match rx.await {
        Ok(reply) => reply_buf.extend_from_slice(&reply.data),
        Err(_) => ReplyBuilder::new(reply_buf).send_err("shard unavailable"),
    }
}

/// DEL, EXISTS, UNLINK: split keys by shard, sum integer results.
async fn dispatch_multi_key_sum(
    args: &[RespExpr],
    conn_ctx: &mut ConnContext,
    reply_buf: &mut BytesMut,
    shard: &Rc<RefCell<EngineShard>>,
    shard_id: u32,
    num_shards: u32,
    registry: &CommandRegistry,
    cross_txs: &[mpsc::Sender<CrossShardTask>],
) {
    let cmd = args[0].clone();
    let mut shard_keys: HashMap<u32, Vec<RespExpr>> = HashMap::new();
    for i in 1..args.len() {
        let key = args[i].as_bytes().unwrap_or(b"");
        let sid = sharding::shard(key, num_shards);
        shard_keys.entry(sid).or_default().push(args[i].clone());
    }

    let mut total: i64 = 0;

    // Execute local keys directly
    if let Some(local_keys) = shard_keys.remove(&shard_id) {
        let mut local_args = vec![cmd.clone()];
        local_args.extend(local_keys);
        let mut local_buf = BytesMut::with_capacity(32);
        {
            let mut s = shard.borrow_mut();
            s.update_time(main_service::now_ms());
            registry.dispatch(&local_args, conn_ctx, &mut s, &mut local_buf);
        }
        total += main_service::parse_resp_integer(&local_buf).unwrap_or(0);
    }

    // Fan out remote keys
    let mut futures = Vec::new();
    for (sid, keys) in shard_keys {
        let mut shard_args = vec![cmd.clone()];
        shard_args.extend(keys);
        let (tx, rx) = oneshot::channel();
        let task = CrossShardTask {
            args: shard_args,
            db_index: conn_ctx.db_index,
            reply_tx: tx,
        };
        let sender = cross_txs[sid as usize].clone();
        futures.push(tokio::task::spawn_local(async move {
            let _ = sender.send(task).await;
            rx.await
        }));
    }

    for fut in futures {
        if let Ok(Ok(reply)) = fut.await {
            total += main_service::parse_resp_integer(&reply.data).unwrap_or(0);
        }
    }

    ReplyBuilder::new(reply_buf).send_integer(total);
}

/// MGET: split by shard, execute local directly, merge in order.
async fn dispatch_mget(
    args: &[RespExpr],
    conn_ctx: &mut ConnContext,
    reply_buf: &mut BytesMut,
    shard: &Rc<RefCell<EngineShard>>,
    shard_id: u32,
    num_shards: u32,
    registry: &CommandRegistry,
    cross_txs: &[mpsc::Sender<CrossShardTask>],
) {
    let num_keys = args.len() - 1;
    let mut shard_positions: HashMap<u32, Vec<(usize, RespExpr)>> = HashMap::new();
    for i in 0..num_keys {
        let key = args[i + 1].as_bytes().unwrap_or(b"");
        let sid = sharding::shard(key, num_shards);
        shard_positions.entry(sid).or_default().push((i, args[i + 1].clone()));
    }

    let mut results: Vec<Option<Vec<u8>>> = vec![None; num_keys];

    // Local keys
    if let Some(local) = shard_positions.remove(&shard_id) {
        let positions: Vec<usize> = local.iter().map(|(p, _)| *p).collect();
        let mut local_args = vec![RespExpr::from_static(b"MGET")];
        for (_, key) in &local {
            local_args.push(key.clone());
        }
        let mut local_buf = BytesMut::with_capacity(128);
        {
            let mut s = shard.borrow_mut();
            s.update_time(main_service::now_ms());
            registry.dispatch(&local_args, conn_ctx, &mut s, &mut local_buf);
        }
        let values = main_service::parse_resp_array(&local_buf);
        for (idx, pos) in positions.iter().enumerate() {
            if idx < values.len() {
                results[*pos] = values[idx].clone();
            }
        }
    }

    // Remote keys
    let mut futures = Vec::new();
    for (sid, key_positions) in shard_positions {
        let positions: Vec<usize> = key_positions.iter().map(|(p, _)| *p).collect();
        let mut shard_args = vec![RespExpr::from_static(b"MGET")];
        for (_, key) in &key_positions {
            shard_args.push(key.clone());
        }
        let (tx, rx) = oneshot::channel();
        let task = CrossShardTask {
            args: shard_args,
            db_index: conn_ctx.db_index,
            reply_tx: tx,
        };
        let sender = cross_txs[sid as usize].clone();
        futures.push((positions, tokio::task::spawn_local(async move {
            let _ = sender.send(task).await;
            rx.await
        })));
    }

    for (positions, fut) in futures {
        if let Ok(Ok(reply)) = fut.await {
            let values = main_service::parse_resp_array(&reply.data);
            for (idx, pos) in positions.iter().enumerate() {
                if idx < values.len() {
                    results[*pos] = values[idx].clone();
                }
            }
        }
    }

    let mut rb = ReplyBuilder::new(reply_buf);
    rb.send_array_len(num_keys);
    for val in &results {
        match val {
            Some(v) => rb.send_bulk_string(v),
            None => rb.send_null(),
        }
    }
}

/// MSET: split by shard, execute local directly, fan out remote.
async fn dispatch_mset(
    args: &[RespExpr],
    cmd_str: &str,
    conn_ctx: &mut ConnContext,
    reply_buf: &mut BytesMut,
    shard: &Rc<RefCell<EngineShard>>,
    shard_id: u32,
    num_shards: u32,
    registry: &CommandRegistry,
    cross_txs: &[mpsc::Sender<CrossShardTask>],
) {
    if (args.len() - 1) % 2 != 0 {
        ReplyBuilder::new(reply_buf).send_err(&format!(
            "wrong number of arguments for '{}' command", cmd_str.to_lowercase()
        ));
        return;
    }

    // MSETNX: all-or-nothing, require same shard
    if cmd_str == "MSETNX" {
        let mut target: Option<u32> = None;
        let mut i = 1;
        while i < args.len() {
            let key = args[i].as_bytes().unwrap_or(b"");
            let sid = sharding::shard(key, num_shards);
            match target {
                None => target = Some(sid),
                Some(s) if s != sid => {
                    ReplyBuilder::new(reply_buf)
                        .send_err("CROSSSLOT Keys in request don't hash to the same slot");
                    return;
                }
                _ => {}
            }
            i += 2;
        }
        if let Some(sid) = target {
            if sid == shard_id {
                let mut s = shard.borrow_mut();
                s.update_time(main_service::now_ms());
                registry.dispatch(args, conn_ctx, &mut s, reply_buf);
            } else {
                dispatch_remote(args, conn_ctx.db_index, sid, cross_txs, reply_buf).await;
            }
        }
        return;
    }

    // MSET: split pairs by shard
    let mut shard_pairs: HashMap<u32, Vec<(RespExpr, RespExpr)>> = HashMap::new();
    let mut i = 1;
    while i < args.len() {
        let key = args[i].as_bytes().unwrap_or(b"");
        let sid = sharding::shard(key, num_shards);
        shard_pairs.entry(sid).or_default().push((args[i].clone(), args[i + 1].clone()));
        i += 2;
    }

    // Local pairs
    if let Some(local_pairs) = shard_pairs.remove(&shard_id) {
        let mut local_args = vec![RespExpr::from_static(b"MSET")];
        for (k, v) in local_pairs {
            local_args.push(k);
            local_args.push(v);
        }
        let mut s = shard.borrow_mut();
        s.update_time(main_service::now_ms());
        let mut discard = BytesMut::new();
        registry.dispatch(&local_args, conn_ctx, &mut s, &mut discard);
    }

    // Remote pairs
    let mut futures = Vec::new();
    for (sid, pairs) in shard_pairs {
        let mut shard_args = vec![RespExpr::from_static(b"MSET")];
        for (k, v) in pairs {
            shard_args.push(k);
            shard_args.push(v);
        }
        let (tx, rx) = oneshot::channel();
        let task = CrossShardTask { args: shard_args, db_index: conn_ctx.db_index, reply_tx: tx };
        let sender = cross_txs[sid as usize].clone();
        futures.push(tokio::task::spawn_local(async move {
            let _ = sender.send(task).await;
            rx.await
        }));
    }
    for fut in futures {
        let _ = fut.await;
    }

    ReplyBuilder::new(reply_buf).send_ok();
}

/// Global commands: DBSIZE, FLUSHDB, FLUSHALL, KEYS, SCAN.
async fn dispatch_global(
    args: &[RespExpr],
    cmd_str: &str,
    conn_ctx: &mut ConnContext,
    reply_buf: &mut BytesMut,
    shard: &Rc<RefCell<EngineShard>>,
    shard_id: u32,
    num_shards: u32,
    registry: &CommandRegistry,
    cross_txs: &[mpsc::Sender<CrossShardTask>],
) {
    // Execute locally first
    let mut local_buf = BytesMut::with_capacity(128);
    {
        let mut s = shard.borrow_mut();
        s.update_time(main_service::now_ms());
        registry.dispatch(args, conn_ctx, &mut s, &mut local_buf);
    }

    // For single-shard setups, just return local result
    if num_shards == 1 {
        reply_buf.extend_from_slice(&local_buf);
        return;
    }

    // Fan out to all other shards
    let mut remote_futures = Vec::new();
    for sid in 0..num_shards {
        if sid == shard_id {
            continue;
        }
        let (tx, rx) = oneshot::channel();
        let task = CrossShardTask {
            args: args.to_vec(),
            db_index: conn_ctx.db_index,
            reply_tx: tx,
        };
        let sender = cross_txs[sid as usize].clone();
        remote_futures.push(tokio::task::spawn_local(async move {
            let _ = sender.send(task).await;
            rx.await
        }));
    }

    let mut remote_bufs = Vec::new();
    for fut in remote_futures {
        if let Ok(Ok(reply)) = fut.await {
            remote_bufs.push(reply.data);
        }
    }

    // Merge based on command type
    match cmd_str {
        "DBSIZE" => {
            let mut total = main_service::parse_resp_integer(&local_buf).unwrap_or(0);
            for buf in &remote_bufs {
                total += main_service::parse_resp_integer(buf).unwrap_or(0);
            }
            ReplyBuilder::new(reply_buf).send_integer(total);
        }
        "FLUSHDB" | "FLUSHALL" => {
            ReplyBuilder::new(reply_buf).send_ok();
        }
        "KEYS" => {
            let mut all_keys: Vec<Vec<u8>> = Vec::new();
            // Local keys
            for k in main_service::parse_resp_array(&local_buf).into_iter().flatten() {
                all_keys.push(k);
            }
            // Remote keys
            for buf in &remote_bufs {
                for k in main_service::parse_resp_array(buf).into_iter().flatten() {
                    all_keys.push(k);
                }
            }
            let mut rb = ReplyBuilder::new(reply_buf);
            rb.send_array_len(all_keys.len());
            for k in &all_keys {
                rb.send_bulk_string(k);
            }
        }
        "SCAN" => {
            // For now, only scan local shard
            reply_buf.extend_from_slice(&local_buf);
        }
        _ => {
            reply_buf.extend_from_slice(&local_buf);
        }
    }
}

fn handle_select(args: &[RespExpr], ctx: &mut ConnContext, buf: &mut BytesMut) -> bool {
    let mut rb = ReplyBuilder::new(buf);
    if args.len() != 2 {
        rb.send_err("wrong number of arguments for 'select' command");
        return true;
    }
    let idx = args[1]
        .as_bytes()
        .and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.parse::<u16>().ok());
    match idx {
        Some(n) if n < 16 => {
            ctx.db_index = n;
            rb.send_ok();
        }
        _ => rb.send_err("DB index is out of range"),
    }
    true
}

fn collect_snapshot(shard: &EngineShard) -> ShardSnapshotData {
    let mut entries = Vec::new();
    for db_idx in 0..16u16 {
        let db = shard.db_slice.db(db_idx);
        for (key, value) in db.prime.iter() {
            let expire_ms = db.expire.find(key).copied();
            entries.push((db_idx, key.as_bytes().to_vec(), value.clone(), expire_ms));
        }
    }
    ShardSnapshotData { entries }
}

fn load_entries(shard: &mut EngineShard, entries: Vec<LoadEntry>) {
    let now = main_service::now_ms();
    shard.update_time(now);
    let mut loaded = 0u64;
    for entry in entries {
        if let Some(expire_ms) = entry.expire_ms {
            if expire_ms <= now {
                continue;
            }
        }
        shard.db_slice.add_or_update(entry.db_index, &entry.key, entry.value);
        if let Some(expire_ms) = entry.expire_ms {
            shard.db_slice.add_expire(entry.db_index, &entry.key, expire_ms);
        }
        loaded += 1;
    }
    if loaded > 0 {
        tracing::debug!("Loaded {} keys", loaded);
    }
}
