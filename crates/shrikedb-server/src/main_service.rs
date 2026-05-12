use std::collections::HashMap;
use std::sync::Arc;

use bytes::BytesMut;
use shrikedb_facade::reply_builder::ReplyBuilder;
use shrikedb_facade::resp_parser::RespExpr;

use crate::command_registry::{CommandEntry, CommandRegistry};
use crate::commands::{generic_family, hash_family, list_family, set_family, string_family, zset_family};
use crate::engine_shard_set::EngineShardSet;
use crate::sharding;

/// Build the command registry with all supported commands.
pub fn build_command_registry() -> CommandRegistry {
    let mut registry = CommandRegistry::new();
    generic_family::register(&mut registry);
    string_family::register(&mut registry);
    list_family::register(&mut registry);
    set_family::register(&mut registry);
    hash_family::register(&mut registry);
    zset_family::register(&mut registry);
    registry
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// The main service routes commands to the appropriate shard(s).
pub struct MainService {
    pub shard_set: Arc<EngineShardSet>,
    pub registry: Arc<CommandRegistry>,
}

/// Result of dispatching a command through the MainService.
pub struct DispatchResult {
    pub data: BytesMut,
    pub keep_alive: bool,
}

impl MainService {
    pub fn new(shard_set: Arc<EngineShardSet>, registry: Arc<CommandRegistry>) -> Self {
        Self {
            shard_set,
            registry,
        }
    }

    /// Dispatch a parsed command to the appropriate shard(s).
    pub async fn dispatch(
        &self,
        args: &[RespExpr],
        db_index: u16,
    ) -> DispatchResult {
        if args.is_empty() {
            let mut buf = BytesMut::new();
            ReplyBuilder::new(&mut buf).send_err("empty command");
            return DispatchResult {
                data: buf,
                keep_alive: true,
            };
        }

        let cmd_name = match args[0].as_bytes() {
            Some(b) => b.to_ascii_uppercase(),
            None => {
                let mut buf = BytesMut::new();
                ReplyBuilder::new(&mut buf).send_err("invalid command");
                return DispatchResult {
                    data: buf,
                    keep_alive: true,
                };
            }
        };
        let cmd_str = match std::str::from_utf8(&cmd_name) {
            Ok(s) => s,
            Err(_) => {
                let mut buf = BytesMut::new();
                ReplyBuilder::new(&mut buf).send_err("invalid command encoding");
                return DispatchResult {
                    data: buf,
                    keep_alive: true,
                };
            }
        };

        // Handle QUIT locally
        if cmd_str == "QUIT" {
            let mut buf = BytesMut::new();
            ReplyBuilder::new(&mut buf).send_ok();
            return DispatchResult {
                data: buf,
                keep_alive: false,
            };
        }

        // Handle SAVE/BGSAVE at the service level (needs all shards)
        if cmd_str == "SAVE" || cmd_str == "BGSAVE" {
            return self.handle_save(cmd_str).await;
        }

        // Look up the command to understand its key structure
        let entry = match self.registry.find(cmd_str) {
            Some(e) => e,
            None => {
                let mut buf = BytesMut::new();
                ReplyBuilder::new(&mut buf).send_err(&format!(
                    "unknown command '{}', with args beginning with: ",
                    cmd_str.to_lowercase()
                ));
                return DispatchResult {
                    data: buf,
                    keep_alive: true,
                };
            }
        };

        // Check arity
        let argc = args.len() as i16;
        if entry.arity > 0 && argc != entry.arity {
            let mut buf = BytesMut::new();
            ReplyBuilder::new(&mut buf).send_err(&format!(
                "wrong number of arguments for '{}' command",
                cmd_str.to_lowercase()
            ));
            return DispatchResult {
                data: buf,
                keep_alive: true,
            };
        }
        if entry.arity < 0 && argc < -entry.arity {
            let mut buf = BytesMut::new();
            ReplyBuilder::new(&mut buf).send_err(&format!(
                "wrong number of arguments for '{}' command",
                cmd_str.to_lowercase()
            ));
            return DispatchResult {
                data: buf,
                keep_alive: true,
            };
        }

        let num_shards = self.shard_set.num_shards();

        // Determine routing strategy
        match classify_command(cmd_str, entry) {
            CommandType::NoKeys => {
                // Commands with no keys: PING, ECHO, COMMAND, SELECT, INFO
                // Send to shard 0 (arbitrary)
                self.dispatch_to_shard(0, args, db_index).await
            }
            CommandType::SingleKey => {
                // Commands with exactly one key: GET, SET, INCR, TTL, EXPIRE, etc.
                let key = args[entry.first_key as usize].as_bytes().unwrap_or(b"");
                let shard_id = sharding::shard(key, num_shards);
                self.dispatch_to_shard(shard_id, args, db_index).await
            }
            CommandType::MultiKeySameHandler => {
                // DEL, EXISTS, UNLINK: multiple keys, sum integer results
                self.dispatch_multi_key_sum(args, entry, db_index).await
            }
            CommandType::MGet => {
                self.dispatch_mget(args, db_index).await
            }
            CommandType::MSet => {
                self.dispatch_mset(args, cmd_str, db_index).await
            }
            CommandType::Rename => {
                // Both keys must be extracted
                let from = args[1].as_bytes().unwrap_or(b"");
                let to = args[2].as_bytes().unwrap_or(b"");
                let s1 = sharding::shard(from, num_shards);
                let s2 = sharding::shard(to, num_shards);
                if s1 != s2 {
                    let mut buf = BytesMut::new();
                    ReplyBuilder::new(&mut buf)
                        .send_err("CROSSSLOT Keys in request don't hash to the same slot");
                    DispatchResult {
                        data: buf,
                        keep_alive: true,
                    }
                } else {
                    self.dispatch_to_shard(s1, args, db_index).await
                }
            }
            CommandType::Global => {
                // FLUSHDB, FLUSHALL, DBSIZE, KEYS, SCAN
                self.dispatch_global(args, cmd_str, db_index).await
            }
        }
    }

    async fn handle_save(&self, cmd: &str) -> DispatchResult {
        let is_bg = cmd == "BGSAVE";
        let shard_set = Arc::clone(&self.shard_set);

        let do_save = async move {
            let snapshots = match shard_set.collect_snapshots().await {
                Ok(s) => s,
                Err(_) => {
                    return Err("failed to collect snapshots".to_string());
                }
            };

            // Convert to persistence format
            let persist_snapshots: Vec<shrikedb_persistence::snapshot::ShardSnapshot> = snapshots
                .into_iter()
                .map(|s| shrikedb_persistence::snapshot::ShardSnapshot {
                    entries: s.entries,
                })
                .collect();

            let path = shrikedb_persistence::snapshot::default_rdb_path();
            match shrikedb_persistence::snapshot::save_rdb(&path, &persist_snapshots) {
                Ok(bytes) => {
                    tracing::info!("RDB saved: {} bytes to {}", bytes, path.display());
                    Ok(())
                }
                Err(e) => Err(format!("RDB save failed: {}", e)),
            }
        };

        if is_bg {
            // BGSAVE: spawn in background, return immediately
            tokio::spawn(async move {
                if let Err(e) = do_save.await {
                    tracing::error!("{}", e);
                }
            });
            let mut buf = BytesMut::new();
            ReplyBuilder::new(&mut buf).send_simple_string("Background saving started");
            DispatchResult {
                data: buf,
                keep_alive: true,
            }
        } else {
            // SAVE: synchronous, block until done
            match do_save.await {
                Ok(()) => {
                    let mut buf = BytesMut::new();
                    ReplyBuilder::new(&mut buf).send_ok();
                    DispatchResult {
                        data: buf,
                        keep_alive: true,
                    }
                }
                Err(e) => {
                    let mut buf = BytesMut::new();
                    ReplyBuilder::new(&mut buf).send_err(&e);
                    DispatchResult {
                        data: buf,
                        keep_alive: true,
                    }
                }
            }
        }
    }

    async fn dispatch_to_shard(
        &self,
        shard_id: u32,
        args: &[RespExpr],
        db_index: u16,
    ) -> DispatchResult {
        let owned_args = own_args(args);
        match self.shard_set.shard(shard_id).dispatch(owned_args, db_index).await {
            Ok(reply) => DispatchResult {
                data: reply.data,
                keep_alive: reply.keep_alive,
            },
            Err(_) => {
                let mut buf = BytesMut::new();
                ReplyBuilder::new(&mut buf).send_err("shard unavailable");
                DispatchResult {
                    data: buf,
                    keep_alive: true,
                }
            }
        }
    }

    /// DEL, EXISTS, UNLINK: split keys by shard, execute on each, sum results.
    async fn dispatch_multi_key_sum(
        &self,
        args: &[RespExpr],
        _entry: &CommandEntry,
        db_index: u16,
    ) -> DispatchResult {
        let num_shards = self.shard_set.num_shards();
        let cmd = args[0].clone();

        // Group keys by shard
        let mut shard_keys: HashMap<u32, Vec<RespExpr>> = HashMap::new();
        for i in 1..args.len() {
            let key_bytes = args[i].as_bytes().unwrap_or(b"");
            let sid = sharding::shard(key_bytes, num_shards);
            shard_keys
                .entry(sid)
                .or_default()
                .push(args[i].clone());
        }

        // Dispatch to each shard in parallel
        let mut futures = Vec::new();
        for (sid, keys) in shard_keys {
            let mut shard_args = vec![cmd.clone()];
            shard_args.extend(keys);
            let handle = self.shard_set.shard(sid).clone();
            futures.push(tokio::spawn(async move {
                handle.dispatch(shard_args, db_index).await
            }));
        }

        // Collect results and sum
        let mut total: i64 = 0;
        for fut in futures {
            if let Ok(Ok(reply)) = fut.await {
                // Parse the integer from the RESP reply
                total += parse_resp_integer(&reply.data).unwrap_or(0);
            }
        }

        let mut buf = BytesMut::new();
        ReplyBuilder::new(&mut buf).send_integer(total);
        DispatchResult {
            data: buf,
            keep_alive: true,
        }
    }

    /// MGET: split keys by shard, execute partial MGETs, merge in original order.
    async fn dispatch_mget(
        &self,
        args: &[RespExpr],
        db_index: u16,
    ) -> DispatchResult {
        let num_shards = self.shard_set.num_shards();
        let num_keys = args.len() - 1;

        // Map each key to its shard and track original position
        let mut shard_key_positions: HashMap<u32, Vec<(usize, RespExpr)>> = HashMap::new();
        for i in 0..num_keys {
            let key_bytes = args[i + 1].as_bytes().unwrap_or(b"");
            let sid = sharding::shard(key_bytes, num_shards);
            shard_key_positions
                .entry(sid)
                .or_default()
                .push((i, args[i + 1].clone()));
        }

        // Dispatch to each shard
        let mut futures = Vec::new();
        for (sid, key_positions) in &shard_key_positions {
            let mut shard_args = vec![RespExpr::from_static(b"MGET")];
            let positions: Vec<usize> = key_positions.iter().map(|(pos, _)| *pos).collect();
            for (_, key) in key_positions {
                shard_args.push(key.clone());
            }
            let handle = self.shard_set.shard(*sid).clone();
            futures.push((*sid, positions, tokio::spawn(async move {
                handle.dispatch(shard_args, db_index).await
            })));
        }

        // Merge results in original order
        let mut results: Vec<Option<Vec<u8>>> = vec![None; num_keys];
        for (_sid, positions, fut) in futures {
            if let Ok(Ok(reply)) = fut.await {
                let values = parse_resp_array(&reply.data);
                for (idx, pos) in positions.iter().enumerate() {
                    if idx < values.len() {
                        results[*pos] = values[idx].clone();
                    }
                }
            }
        }

        // Build merged RESP response
        let mut buf = BytesMut::new();
        let mut rb = ReplyBuilder::new(&mut buf);
        rb.send_array_len(num_keys);
        for val in &results {
            match val {
                Some(v) => rb.send_bulk_string(v),
                None => rb.send_null(),
            }
        }
        DispatchResult {
            data: buf,
            keep_alive: true,
        }
    }

    /// MSET/MSETNX: split key-value pairs by shard.
    async fn dispatch_mset(
        &self,
        args: &[RespExpr],
        cmd_str: &str,
        db_index: u16,
    ) -> DispatchResult {
        let num_shards = self.shard_set.num_shards();

        if (args.len() - 1) % 2 != 0 {
            let mut buf = BytesMut::new();
            ReplyBuilder::new(&mut buf).send_err(&format!(
                "wrong number of arguments for '{}' command",
                cmd_str.to_lowercase()
            ));
            return DispatchResult {
                data: buf,
                keep_alive: true,
            };
        }

        // For MSETNX, we need all-or-nothing semantics. In multi-shard mode,
        // this requires 2-phase commit which we don't have yet. For now, if all
        // keys are on the same shard, dispatch directly; otherwise error.
        if cmd_str == "MSETNX" {
            let mut target_shard: Option<u32> = None;
            let mut all_same = true;
            let mut i = 1;
            while i < args.len() {
                let key = args[i].as_bytes().unwrap_or(b"");
                let sid = sharding::shard(key, num_shards);
                match target_shard {
                    None => target_shard = Some(sid),
                    Some(s) if s != sid => {
                        all_same = false;
                        break;
                    }
                    _ => {}
                }
                i += 2;
            }
            if all_same {
                if let Some(sid) = target_shard {
                    return self.dispatch_to_shard(sid, args, db_index).await;
                }
            }
            let mut buf = BytesMut::new();
            ReplyBuilder::new(&mut buf)
                .send_err("CROSSSLOT Keys in request don't hash to the same slot");
            return DispatchResult {
                data: buf,
                keep_alive: true,
            };
        }

        // MSET: split pairs by shard
        let mut shard_pairs: HashMap<u32, Vec<(RespExpr, RespExpr)>> = HashMap::new();
        let mut i = 1;
        while i < args.len() {
            let key_bytes = args[i].as_bytes().unwrap_or(b"");
            let sid = sharding::shard(key_bytes, num_shards);
            shard_pairs
                .entry(sid)
                .or_default()
                .push((args[i].clone(), args[i + 1].clone()));
            i += 2;
        }

        // Dispatch to each shard in parallel
        let mut futures = Vec::new();
        for (sid, pairs) in shard_pairs {
            let mut shard_args = vec![RespExpr::from_static(b"MSET")];
            for (k, v) in pairs {
                shard_args.push(k);
                shard_args.push(v);
            }
            let handle = self.shard_set.shard(sid).clone();
            futures.push(tokio::spawn(async move {
                handle.dispatch(shard_args, db_index).await
            }));
        }

        // Wait for all
        for fut in futures {
            let _ = fut.await;
        }

        let mut buf = BytesMut::new();
        ReplyBuilder::new(&mut buf).send_ok();
        DispatchResult {
            data: buf,
            keep_alive: true,
        }
    }

    /// Global commands: broadcast to all shards and merge results.
    async fn dispatch_global(
        &self,
        args: &[RespExpr],
        cmd_str: &str,
        db_index: u16,
    ) -> DispatchResult {
        match cmd_str {
            "DBSIZE" => {
                // Sum sizes from all shards
                let mut futures = Vec::new();
                for handle in self.shard_set.all_shards() {
                    let h = handle.clone();
                    let a = own_args(args);
                    futures.push(tokio::spawn(
                        async move { h.dispatch(a, db_index).await },
                    ));
                }
                let mut total: i64 = 0;
                for fut in futures {
                    if let Ok(Ok(reply)) = fut.await {
                        total += parse_resp_integer(&reply.data).unwrap_or(0);
                    }
                }
                let mut buf = BytesMut::new();
                ReplyBuilder::new(&mut buf).send_integer(total);
                DispatchResult {
                    data: buf,
                    keep_alive: true,
                }
            }
            "FLUSHDB" | "FLUSHALL" => {
                // Broadcast to all shards
                let mut futures = Vec::new();
                for handle in self.shard_set.all_shards() {
                    let h = handle.clone();
                    let a = own_args(args);
                    futures.push(tokio::spawn(
                        async move { h.dispatch(a, db_index).await },
                    ));
                }
                for fut in futures {
                    let _ = fut.await;
                }
                let mut buf = BytesMut::new();
                ReplyBuilder::new(&mut buf).send_ok();
                DispatchResult {
                    data: buf,
                    keep_alive: true,
                }
            }
            "KEYS" => {
                // Broadcast to all shards, merge key lists
                let mut futures = Vec::new();
                for handle in self.shard_set.all_shards() {
                    let h = handle.clone();
                    let a = own_args(args);
                    futures.push(tokio::spawn(
                        async move { h.dispatch(a, db_index).await },
                    ));
                }
                let mut all_keys: Vec<Vec<u8>> = Vec::new();
                for fut in futures {
                    if let Ok(Ok(reply)) = fut.await {
                        let keys = parse_resp_array(&reply.data);
                        for k in keys.into_iter().flatten() {
                            all_keys.push(k);
                        }
                    }
                }
                let mut buf = BytesMut::new();
                let mut rb = ReplyBuilder::new(&mut buf);
                rb.send_array_len(all_keys.len());
                for k in &all_keys {
                    rb.send_bulk_string(k);
                }
                DispatchResult {
                    data: buf,
                    keep_alive: true,
                }
            }
            "SCAN" => {
                // For simplicity in multi-shard mode, delegate SCAN to shard 0.
                // A proper implementation would encode shard_id in the cursor.
                self.dispatch_to_shard(0, args, db_index).await
            }
            _ => {
                // Unknown global command - just send to shard 0
                self.dispatch_to_shard(0, args, db_index).await
            }
        }
    }
}

pub fn classify_command(cmd: &str, entry: &CommandEntry) -> CommandType {
    // Global commands (no keys, affect all shards)
    match cmd {
        "DBSIZE" | "FLUSHDB" | "FLUSHALL" | "KEYS" | "SCAN" => return CommandType::Global,
        _ => {}
    }

    // No-key commands
    if entry.first_key == 0 {
        return CommandType::NoKeys;
    }

    // Multi-key commands with special handling
    match cmd {
        "DEL" | "UNLINK" | "EXISTS" => return CommandType::MultiKeySameHandler,
        "MGET" => return CommandType::MGet,
        "MSET" | "MSETNX" => return CommandType::MSet,
        "RENAME" | "RENAMENX" => return CommandType::Rename,
        _ => {}
    }

    CommandType::SingleKey
}

pub enum CommandType {
    NoKeys,
    SingleKey,
    MultiKeySameHandler,
    MGet,
    MSet,
    Rename,
    Global,
}

/// Clone args into owned RespExprs.
fn own_args(args: &[RespExpr]) -> Vec<RespExpr> {
    args.to_vec()
}

/// Parse a RESP integer reply (`:N\r\n`) from raw bytes.
pub fn parse_resp_integer(data: &[u8]) -> Option<i64> {
    if data.is_empty() || data[0] != b':' {
        return None;
    }
    let end = data.iter().position(|&b| b == b'\r')?;
    std::str::from_utf8(&data[1..end]).ok()?.parse().ok()
}

/// Parse a RESP array of bulk strings from raw bytes.
/// Returns Vec<Option<Vec<u8>>> where None represents null bulk strings.
pub fn parse_resp_array(data: &[u8]) -> Vec<Option<Vec<u8>>> {
    let mut results = Vec::new();
    if data.is_empty() || data[0] != b'*' {
        return results;
    }

    let mut pos = 0;
    // Skip array length line
    while pos < data.len() && data[pos] != b'\n' {
        pos += 1;
    }
    pos += 1;

    // Parse each element
    while pos < data.len() {
        if data[pos] == b'$' {
            // Bulk string
            let len_start = pos + 1;
            let mut len_end = len_start;
            while len_end < data.len() && data[len_end] != b'\r' {
                len_end += 1;
            }
            let len_str = std::str::from_utf8(&data[len_start..len_end]).unwrap_or("-1");
            let len: i64 = len_str.parse().unwrap_or(-1);
            pos = len_end + 2; // skip \r\n

            if len < 0 {
                results.push(None);
            } else {
                let len = len as usize;
                if pos + len <= data.len() {
                    results.push(Some(data[pos..pos + len].to_vec()));
                    pos += len + 2; // skip data + \r\n
                } else {
                    break;
                }
            }
        } else {
            // Skip unknown type
            while pos < data.len() && data[pos] != b'\n' {
                pos += 1;
            }
            pos += 1;
        }
    }

    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_resp_integer() {
        assert_eq!(parse_resp_integer(b":42\r\n"), Some(42));
        assert_eq!(parse_resp_integer(b":0\r\n"), Some(0));
        assert_eq!(parse_resp_integer(b":-1\r\n"), Some(-1));
        assert_eq!(parse_resp_integer(b"+OK\r\n"), None);
    }

    #[test]
    fn test_parse_resp_array() {
        let data = b"*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n";
        let result = parse_resp_array(data);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], Some(b"foo".to_vec()));
        assert_eq!(result[1], None);
        assert_eq!(result[2], Some(b"bar".to_vec()));
    }
}
