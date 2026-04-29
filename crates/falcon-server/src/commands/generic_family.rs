use falcon_core::dash::cursor::Cursor;

use crate::command_registry::{CommandContext, CommandEntry, CommandHandler, CommandRegistry};
use crate::db_slice::TtlResult;

pub fn register(registry: &mut CommandRegistry) {
    let cmds: &[(&str, CommandHandler, i16, u16)] = &[
        ("PING", cmd_ping, -1, 0),
        ("ECHO", cmd_echo, 2, 0),
        ("COMMAND", cmd_command, -1, 0),
        ("INFO", cmd_info, -1, 0),
        ("SELECT", cmd_select, 2, 0),
        ("DBSIZE", cmd_dbsize, 1, 0),
        ("FLUSHDB", cmd_flushdb, -1, 0),
        ("FLUSHALL", cmd_flushall, -1, 0),
        ("DEL", cmd_del, -2, 1),
        ("UNLINK", cmd_del, -2, 1),
        ("EXISTS", cmd_exists, -2, 1),
        ("TYPE", cmd_type, 2, 1),
        ("RENAME", cmd_rename, 3, 1),
        ("RENAMENX", cmd_renamenx, 3, 1),
        ("EXPIRE", cmd_expire, 3, 1),
        ("PEXPIRE", cmd_pexpire, 3, 1),
        ("EXPIREAT", cmd_expireat, 3, 1),
        ("PEXPIREAT", cmd_pexpireat, 3, 1),
        ("TTL", cmd_ttl, 2, 1),
        ("PTTL", cmd_pttl, 2, 1),
        ("PERSIST", cmd_persist, 2, 1),
        ("KEYS", cmd_keys, 2, 0),
        ("SCAN", cmd_scan, -2, 0),
        ("RANDOMKEY", cmd_randomkey, 1, 0),
        ("HELLO", cmd_hello, -1, 0),
        ("CLIENT", cmd_client, -2, 0),
        ("CONFIG", cmd_config, -2, 0),
        ("RESET", cmd_reset, 1, 0),
    ];

    for &(name, handler, arity, first_key) in cmds {
        registry.register(CommandEntry {
            name,
            handler,
            arity,
            first_key,
            last_key: if first_key > 0 { 1 } else { 0 },
            key_step: 1,
        });
    }
}

fn arg(ctx: &CommandContext, idx: usize) -> Vec<u8> {
    ctx.args[idx].as_bytes().unwrap().to_vec()
}

fn arg_str(ctx: &CommandContext, idx: usize) -> Option<String> {
    ctx.args
        .get(idx)?
        .as_bytes()
        .and_then(|b| std::str::from_utf8(b).ok())
        .map(|s| s.to_string())
}

fn arg_int(ctx: &CommandContext, idx: usize) -> Option<i64> {
    arg_str(ctx, idx)?.trim().parse().ok()
}

fn cmd_ping(mut ctx: CommandContext<'_>) {
    if ctx.args.len() > 1 {
        let msg = arg(&ctx, 1);
        ctx.reply.send_bulk_string(&msg);
    } else {
        ctx.reply.send_pong();
    }
}

fn cmd_echo(mut ctx: CommandContext<'_>) {
    let msg = arg(&ctx, 1);
    ctx.reply.send_bulk_string(&msg);
}

fn cmd_command(mut ctx: CommandContext<'_>) {
    ctx.reply.send_array_len(0);
}

fn cmd_info(mut ctx: CommandContext<'_>) {
    let info = format!(
        "# Server\r\nfalcondb_version:0.1.0\r\n\
         # Keyspace\r\ndb0:keys={},expires=0\r\n",
        ctx.shard.db_slice.db_size(ctx.conn_ctx.db_index)
    );
    ctx.reply.send_bulk_string(info.as_bytes());
}

fn cmd_select(mut ctx: CommandContext<'_>) {
    let idx = match arg_int(&ctx, 1) {
        Some(n) if n >= 0 && n < 16 => n as u16,
        _ => {
            ctx.reply.send_err("DB index is out of range");
            return;
        }
    };
    ctx.conn_ctx.db_index = idx;
    ctx.reply.send_ok();
}

fn cmd_dbsize(mut ctx: CommandContext<'_>) {
    let size = ctx.shard.db_slice.db_size(ctx.conn_ctx.db_index);
    ctx.reply.send_integer(size as i64);
}

fn cmd_flushdb(mut ctx: CommandContext<'_>) {
    ctx.shard.db_slice.flush_db(ctx.conn_ctx.db_index);
    ctx.reply.send_ok();
}

fn cmd_flushall(mut ctx: CommandContext<'_>) {
    for i in 0..16u16 {
        ctx.shard.db_slice.flush_db(i);
    }
    ctx.reply.send_ok();
}

fn cmd_del(mut ctx: CommandContext<'_>) {
    let db = ctx.conn_ctx.db_index;
    let keys: Vec<Vec<u8>> = (1..ctx.args.len()).map(|i| arg(&ctx, i)).collect();
    let mut count = 0i64;
    for key in &keys {
        if ctx.shard.db_slice.del(db, key) {
            count += 1;
        }
    }
    ctx.reply.send_integer(count);
}

fn cmd_exists(mut ctx: CommandContext<'_>) {
    let db = ctx.conn_ctx.db_index;
    let keys: Vec<Vec<u8>> = (1..ctx.args.len()).map(|i| arg(&ctx, i)).collect();
    let mut count = 0i64;
    for key in &keys {
        if ctx.shard.db_slice.exists(db, key) {
            count += 1;
        }
    }
    ctx.reply.send_integer(count);
}

fn cmd_type(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find(db, &key) {
        Some(r) => ctx.reply.send_simple_string(r.value.type_name()),
        None => ctx.reply.send_simple_string("none"),
    }
}

fn cmd_rename(mut ctx: CommandContext<'_>) {
    let from = arg(&ctx, 1);
    let to = arg(&ctx, 2);
    let db = ctx.conn_ctx.db_index;
    if ctx.shard.db_slice.rename(db, &from, &to) {
        ctx.reply.send_ok();
    } else {
        ctx.reply.send_err("no such key");
    }
}

fn cmd_renamenx(mut ctx: CommandContext<'_>) {
    let from = arg(&ctx, 1);
    let to = arg(&ctx, 2);
    let db = ctx.conn_ctx.db_index;

    if !ctx.shard.db_slice.exists(db, &from) {
        ctx.reply.send_err("no such key");
        return;
    }
    if ctx.shard.db_slice.exists(db, &to) {
        ctx.reply.send_integer(0);
        return;
    }
    ctx.shard.db_slice.rename(db, &from, &to);
    ctx.reply.send_integer(1);
}

fn cmd_expire(mut ctx: CommandContext<'_>) {
    set_expire_relative(&mut ctx, 1000);
}

fn cmd_pexpire(mut ctx: CommandContext<'_>) {
    set_expire_relative(&mut ctx, 1);
}

fn cmd_expireat(mut ctx: CommandContext<'_>) {
    set_expire_absolute(&mut ctx, 1000);
}

fn cmd_pexpireat(mut ctx: CommandContext<'_>) {
    set_expire_absolute(&mut ctx, 1);
}

fn set_expire_relative(ctx: &mut CommandContext<'_>, multiplier: u64) {
    let key = arg(ctx, 1);
    let secs = match arg_int(ctx, 2) {
        Some(n) => n,
        None => {
            ctx.reply
                .send_err("value is not an integer or out of range");
            return;
        }
    };
    let db = ctx.conn_ctx.db_index;
    if secs <= 0 {
        if ctx.shard.db_slice.exists(db, &key) {
            ctx.shard.db_slice.del(db, &key);
            ctx.reply.send_integer(1);
        } else {
            ctx.reply.send_integer(0);
        }
        return;
    }
    if !ctx.shard.db_slice.exists(db, &key) {
        ctx.reply.send_integer(0);
        return;
    }
    let deadline = ctx.shard.db_slice.now_ms() + (secs as u64) * multiplier;
    ctx.shard.db_slice.add_expire(db, &key, deadline);
    ctx.reply.send_integer(1);
}

fn set_expire_absolute(ctx: &mut CommandContext<'_>, multiplier: u64) {
    let key = arg(ctx, 1);
    let ts = match arg_int(ctx, 2) {
        Some(n) => n,
        None => {
            ctx.reply
                .send_err("value is not an integer or out of range");
            return;
        }
    };
    let db = ctx.conn_ctx.db_index;
    if !ctx.shard.db_slice.exists(db, &key) {
        ctx.reply.send_integer(0);
        return;
    }
    let deadline_ms = (ts as u64) * multiplier;
    if deadline_ms <= ctx.shard.db_slice.now_ms() {
        ctx.shard.db_slice.del(db, &key);
        ctx.reply.send_integer(1);
        return;
    }
    ctx.shard.db_slice.add_expire(db, &key, deadline_ms);
    ctx.reply.send_integer(1);
}

fn cmd_ttl(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.ttl_ms(db, &key) {
        TtlResult::KeyNotFound => ctx.reply.send_integer(-2),
        TtlResult::NoExpiry => ctx.reply.send_integer(-1),
        TtlResult::Expires(ms) => ctx.reply.send_integer((ms / 1000) as i64),
    }
}

fn cmd_pttl(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.ttl_ms(db, &key) {
        TtlResult::KeyNotFound => ctx.reply.send_integer(-2),
        TtlResult::NoExpiry => ctx.reply.send_integer(-1),
        TtlResult::Expires(ms) => ctx.reply.send_integer(ms as i64),
    }
}

fn cmd_persist(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;
    if !ctx.shard.db_slice.exists(db, &key) {
        ctx.reply.send_integer(0);
        return;
    }
    let removed = ctx.shard.db_slice.remove_expire(db, &key);
    ctx.reply.send_integer(if removed { 1 } else { 0 });
}

fn cmd_keys(mut ctx: CommandContext<'_>) {
    let pattern = arg_str(&ctx, 1).unwrap_or_else(|| "*".to_string());
    let db = ctx.conn_ctx.db_index;
    let keys = ctx.shard.db_slice.keys(db, &pattern);
    ctx.reply.send_array_len(keys.len());
    for k in &keys {
        ctx.reply.send_bulk_string(k);
    }
}

fn cmd_scan(mut ctx: CommandContext<'_>) {
    let cursor_val = match arg_str(&ctx, 1)
        .and_then(|s| s.parse::<u64>().ok())
    {
        Some(n) => n,
        None => {
            ctx.reply.send_err("invalid cursor");
            return;
        }
    };

    let mut count = 10usize;
    let mut pattern: Option<String> = None;

    let num_args = ctx.args.len();
    let mut i = 2;
    while i < num_args {
        let opt = arg_str(&ctx, i).unwrap_or_default().to_uppercase();
        match opt.as_str() {
            "COUNT" => {
                i += 1;
                if let Some(n) = arg_str(&ctx, i).and_then(|s| s.parse().ok()) {
                    count = n;
                }
            }
            "MATCH" => {
                i += 1;
                pattern = arg_str(&ctx, i);
            }
            _ => {}
        }
        i += 1;
    }

    let db = ctx.conn_ctx.db_index;
    let cursor = Cursor::new(cursor_val);

    let (keys, next_cursor) = ctx
        .shard
        .db_slice
        .scan(db, cursor, count, pattern.as_deref());

    ctx.reply.send_array_len(2);
    ctx.reply
        .send_bulk_string(next_cursor.value().to_string().as_bytes());
    ctx.reply.send_array_len(keys.len());
    for k in &keys {
        ctx.reply.send_bulk_string(k);
    }
}

fn cmd_randomkey(mut ctx: CommandContext<'_>) {
    let db = ctx.conn_ctx.db_index;
    let table = &ctx.shard.db_slice.db(db).prime;
    match table.random_entry() {
        Some((k, _)) => ctx.reply.send_bulk_string(k.as_bytes()),
        None => ctx.reply.send_null(),
    }
}

/// HELLO [protover [AUTH username password] [SETNAME clientname]]
/// Returns server info. We only support RESP2 (protocol 2).
fn cmd_hello(mut ctx: CommandContext<'_>) {
    // Check if client requests RESP3 (protocol 3) — we don't support it
    if ctx.args.len() > 1 {
        let ver = arg_int(&ctx, 1).unwrap_or(2);
        if ver == 3 {
            // Return RESP2 response that tells the client we only support proto 2
            // Some clients accept this gracefully and fall back to RESP2
        }
    }
    // Return a flat array of key-value pairs (RESP2 compatible map)
    ctx.reply.send_array_len(14);
    ctx.reply.send_bulk_string(b"server");
    ctx.reply.send_bulk_string(b"falcondb");
    ctx.reply.send_bulk_string(b"version");
    ctx.reply.send_bulk_string(b"0.1.0");
    ctx.reply.send_bulk_string(b"proto");
    ctx.reply.send_integer(2);
    ctx.reply.send_bulk_string(b"id");
    ctx.reply.send_integer(1);
    ctx.reply.send_bulk_string(b"mode");
    ctx.reply.send_bulk_string(b"standalone");
    ctx.reply.send_bulk_string(b"role");
    ctx.reply.send_bulk_string(b"master");
    ctx.reply.send_bulk_string(b"modules");
    ctx.reply.send_array_len(0);
}

/// CLIENT subcommand - minimal support for CLIENT SETNAME, CLIENT GETNAME, CLIENT INFO
fn cmd_client(mut ctx: CommandContext<'_>) {
    let sub = arg_str(&ctx, 1).unwrap_or_default().to_uppercase();
    match sub.as_str() {
        "SETNAME" => ctx.reply.send_ok(),
        "GETNAME" => ctx.reply.send_null(),
        "INFO" => ctx.reply.send_bulk_string(b"id=1 fd=0 name= db=0\r\n"),
        "ID" => ctx.reply.send_integer(1),
        _ => ctx.reply.send_ok(),
    }
}

/// CONFIG GET/SET - minimal stub
fn cmd_config(mut ctx: CommandContext<'_>) {
    let sub = arg_str(&ctx, 1).unwrap_or_default().to_uppercase();
    match sub.as_str() {
        "GET" => {
            // Return empty array for any config key
            ctx.reply.send_array_len(0);
        }
        "SET" => ctx.reply.send_ok(),
        "RESETSTAT" => ctx.reply.send_ok(),
        _ => ctx.reply.send_err("unsupported CONFIG subcommand"),
    }
}

/// RESET - reset connection state
fn cmd_reset(mut ctx: CommandContext<'_>) {
    ctx.conn_ctx.db_index = 0;
    ctx.reply.send_simple_string("RESET");
}
