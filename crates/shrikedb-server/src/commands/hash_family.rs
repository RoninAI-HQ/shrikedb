use std::collections::HashMap;

use shrikedb_core::compact_obj::PrimeValue;

use crate::command_registry::{CommandContext, CommandEntry, CommandHandler, CommandRegistry};

pub fn register(registry: &mut CommandRegistry) {
    let cmds: &[(&str, CommandHandler, i16)] = &[
        ("HSET", cmd_hset, -4),
        ("HGET", cmd_hget, 3),
        ("HDEL", cmd_hdel, -3),
        ("HEXISTS", cmd_hexists, 3),
        ("HGETALL", cmd_hgetall, 2),
        ("HKEYS", cmd_hkeys, 2),
        ("HVALS", cmd_hvals, 2),
        ("HLEN", cmd_hlen, 2),
        ("HMSET", cmd_hmset, -4),
        ("HMGET", cmd_hmget, -3),
        ("HINCRBY", cmd_hincrby, 4),
        ("HINCRBYFLOAT", cmd_hincrbyfloat, 4),
        ("HSETNX", cmd_hsetnx, 4),
    ];
    for &(name, handler, arity) in cmds {
        registry.register(CommandEntry {
            name, handler, arity,
            first_key: 1, last_key: 1, key_step: 1,
        });
    }
}

fn arg(ctx: &CommandContext, i: usize) -> Vec<u8> {
    ctx.args[i].as_bytes().unwrap().to_vec()
}

fn arg_str(ctx: &CommandContext, i: usize) -> Option<String> {
    ctx.args.get(i)?.as_bytes().and_then(|b| std::str::from_utf8(b).ok()).map(|s| s.to_string())
}

fn cmd_hset(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;
    if (ctx.args.len() - 2) % 2 != 0 {
        ctx.reply.send_err("wrong number of arguments for 'hset' command");
        return;
    }
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..(ctx.args.len() - 2) / 2)
        .map(|j| (arg(&ctx, 2 + j * 2), arg(&ctx, 3 + j * 2)))
        .collect();

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::Hash(_)) => {
            if let PrimeValue::Hash(ref mut map) = r.value {
                let mut added = 0i64;
                for (f, v) in pairs {
                    if map.insert(f, v).is_none() { added += 1; }
                }
                ctx.reply.send_integer(added);
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => {
            let mut map = HashMap::new();
            let mut added = 0i64;
            for (f, v) in pairs {
                if map.insert(f, v).is_none() { added += 1; }
            }
            ctx.shard.db_slice.add_or_update(db, &key, PrimeValue::Hash(map));
            ctx.reply.send_integer(added);
        }
    }
}

fn cmd_hget(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let field = arg(&ctx, 2);
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::Hash(_)) => {
            if let PrimeValue::Hash(ref map) = r.value {
                match map.get(&field) {
                    Some(v) => ctx.reply.send_bulk_string(v),
                    None => ctx.reply.send_null(),
                }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_null(),
    }
}

fn cmd_hdel(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let fields: Vec<Vec<u8>> = (2..ctx.args.len()).map(|i| arg(&ctx, i)).collect();
    let db = ctx.conn_ctx.db_index;

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::Hash(_)) => {
            if let PrimeValue::Hash(ref mut map) = r.value {
                let mut removed = 0i64;
                for f in &fields { if map.remove(f).is_some() { removed += 1; } }
                if map.is_empty() { ctx.shard.db_slice.del(db, &key); }
                ctx.reply.send_integer(removed);
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_integer(0),
    }
}

fn cmd_hexists(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let field = arg(&ctx, 2);
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::Hash(_)) => {
            if let PrimeValue::Hash(ref map) = r.value {
                ctx.reply.send_integer(if map.contains_key(&field) { 1 } else { 0 });
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_integer(0),
    }
}

fn cmd_hgetall(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::Hash(_)) => {
            if let PrimeValue::Hash(ref map) = r.value {
                ctx.reply.send_array_len(map.len() * 2);
                for (f, v) in map {
                    ctx.reply.send_bulk_string(f);
                    ctx.reply.send_bulk_string(v);
                }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_array_len(0),
    }
}

fn cmd_hkeys(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::Hash(_)) => {
            if let PrimeValue::Hash(ref map) = r.value {
                ctx.reply.send_array_len(map.len());
                for f in map.keys() { ctx.reply.send_bulk_string(f); }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_array_len(0),
    }
}

fn cmd_hvals(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::Hash(_)) => {
            if let PrimeValue::Hash(ref map) = r.value {
                ctx.reply.send_array_len(map.len());
                for v in map.values() { ctx.reply.send_bulk_string(v); }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_array_len(0),
    }
}

fn cmd_hlen(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::Hash(_)) => {
            if let PrimeValue::Hash(ref map) = r.value {
                ctx.reply.send_integer(map.len() as i64);
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_integer(0),
    }
}

fn cmd_hmset(mut ctx: CommandContext<'_>) {
    // HMSET is identical to HSET but returns OK instead of count
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;
    if (ctx.args.len() - 2) % 2 != 0 {
        ctx.reply.send_err("wrong number of arguments for 'hmset' command");
        return;
    }
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..(ctx.args.len() - 2) / 2)
        .map(|j| (arg(&ctx, 2 + j * 2), arg(&ctx, 3 + j * 2)))
        .collect();

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::Hash(_)) => {
            if let PrimeValue::Hash(ref mut map) = r.value {
                for (f, v) in pairs { map.insert(f, v); }
            }
            ctx.reply.send_ok();
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => {
            let mut map = HashMap::new();
            for (f, v) in pairs { map.insert(f, v); }
            ctx.shard.db_slice.add_or_update(db, &key, PrimeValue::Hash(map));
            ctx.reply.send_ok();
        }
    }
}

fn cmd_hmget(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let fields: Vec<Vec<u8>> = (2..ctx.args.len()).map(|i| arg(&ctx, i)).collect();
    let db = ctx.conn_ctx.db_index;

    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::Hash(_)) => {
            if let PrimeValue::Hash(ref map) = r.value {
                ctx.reply.send_array_len(fields.len());
                for f in &fields {
                    match map.get(f) {
                        Some(v) => ctx.reply.send_bulk_string(v),
                        None => ctx.reply.send_null(),
                    }
                }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => {
            ctx.reply.send_array_len(fields.len());
            for _ in &fields { ctx.reply.send_null(); }
        }
    }
}

fn cmd_hincrby(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let field = arg(&ctx, 2);
    let delta = match arg_str(&ctx, 3).and_then(|s| s.parse::<i64>().ok()) {
        Some(n) => n,
        None => { ctx.reply.send_err("value is not an integer or out of range"); return; }
    };
    let db = ctx.conn_ctx.db_index;

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::Hash(_)) => {
            if let PrimeValue::Hash(ref mut map) = r.value {
                let current: i64 = map.get(&field)
                    .and_then(|v| std::str::from_utf8(v).ok()?.parse().ok())
                    .unwrap_or(0);
                let new_val = current + delta;
                map.insert(field, new_val.to_string().into_bytes());
                ctx.reply.send_integer(new_val);
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => {
            let mut map = HashMap::new();
            map.insert(field, delta.to_string().into_bytes());
            ctx.shard.db_slice.add_or_update(db, &key, PrimeValue::Hash(map));
            ctx.reply.send_integer(delta);
        }
    }
}

fn cmd_hincrbyfloat(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let field = arg(&ctx, 2);
    let delta = match arg_str(&ctx, 3).and_then(|s| s.parse::<f64>().ok()) {
        Some(f) if f.is_finite() => f,
        _ => { ctx.reply.send_err("value is not a valid float"); return; }
    };
    let db = ctx.conn_ctx.db_index;

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::Hash(_)) => {
            if let PrimeValue::Hash(ref mut map) = r.value {
                let current: f64 = map.get(&field)
                    .and_then(|v| std::str::from_utf8(v).ok()?.parse().ok())
                    .unwrap_or(0.0);
                let new_val = current + delta;
                let s = new_val.to_string();
                map.insert(field, s.as_bytes().to_vec());
                ctx.reply.send_bulk_string(s.as_bytes());
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => {
            let mut map = HashMap::new();
            let s = delta.to_string();
            map.insert(field, s.as_bytes().to_vec());
            ctx.shard.db_slice.add_or_update(db, &key, PrimeValue::Hash(map));
            ctx.reply.send_bulk_string(s.as_bytes());
        }
    }
}

fn cmd_hsetnx(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let field = arg(&ctx, 2);
    let value = arg(&ctx, 3);
    let db = ctx.conn_ctx.db_index;

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::Hash(_)) => {
            if let PrimeValue::Hash(ref mut map) = r.value {
                if map.contains_key(&field) {
                    ctx.reply.send_integer(0);
                } else {
                    map.insert(field, value);
                    ctx.reply.send_integer(1);
                }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => {
            let mut map = HashMap::new();
            map.insert(field, value);
            ctx.shard.db_slice.add_or_update(db, &key, PrimeValue::Hash(map));
            ctx.reply.send_integer(1);
        }
    }
}
