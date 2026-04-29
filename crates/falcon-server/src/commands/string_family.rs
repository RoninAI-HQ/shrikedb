use falcon_core::compact_obj::PrimeValue;

use crate::command_registry::{CommandContext, CommandEntry, CommandHandler, CommandRegistry};

pub fn register(registry: &mut CommandRegistry) {
    let cmds: &[(&str, CommandHandler, i16)] = &[
        ("GET", cmd_get, 2),
        ("SET", cmd_set, -3),
        ("SETNX", cmd_setnx, 3),
        ("SETEX", cmd_setex, 4),
        ("PSETEX", cmd_psetex, 4),
        ("MGET", cmd_mget, -2),
        ("MSET", cmd_mset, -3),
        ("MSETNX", cmd_msetnx, -3),
        ("GETSET", cmd_getset, 3),
        ("GETDEL", cmd_getdel, 2),
        ("GETEX", cmd_getex, -2),
        ("INCR", cmd_incr, 2),
        ("DECR", cmd_decr, 2),
        ("INCRBY", cmd_incrby, 3),
        ("DECRBY", cmd_decrby, 3),
        ("INCRBYFLOAT", cmd_incrbyfloat, 3),
        ("APPEND", cmd_append, 3),
        ("STRLEN", cmd_strlen, 2),
        ("GETRANGE", cmd_getrange, 4),
        ("SETRANGE", cmd_setrange, 4),
    ];

    for &(name, handler, arity) in cmds {
        registry.register(CommandEntry {
            name,
            handler,
            arity,
            first_key: 1,
            last_key: 1,
            key_step: 1,
        });
    }
}

/// Extract argument bytes. Bytes::clone is cheap (Arc increment, no data copy).
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

fn arg_positive_int(ctx: &CommandContext, idx: usize) -> Option<u64> {
    let n = arg_int(ctx, idx)?;
    if n <= 0 {
        None
    } else {
        Some(n as u64)
    }
}

fn cmd_get(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if r.value.is_string() => ctx.reply.send_bulk_string(&r.value.as_bytes()),
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_null(),
    }
}

fn cmd_set(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let value = arg(&ctx, 2);
    let db = ctx.conn_ctx.db_index;

    // Parse options
    let mut nx = false;
    let mut xx = false;
    let mut get = false;
    let mut keep_ttl = false;
    let mut expire_ms: Option<u64> = None;
    let mut expire_absolute = false;

    let mut i = 3;
    while i < ctx.args.len() {
        let opt = arg_str(&ctx, i).unwrap_or_default().to_uppercase();
        match opt.as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "GET" => get = true,
            "KEEPTTL" => keep_ttl = true,
            "EX" => {
                i += 1;
                match arg_positive_int(&ctx, i) {
                    Some(secs) => {
                        expire_ms = Some(secs * 1000);
                        expire_absolute = false;
                    }
                    None => {
                        ctx.reply
                            .send_err("value is not an integer or out of range");
                        return;
                    }
                }
            }
            "PX" => {
                i += 1;
                match arg_positive_int(&ctx, i) {
                    Some(ms) => {
                        expire_ms = Some(ms);
                        expire_absolute = false;
                    }
                    None => {
                        ctx.reply
                            .send_err("value is not an integer or out of range");
                        return;
                    }
                }
            }
            "EXAT" => {
                i += 1;
                match arg_positive_int(&ctx, i) {
                    Some(ts) => {
                        expire_ms = Some(ts * 1000);
                        expire_absolute = true;
                    }
                    None => {
                        ctx.reply
                            .send_err("value is not an integer or out of range");
                        return;
                    }
                }
            }
            "PXAT" => {
                i += 1;
                match arg_positive_int(&ctx, i) {
                    Some(ms) => {
                        expire_ms = Some(ms);
                        expire_absolute = true;
                    }
                    None => {
                        ctx.reply
                            .send_err("value is not an integer or out of range");
                        return;
                    }
                }
            }
            _ => {
                ctx.reply.send_err("syntax error");
                return;
            }
        }
        i += 1;
    }

    if nx && xx {
        ctx.reply
            .send_err("XX and NX options at the same time are not compatible");
        return;
    }

    // GET option: capture old value
    let old_value = if get {
        match ctx.shard.db_slice.find(db, &key) {
            Some(r) if r.value.is_string() => Some(r.value.as_bytes()),
            Some(_) => {
                ctx.reply.send_wrong_type("");
                return;
            }
            None => None,
        }
    } else {
        None
    };

    // NX/XX checks
    let exists = ctx.shard.db_slice.exists(db, &key);
    if nx && exists {
        if get {
            match &old_value {
                Some(v) => ctx.reply.send_bulk_string(v),
                None => ctx.reply.send_null(),
            }
        } else {
            ctx.reply.send_null();
        }
        return;
    }
    if xx && !exists {
        ctx.reply.send_null();
        return;
    }

    // Preserve TTL if KEEPTTL
    let old_expire = if keep_ttl && exists {
        match ctx.shard.db_slice.ttl_ms(db, &key) {
            crate::db_slice::TtlResult::Expires(ms) => Some(ctx.shard.db_slice.now_ms() + ms),
            _ => None,
        }
    } else {
        None
    };

    let pv = PrimeValue::from_bytes(&value);
    ctx.shard.db_slice.add_or_update(db, &key, pv);

    // Set expiry
    if let Some(ms) = expire_ms {
        let deadline = if expire_absolute {
            ms
        } else {
            ctx.shard.db_slice.now_ms() + ms
        };
        ctx.shard.db_slice.add_expire(db, &key, deadline);
    } else if let Some(deadline) = old_expire {
        ctx.shard.db_slice.add_expire(db, &key, deadline);
    } else if !keep_ttl {
        ctx.shard.db_slice.remove_expire(db, &key);
    }

    if get {
        match old_value {
            Some(v) => ctx.reply.send_bulk_string(&v),
            None => ctx.reply.send_null(),
        }
    } else {
        ctx.reply.send_ok();
    }
}

fn cmd_setnx(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let value = arg(&ctx, 2);
    let db = ctx.conn_ctx.db_index;
    let added = ctx
        .shard
        .db_slice
        .add_if_absent(db, &key, PrimeValue::from_bytes(&value));
    ctx.reply.send_integer(if added { 1 } else { 0 });
}

fn cmd_setex(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let secs = match arg_int(&ctx, 2) {
        Some(s) if s > 0 => s as u64,
        _ => {
            ctx.reply
                .send_err("value is not an integer or out of range");
            return;
        }
    };
    let value = arg(&ctx, 3);
    let db = ctx.conn_ctx.db_index;
    ctx.shard
        .db_slice
        .add_or_update(db, &key, PrimeValue::from_bytes(&value));
    let deadline = ctx.shard.db_slice.now_ms() + secs * 1000;
    ctx.shard.db_slice.add_expire(db, &key, deadline);
    ctx.reply.send_ok();
}

fn cmd_psetex(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let ms = match arg_int(&ctx, 2) {
        Some(s) if s > 0 => s as u64,
        _ => {
            ctx.reply
                .send_err("value is not an integer or out of range");
            return;
        }
    };
    let value = arg(&ctx, 3);
    let db = ctx.conn_ctx.db_index;
    ctx.shard
        .db_slice
        .add_or_update(db, &key, PrimeValue::from_bytes(&value));
    let deadline = ctx.shard.db_slice.now_ms() + ms;
    ctx.shard.db_slice.add_expire(db, &key, deadline);
    ctx.reply.send_ok();
}

fn cmd_mget(mut ctx: CommandContext<'_>) {
    let db = ctx.conn_ctx.db_index;
    let keys: Vec<Vec<u8>> = (1..ctx.args.len()).map(|i| arg(&ctx, i)).collect();
    ctx.reply.send_array_len(keys.len());
    for key in &keys {
        match ctx.shard.db_slice.find(db, key) {
            Some(r) if r.value.is_string() => ctx.reply.send_bulk_string(&r.value.as_bytes()),
            _ => ctx.reply.send_null(),
        }
    }
}

fn cmd_mset(mut ctx: CommandContext<'_>) {
    let db = ctx.conn_ctx.db_index;
    if (ctx.args.len() - 1) % 2 != 0 {
        ctx.reply
            .send_err("wrong number of arguments for 'mset' command");
        return;
    }
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..(ctx.args.len() - 1) / 2)
        .map(|j| (arg(&ctx, 1 + j * 2), arg(&ctx, 2 + j * 2)))
        .collect();
    for (key, value) in &pairs {
        ctx.shard
            .db_slice
            .add_or_update(db, key, PrimeValue::from_bytes(value));
        ctx.shard.db_slice.remove_expire(db, key);
    }
    ctx.reply.send_ok();
}

fn cmd_msetnx(mut ctx: CommandContext<'_>) {
    let db = ctx.conn_ctx.db_index;
    if (ctx.args.len() - 1) % 2 != 0 {
        ctx.reply
            .send_err("wrong number of arguments for 'msetnx' command");
        return;
    }
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = (0..(ctx.args.len() - 1) / 2)
        .map(|j| (arg(&ctx, 1 + j * 2), arg(&ctx, 2 + j * 2)))
        .collect();

    // Check all keys first -- MSETNX is all-or-nothing
    for (key, _) in &pairs {
        if ctx.shard.db_slice.exists(db, key) {
            ctx.reply.send_integer(0);
            return;
        }
    }
    for (key, value) in &pairs {
        ctx.shard
            .db_slice
            .add_or_update(db, key, PrimeValue::from_bytes(value));
    }
    ctx.reply.send_integer(1);
}

fn cmd_getset(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let value = arg(&ctx, 2);
    let db = ctx.conn_ctx.db_index;

    let old = match ctx.shard.db_slice.find(db, &key) {
        Some(r) if r.value.is_string() => Some(r.value.as_bytes()),
        Some(_) => {
            ctx.reply.send_wrong_type("");
            return;
        }
        None => None,
    };

    ctx.shard
        .db_slice
        .add_or_update(db, &key, PrimeValue::from_bytes(&value));
    ctx.shard.db_slice.remove_expire(db, &key);

    match old {
        Some(v) => ctx.reply.send_bulk_string(&v),
        None => ctx.reply.send_null(),
    }
}

fn cmd_getdel(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;

    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if r.value.is_string() => {
            let v = r.value.as_bytes();
            ctx.reply.send_bulk_string(&v);
            ctx.shard.db_slice.del(db, &key);
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_null(),
    }
}

fn cmd_getex(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;

    let val = match ctx.shard.db_slice.find(db, &key) {
        Some(r) if r.value.is_string() => r.value.as_bytes(),
        Some(_) => {
            ctx.reply.send_wrong_type("");
            return;
        }
        None => {
            ctx.reply.send_null();
            return;
        }
    };

    if ctx.args.len() > 2 {
        let opt = arg_str(&ctx, 2).unwrap_or_default().to_uppercase();
        match opt.as_str() {
            "EX" => {
                if let Some(secs) = arg_positive_int(&ctx, 3) {
                    let deadline = ctx.shard.db_slice.now_ms() + secs * 1000;
                    ctx.shard.db_slice.add_expire(db, &key, deadline);
                } else {
                    ctx.reply
                        .send_err("value is not an integer or out of range");
                    return;
                }
            }
            "PX" => {
                if let Some(ms) = arg_positive_int(&ctx, 3) {
                    let deadline = ctx.shard.db_slice.now_ms() + ms;
                    ctx.shard.db_slice.add_expire(db, &key, deadline);
                } else {
                    ctx.reply
                        .send_err("value is not an integer or out of range");
                    return;
                }
            }
            "EXAT" => {
                if let Some(ts) = arg_positive_int(&ctx, 3) {
                    ctx.shard.db_slice.add_expire(db, &key, ts * 1000);
                } else {
                    ctx.reply
                        .send_err("value is not an integer or out of range");
                    return;
                }
            }
            "PXAT" => {
                if let Some(ms) = arg_positive_int(&ctx, 3) {
                    ctx.shard.db_slice.add_expire(db, &key, ms);
                } else {
                    ctx.reply
                        .send_err("value is not an integer or out of range");
                    return;
                }
            }
            "PERSIST" => {
                ctx.shard.db_slice.remove_expire(db, &key);
            }
            _ => {
                ctx.reply.send_err("syntax error");
                return;
            }
        }
    }

    ctx.reply.send_bulk_string(&val);
}

fn cmd_incr(ctx: CommandContext<'_>) {
    incr_by(ctx, 1);
}

fn cmd_decr(ctx: CommandContext<'_>) {
    incr_by(ctx, -1);
}

fn cmd_incrby(mut ctx: CommandContext<'_>) {
    let delta = match arg_int(&ctx, 2) {
        Some(n) => n,
        None => {
            ctx.reply
                .send_err("value is not an integer or out of range");
            return;
        }
    };
    incr_by(ctx, delta);
}

fn cmd_decrby(mut ctx: CommandContext<'_>) {
    let delta = match arg_int(&ctx, 2) {
        Some(n) => n,
        None => {
            ctx.reply
                .send_err("value is not an integer or out of range");
            return;
        }
    };
    incr_by(ctx, -delta);
}

fn incr_by(mut ctx: CommandContext<'_>, delta: i64) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;

    let current = match ctx.shard.db_slice.find(db, &key) {
        Some(r) => match r.value.as_integer() {
            Some(n) => n,
            None => {
                ctx.reply
                    .send_err("value is not an integer or out of range");
                return;
            }
        },
        None => 0,
    };

    let new_val = match current.checked_add(delta) {
        Some(n) => n,
        None => {
            ctx.reply
                .send_err("increment or decrement would overflow");
            return;
        }
    };

    ctx.shard
        .db_slice
        .add_or_update(db, &key, PrimeValue::Integer(new_val));
    ctx.reply.send_integer(new_val);
}

fn cmd_incrbyfloat(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let delta_str = arg_str(&ctx, 2).unwrap_or_default();
    let delta: f64 = match delta_str.trim().parse() {
        Ok(f) if f64::is_finite(f) => f,
        _ => {
            ctx.reply.send_err("value is not a valid float");
            return;
        }
    };
    let db = ctx.conn_ctx.db_index;

    let current = match ctx.shard.db_slice.find(db, &key) {
        Some(r) => match r.value.as_float() {
            Some(f) => f,
            None => {
                ctx.reply.send_err("value is not a valid float");
                return;
            }
        },
        None => 0.0,
    };

    let new_val = current + delta;
    if !new_val.is_finite() {
        ctx.reply
            .send_err("increment would produce NaN or Infinity");
        return;
    }

    let s = format_float(new_val);
    ctx.shard
        .db_slice
        .add_or_update(db, &key, PrimeValue::String(s.as_bytes().to_vec()));
    ctx.reply.send_bulk_string(s.as_bytes());
}

fn format_float(f: f64) -> String {
    let i = f as i64;
    if (i as f64) == f {
        return i.to_string();
    }
    let s = format!("{:.17}", f);
    let s = s.trim_end_matches('0');
    let s = s.trim_end_matches('.');
    s.to_string()
}

fn cmd_append(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let append_data = arg(&ctx, 2);
    let db = ctx.conn_ctx.db_index;

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) => {
            if !r.value.is_string() {
                ctx.reply.send_wrong_type("");
                return;
            }
            r.value.ensure_string();
            if let PrimeValue::String(ref mut s) = r.value {
                s.extend_from_slice(&append_data);
                let len = s.len();
                ctx.reply.send_integer(len as i64);
            }
        }
        None => {
            let len = append_data.len();
            ctx.shard
                .db_slice
                .add_or_update(db, &key, PrimeValue::String(append_data));
            ctx.reply.send_integer(len as i64);
        }
    }
}

fn cmd_strlen(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if r.value.is_string() => ctx.reply.send_integer(r.value.strlen() as i64),
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_integer(0),
    }
}

fn cmd_getrange(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let start: i64 = match arg_int(&ctx, 2) {
        Some(n) => n,
        None => {
            ctx.reply
                .send_err("value is not an integer or out of range");
            return;
        }
    };
    let end: i64 = match arg_int(&ctx, 3) {
        Some(n) => n,
        None => {
            ctx.reply
                .send_err("value is not an integer or out of range");
            return;
        }
    };
    let db = ctx.conn_ctx.db_index;

    let data = match ctx.shard.db_slice.find(db, &key) {
        Some(r) if r.value.is_string() => r.value.as_bytes(),
        Some(_) => {
            ctx.reply.send_wrong_type("");
            return;
        }
        None => {
            ctx.reply.send_bulk_string(b"");
            return;
        }
    };

    let len = data.len() as i64;
    let s = if start < 0 {
        (len + start).max(0)
    } else {
        start.min(len)
    } as usize;
    let e = if end < 0 {
        (len + end).max(0)
    } else {
        end.min(len - 1).max(0)
    } as usize;

    if s > e || s >= data.len() {
        ctx.reply.send_bulk_string(b"");
    } else {
        ctx.reply.send_bulk_string(&data[s..=e]);
    }
}

fn cmd_setrange(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let offset = match arg_int(&ctx, 2) {
        Some(n) if n >= 0 => n as usize,
        _ => {
            ctx.reply.send_err("offset is out of range");
            return;
        }
    };
    let value = arg(&ctx, 3);
    let db = ctx.conn_ctx.db_index;
    let needed = offset + value.len();

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) => {
            if !r.value.is_string() {
                ctx.reply.send_wrong_type("");
                return;
            }
            r.value.ensure_string();
            if let PrimeValue::String(ref mut s) = r.value {
                if s.len() < needed {
                    s.resize(needed, 0);
                }
                s[offset..offset + value.len()].copy_from_slice(&value);
                let len = s.len();
                ctx.reply.send_integer(len as i64);
            }
        }
        None => {
            let mut s = vec![0u8; needed];
            s[offset..offset + value.len()].copy_from_slice(&value);
            let len = s.len();
            ctx.shard
                .db_slice
                .add_or_update(db, &key, PrimeValue::String(s));
            ctx.reply.send_integer(len as i64);
        }
    }
}
