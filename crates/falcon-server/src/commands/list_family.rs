use std::collections::VecDeque;

use falcon_core::compact_obj::PrimeValue;

use crate::command_registry::{CommandContext, CommandEntry, CommandHandler, CommandRegistry};

pub fn register(registry: &mut CommandRegistry) {
    let cmds: &[(&str, CommandHandler, i16)] = &[
        ("LPUSH", cmd_lpush, -3),
        ("RPUSH", cmd_rpush, -3),
        ("LPOP", cmd_lpop, -2),
        ("RPOP", cmd_rpop, -2),
        ("LLEN", cmd_llen, 2),
        ("LRANGE", cmd_lrange, 4),
        ("LINDEX", cmd_lindex, 3),
        ("LSET", cmd_lset, 4),
        ("LREM", cmd_lrem, 4),
        ("LTRIM", cmd_ltrim, 4),
        ("LINSERT", cmd_linsert, 5),
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

fn arg_int(ctx: &CommandContext, i: usize) -> Option<i64> {
    let b = ctx.args.get(i)?.as_bytes()?;
    std::str::from_utf8(b).ok()?.trim().parse().ok()
}

fn get_list_mut<'a>(ctx: &'a mut CommandContext, key: &[u8]) -> Option<&'a mut VecDeque<Vec<u8>>> {
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find_mut(db, key)? {
        r if matches!(r.value, PrimeValue::List(_)) => {
            if let PrimeValue::List(ref mut list) = r.value {
                Some(list)
            } else {
                None
            }
        }
        _ => None,
    }
}

fn cmd_lpush(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;
    let values: Vec<Vec<u8>> = (2..ctx.args.len()).map(|i| arg(&ctx, i)).collect();

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) => match r.value {
            PrimeValue::List(ref mut list) => {
                for v in &values { list.push_front(v.clone()); }
                let len = list.len();
                ctx.reply.send_integer(len as i64);
            }
            _ => ctx.reply.send_wrong_type(""),
        },
        None => {
            let mut list = VecDeque::new();
            for v in &values { list.push_front(v.clone()); }
            let len = list.len();
            ctx.shard.db_slice.add_or_update(db, &key, PrimeValue::List(list));
            ctx.reply.send_integer(len as i64);
        }
    }
}

fn cmd_rpush(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;
    let values: Vec<Vec<u8>> = (2..ctx.args.len()).map(|i| arg(&ctx, i)).collect();

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) => match r.value {
            PrimeValue::List(ref mut list) => {
                for v in &values { list.push_back(v.clone()); }
                let len = list.len();
                ctx.reply.send_integer(len as i64);
            }
            _ => ctx.reply.send_wrong_type(""),
        },
        None => {
            let mut list = VecDeque::new();
            for v in values { list.push_back(v); }
            let len = list.len();
            ctx.shard.db_slice.add_or_update(db, &key, PrimeValue::List(list));
            ctx.reply.send_integer(len as i64);
        }
    }
}

fn cmd_lpop(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let count = if ctx.args.len() > 2 { arg_int(&ctx, 2).unwrap_or(1).max(0) as usize } else { 1 };
    let db = ctx.conn_ctx.db_index;
    let single = ctx.args.len() <= 2;

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::List(_)) => {
            if let PrimeValue::List(ref mut list) = r.value {
                if single {
                    match list.pop_front() {
                        Some(v) => ctx.reply.send_bulk_string(&v),
                        None => ctx.reply.send_null(),
                    }
                } else {
                    let n = count.min(list.len());
                    ctx.reply.send_array_len(n);
                    for _ in 0..n {
                        if let Some(v) = list.pop_front() {
                            ctx.reply.send_bulk_string(&v);
                        }
                    }
                }
                if list.is_empty() { ctx.shard.db_slice.del(db, &key); }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_null(),
    }
}

fn cmd_rpop(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let count = if ctx.args.len() > 2 { arg_int(&ctx, 2).unwrap_or(1).max(0) as usize } else { 1 };
    let db = ctx.conn_ctx.db_index;
    let single = ctx.args.len() <= 2;

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::List(_)) => {
            if let PrimeValue::List(ref mut list) = r.value {
                if single {
                    match list.pop_back() {
                        Some(v) => ctx.reply.send_bulk_string(&v),
                        None => ctx.reply.send_null(),
                    }
                } else {
                    let n = count.min(list.len());
                    ctx.reply.send_array_len(n);
                    for _ in 0..n {
                        if let Some(v) = list.pop_back() {
                            ctx.reply.send_bulk_string(&v);
                        }
                    }
                }
                if list.is_empty() { ctx.shard.db_slice.del(db, &key); }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_null(),
    }
}

fn cmd_llen(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find(db, &key) {
        Some(r) => match r.value {
            PrimeValue::List(ref l) => ctx.reply.send_integer(l.len() as i64),
            _ => ctx.reply.send_wrong_type(""),
        },
        None => ctx.reply.send_integer(0),
    }
}

fn cmd_lrange(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let start = arg_int(&ctx, 2).unwrap_or(0);
    let stop = arg_int(&ctx, 3).unwrap_or(-1);
    let db = ctx.conn_ctx.db_index;

    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::List(_)) => {
            if let PrimeValue::List(ref list) = r.value {
                let len = list.len() as i64;
                let s = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
                let e = if stop < 0 { (len + stop).max(0) } else { stop.min(len - 1) } as usize;
                if s > e || s >= list.len() {
                    ctx.reply.send_array_len(0);
                } else {
                    ctx.reply.send_array_len(e - s + 1);
                    for i in s..=e { ctx.reply.send_bulk_string(&list[i]); }
                }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_array_len(0),
    }
}

fn cmd_lindex(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let idx = arg_int(&ctx, 2).unwrap_or(0);
    let db = ctx.conn_ctx.db_index;

    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::List(_)) => {
            if let PrimeValue::List(ref list) = r.value {
                let i = if idx < 0 { list.len() as i64 + idx } else { idx } as usize;
                match list.get(i) {
                    Some(v) => ctx.reply.send_bulk_string(v),
                    None => ctx.reply.send_null(),
                }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_null(),
    }
}

fn cmd_lset(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let idx = arg_int(&ctx, 2).unwrap_or(0);
    let value = arg(&ctx, 3);
    let db = ctx.conn_ctx.db_index;

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::List(_)) => {
            if let PrimeValue::List(ref mut list) = r.value {
                let i = if idx < 0 { list.len() as i64 + idx } else { idx } as usize;
                if i < list.len() {
                    list[i] = value;
                    ctx.reply.send_ok();
                } else {
                    ctx.reply.send_err("index out of range");
                }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_err("no such key"),
    }
}

fn cmd_lrem(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let count = arg_int(&ctx, 2).unwrap_or(0);
    let value = arg(&ctx, 3);
    let db = ctx.conn_ctx.db_index;

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::List(_)) => {
            if let PrimeValue::List(ref mut list) = r.value {
                let mut removed = 0i64;
                let max = if count == 0 { i64::MAX } else { count.abs() };
                if count >= 0 {
                    list.retain(|item| {
                        if removed < max && item == &value { removed += 1; false } else { true }
                    });
                } else {
                    // Remove from tail
                    let mut indices = Vec::new();
                    for (i, item) in list.iter().enumerate().rev() {
                        if removed < max && item == &value {
                            indices.push(i);
                            removed += 1;
                        }
                    }
                    for i in indices { list.remove(i); }
                }
                if list.is_empty() { ctx.shard.db_slice.del(db, &key); }
                ctx.reply.send_integer(removed);
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_integer(0),
    }
}

fn cmd_ltrim(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let start = arg_int(&ctx, 2).unwrap_or(0);
    let stop = arg_int(&ctx, 3).unwrap_or(-1);
    let db = ctx.conn_ctx.db_index;

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::List(_)) => {
            if let PrimeValue::List(ref mut list) = r.value {
                let len = list.len() as i64;
                let s = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
                let e = if stop < 0 { (len + stop).max(0) } else { stop.min(len - 1) } as usize;
                if s > e || s >= list.len() {
                    list.clear();
                } else {
                    let trimmed: VecDeque<_> = list.drain(s..=e).collect();
                    *list = trimmed;
                }
                if list.is_empty() { ctx.shard.db_slice.del(db, &key); }
            }
            ctx.reply.send_ok();
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_ok(),
    }
}

fn cmd_linsert(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let pos = std::str::from_utf8(&arg(&ctx, 2)).unwrap_or("").to_uppercase();
    let pivot = arg(&ctx, 3);
    let value = arg(&ctx, 4);
    let db = ctx.conn_ctx.db_index;

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::List(_)) => {
            if let PrimeValue::List(ref mut list) = r.value {
                let idx = list.iter().position(|item| item == &pivot);
                match idx {
                    Some(i) => {
                        let insert_at = if pos == "BEFORE" { i } else { i + 1 };
                        list.insert(insert_at, value);
                        ctx.reply.send_integer(list.len() as i64);
                    }
                    None => ctx.reply.send_integer(-1),
                }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_integer(0),
    }
}
