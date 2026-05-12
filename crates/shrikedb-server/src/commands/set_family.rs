use std::collections::HashSet;

use shrikedb_core::compact_obj::PrimeValue;

use crate::command_registry::{CommandContext, CommandEntry, CommandHandler, CommandRegistry};

pub fn register(registry: &mut CommandRegistry) {
    let cmds: &[(&str, CommandHandler, i16)] = &[
        ("SADD", cmd_sadd, -3),
        ("SREM", cmd_srem, -3),
        ("SMEMBERS", cmd_smembers, 2),
        ("SISMEMBER", cmd_sismember, 3),
        ("SMISMEMBER", cmd_smismember, -3),
        ("SCARD", cmd_scard, 2),
        ("SRANDMEMBER", cmd_srandmember, -2),
        ("SPOP", cmd_spop, -2),
        ("SINTER", cmd_sinter, -2),
        ("SUNION", cmd_sunion, -2),
        ("SDIFF", cmd_sdiff, -2),
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

fn cmd_sadd(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let members: Vec<Vec<u8>> = (2..ctx.args.len()).map(|i| arg(&ctx, i)).collect();
    let db = ctx.conn_ctx.db_index;

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) => match r.value {
            PrimeValue::Set(ref mut set) => {
                let mut added = 0i64;
                for m in members { if set.insert(m) { added += 1; } }
                ctx.reply.send_integer(added);
            }
            _ => ctx.reply.send_wrong_type(""),
        },
        None => {
            let mut set = HashSet::new();
            let mut added = 0i64;
            for m in members { if set.insert(m) { added += 1; } }
            ctx.shard.db_slice.add_or_update(db, &key, PrimeValue::Set(set));
            ctx.reply.send_integer(added);
        }
    }
}

fn cmd_srem(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let members: Vec<Vec<u8>> = (2..ctx.args.len()).map(|i| arg(&ctx, i)).collect();
    let db = ctx.conn_ctx.db_index;

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::Set(_)) => {
            if let PrimeValue::Set(ref mut set) = r.value {
                let mut removed = 0i64;
                for m in &members { if set.remove(m) { removed += 1; } }
                if set.is_empty() { ctx.shard.db_slice.del(db, &key); }
                ctx.reply.send_integer(removed);
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_integer(0),
    }
}

fn cmd_smembers(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::Set(_)) => {
            if let PrimeValue::Set(ref set) = r.value {
                ctx.reply.send_array_len(set.len());
                for m in set { ctx.reply.send_bulk_string(m); }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_array_len(0),
    }
}

fn cmd_sismember(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let member = arg(&ctx, 2);
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::Set(_)) => {
            if let PrimeValue::Set(ref set) = r.value {
                ctx.reply.send_integer(if set.contains(&member) { 1 } else { 0 });
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_integer(0),
    }
}

fn cmd_smismember(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let members: Vec<Vec<u8>> = (2..ctx.args.len()).map(|i| arg(&ctx, i)).collect();
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::Set(_)) => {
            if let PrimeValue::Set(ref set) = r.value {
                ctx.reply.send_array_len(members.len());
                for m in &members {
                    ctx.reply.send_integer(if set.contains(m) { 1 } else { 0 });
                }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => {
            ctx.reply.send_array_len(members.len());
            for _ in &members { ctx.reply.send_integer(0); }
        }
    }
}

fn cmd_scard(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::Set(_)) => {
            if let PrimeValue::Set(ref set) = r.value {
                ctx.reply.send_integer(set.len() as i64);
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_integer(0),
    }
}

fn cmd_srandmember(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::Set(_)) => {
            if let PrimeValue::Set(ref set) = r.value {
                if let Some(m) = set.iter().next() {
                    ctx.reply.send_bulk_string(m);
                } else {
                    ctx.reply.send_null();
                }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_null(),
    }
}

fn cmd_spop(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::Set(_)) => {
            if let PrimeValue::Set(ref mut set) = r.value {
                let member = set.iter().next().cloned();
                match member {
                    Some(m) => {
                        set.remove(&m);
                        if set.is_empty() { ctx.shard.db_slice.del(db, &key); }
                        ctx.reply.send_bulk_string(&m);
                    }
                    None => ctx.reply.send_null(),
                }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_null(),
    }
}

// Set operations (single-shard only for now)
fn cmd_sinter(mut ctx: CommandContext<'_>) {
    set_op(&mut ctx, SetOp::Inter);
}

fn cmd_sunion(mut ctx: CommandContext<'_>) {
    set_op(&mut ctx, SetOp::Union);
}

fn cmd_sdiff(mut ctx: CommandContext<'_>) {
    set_op(&mut ctx, SetOp::Diff);
}

enum SetOp { Inter, Union, Diff }

fn set_op(ctx: &mut CommandContext<'_>, op: SetOp) {
    let db = ctx.conn_ctx.db_index;
    let keys: Vec<Vec<u8>> = (1..ctx.args.len()).map(|i| arg(ctx, i)).collect();

    let mut sets: Vec<HashSet<Vec<u8>>> = Vec::new();
    for key in &keys {
        match ctx.shard.db_slice.find(db, key) {
            Some(r) if matches!(r.value, PrimeValue::Set(_)) => {
                if let PrimeValue::Set(ref set) = r.value {
                    sets.push(set.clone());
                }
            }
            Some(_) => { ctx.reply.send_wrong_type(""); return; }
            None => { sets.push(HashSet::new()); }
        }
    }

    if sets.is_empty() {
        ctx.reply.send_array_len(0);
        return;
    }

    let result: HashSet<Vec<u8>> = match op {
        SetOp::Inter => {
            let mut it = sets.into_iter();
            let first = it.next().unwrap();
            it.fold(first, |acc, s| acc.intersection(&s).cloned().collect())
        }
        SetOp::Union => {
            let mut it = sets.into_iter();
            let first = it.next().unwrap();
            it.fold(first, |acc, s| acc.union(&s).cloned().collect())
        }
        SetOp::Diff => {
            let mut it = sets.into_iter();
            let first = it.next().unwrap();
            it.fold(first, |acc, s| acc.difference(&s).cloned().collect())
        }
    };

    ctx.reply.send_array_len(result.len());
    for m in &result { ctx.reply.send_bulk_string(m); }
}
