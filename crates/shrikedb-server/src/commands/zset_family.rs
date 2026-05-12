use std::collections::{BTreeMap, HashMap};

use shrikedb_core::compact_obj::{PrimeValue, SortedSetEntry};

use crate::command_registry::{CommandContext, CommandEntry, CommandHandler, CommandRegistry};

pub fn register(registry: &mut CommandRegistry) {
    let cmds: &[(&str, CommandHandler, i16)] = &[
        ("ZADD", cmd_zadd, -4),
        ("ZREM", cmd_zrem, -3),
        ("ZSCORE", cmd_zscore, 3),
        ("ZRANK", cmd_zrank, 3),
        ("ZREVRANK", cmd_zrevrank, 3),
        ("ZRANGE", cmd_zrange, -4),
        ("ZRANGEBYSCORE", cmd_zrangebyscore, -4),
        ("ZCARD", cmd_zcard, 2),
        ("ZCOUNT", cmd_zcount, 4),
        ("ZINCRBY", cmd_zincrby, 4),
        ("ZPOPMIN", cmd_zpopmin, -2),
        ("ZPOPMAX", cmd_zpopmax, -2),
        ("ZMSCORE", cmd_zmscore, -3),
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

fn new_zset() -> PrimeValue {
    PrimeValue::ZSet {
        members: HashMap::new(),
        scores: BTreeMap::new(),
    }
}

fn zset_add(members: &mut HashMap<Vec<u8>, f64>, scores: &mut BTreeMap<SortedSetEntry, ()>, member: Vec<u8>, score: f64) -> bool {
    if let Some(&old_score) = members.get(&member) {
        scores.remove(&SortedSetEntry { score: old_score, member: member.clone() });
        scores.insert(SortedSetEntry { score, member: member.clone() }, ());
        members.insert(member, score);
        false // not new
    } else {
        scores.insert(SortedSetEntry { score, member: member.clone() }, ());
        members.insert(member, score);
        true // new
    }
}

fn zset_remove(members: &mut HashMap<Vec<u8>, f64>, scores: &mut BTreeMap<SortedSetEntry, ()>, member: &[u8]) -> bool {
    if let Some(score) = members.remove(member) {
        scores.remove(&SortedSetEntry { score, member: member.to_vec() });
        true
    } else {
        false
    }
}

fn cmd_zadd(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;

    // Parse score-member pairs (simplified: no NX/XX/GT/LT/CH flags)
    if (ctx.args.len() - 2) % 2 != 0 {
        ctx.reply.send_err("wrong number of arguments for 'zadd' command");
        return;
    }
    let pairs: Vec<(f64, Vec<u8>)> = (0..(ctx.args.len() - 2) / 2)
        .filter_map(|j| {
            let score: f64 = arg_str(&ctx, 2 + j * 2)?.parse().ok()?;
            Some((score, arg(&ctx, 3 + j * 2)))
        })
        .collect();

    if pairs.len() != (ctx.args.len() - 2) / 2 {
        ctx.reply.send_err("value is not a valid float");
        return;
    }

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::ZSet { .. }) => {
            if let PrimeValue::ZSet { ref mut members, ref mut scores } = r.value {
                let mut added = 0i64;
                for (score, member) in pairs {
                    if zset_add(members, scores, member, score) { added += 1; }
                }
                ctx.reply.send_integer(added);
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => {
            let mut members = HashMap::new();
            let mut scores = BTreeMap::new();
            let mut added = 0i64;
            for (score, member) in pairs {
                if zset_add(&mut members, &mut scores, member, score) { added += 1; }
            }
            ctx.shard.db_slice.add_or_update(db, &key, PrimeValue::ZSet { members, scores });
            ctx.reply.send_integer(added);
        }
    }
}

fn cmd_zrem(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let mems: Vec<Vec<u8>> = (2..ctx.args.len()).map(|i| arg(&ctx, i)).collect();
    let db = ctx.conn_ctx.db_index;

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::ZSet { .. }) => {
            if let PrimeValue::ZSet { ref mut members, ref mut scores } = r.value {
                let mut removed = 0i64;
                for m in &mems { if zset_remove(members, scores, m) { removed += 1; } }
                if members.is_empty() { ctx.shard.db_slice.del(db, &key); }
                ctx.reply.send_integer(removed);
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_integer(0),
    }
}

fn cmd_zscore(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let member = arg(&ctx, 2);
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::ZSet { .. }) => {
            if let PrimeValue::ZSet { ref members, .. } = r.value {
                match members.get(&member) {
                    Some(s) => ctx.reply.send_bulk_string(s.to_string().as_bytes()),
                    None => ctx.reply.send_null(),
                }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_null(),
    }
}

fn cmd_zmscore(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let mems: Vec<Vec<u8>> = (2..ctx.args.len()).map(|i| arg(&ctx, i)).collect();
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::ZSet { .. }) => {
            if let PrimeValue::ZSet { ref members, .. } = r.value {
                ctx.reply.send_array_len(mems.len());
                for m in &mems {
                    match members.get(m) {
                        Some(s) => ctx.reply.send_bulk_string(s.to_string().as_bytes()),
                        None => ctx.reply.send_null(),
                    }
                }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => {
            ctx.reply.send_array_len(mems.len());
            for _ in &mems { ctx.reply.send_null(); }
        }
    }
}

fn cmd_zrank(mut ctx: CommandContext<'_>) {
    zrank_impl(&mut ctx, false);
}

fn cmd_zrevrank(mut ctx: CommandContext<'_>) {
    zrank_impl(&mut ctx, true);
}

fn zrank_impl(ctx: &mut CommandContext<'_>, rev: bool) {
    let key = arg(ctx, 1);
    let member = arg(ctx, 2);
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::ZSet { .. }) => {
            if let PrimeValue::ZSet { ref members, ref scores } = r.value {
                match members.get(&member) {
                    Some(&score) => {
                        let entry = SortedSetEntry { score, member: member.clone() };
                        let rank = if rev {
                            scores.range(entry..).count() - 1
                        } else {
                            scores.range(..entry).count()
                        };
                        // BTreeMap range requires Ord bounds but we have the entry
                        // Simple: iterate and count position
                        let mut pos = 0usize;
                        let target = SortedSetEntry { score, member };
                        if rev {
                            for (e, _) in scores.iter().rev() {
                                if *e == target { break; }
                                pos += 1;
                            }
                        } else {
                            for (e, _) in scores.iter() {
                                if *e == target { break; }
                                pos += 1;
                            }
                        }
                        ctx.reply.send_integer(pos as i64);
                    }
                    None => ctx.reply.send_null(),
                }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_null(),
    }
}

fn cmd_zrange(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let start = arg_str(&ctx, 2).and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
    let stop = arg_str(&ctx, 3).and_then(|s| s.parse::<i64>().ok()).unwrap_or(-1);
    let withscores = ctx.args.len() > 4 && arg_str(&ctx, 4).map(|s| s.to_uppercase()) == Some("WITHSCORES".to_string());
    let db = ctx.conn_ctx.db_index;

    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::ZSet { .. }) => {
            if let PrimeValue::ZSet { ref scores, .. } = r.value {
                let len = scores.len() as i64;
                let s = if start < 0 { (len + start).max(0) } else { start.min(len) } as usize;
                let e = if stop < 0 { (len + stop).max(0) } else { stop.min(len - 1) } as usize;
                if s > e || s >= scores.len() {
                    ctx.reply.send_array_len(0);
                } else {
                    let entries: Vec<_> = scores.keys().skip(s).take(e - s + 1).collect();
                    if withscores {
                        ctx.reply.send_array_len(entries.len() * 2);
                        for e in &entries {
                            ctx.reply.send_bulk_string(&e.member);
                            ctx.reply.send_bulk_string(e.score.to_string().as_bytes());
                        }
                    } else {
                        ctx.reply.send_array_len(entries.len());
                        for e in &entries { ctx.reply.send_bulk_string(&e.member); }
                    }
                }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_array_len(0),
    }
}

fn cmd_zrangebyscore(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let min_s = arg_str(&ctx, 2).unwrap_or_else(|| "-inf".to_string());
    let max_s = arg_str(&ctx, 3).unwrap_or_else(|| "+inf".to_string());
    let db = ctx.conn_ctx.db_index;

    let min = parse_score_bound(&min_s, f64::NEG_INFINITY);
    let max = parse_score_bound(&max_s, f64::INFINITY);

    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::ZSet { .. }) => {
            if let PrimeValue::ZSet { ref scores, .. } = r.value {
                let entries: Vec<_> = scores.keys()
                    .filter(|e| e.score >= min && e.score <= max)
                    .collect();
                ctx.reply.send_array_len(entries.len());
                for e in &entries { ctx.reply.send_bulk_string(&e.member); }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_array_len(0),
    }
}

fn cmd_zcard(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let db = ctx.conn_ctx.db_index;
    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::ZSet { .. }) => {
            if let PrimeValue::ZSet { ref members, .. } = r.value {
                ctx.reply.send_integer(members.len() as i64);
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_integer(0),
    }
}

fn cmd_zcount(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let min_s = arg_str(&ctx, 2).unwrap_or_else(|| "-inf".to_string());
    let max_s = arg_str(&ctx, 3).unwrap_or_else(|| "+inf".to_string());
    let db = ctx.conn_ctx.db_index;

    let min = parse_score_bound(&min_s, f64::NEG_INFINITY);
    let max = parse_score_bound(&max_s, f64::INFINITY);

    match ctx.shard.db_slice.find(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::ZSet { .. }) => {
            if let PrimeValue::ZSet { ref scores, .. } = r.value {
                let count = scores.keys().filter(|e| e.score >= min && e.score <= max).count();
                ctx.reply.send_integer(count as i64);
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_integer(0),
    }
}

fn cmd_zincrby(mut ctx: CommandContext<'_>) {
    let key = arg(&ctx, 1);
    let delta: f64 = match arg_str(&ctx, 2).and_then(|s| s.parse().ok()) {
        Some(f) => f,
        None => { ctx.reply.send_err("value is not a valid float"); return; }
    };
    let member = arg(&ctx, 3);
    let db = ctx.conn_ctx.db_index;

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::ZSet { .. }) => {
            if let PrimeValue::ZSet { ref mut members, ref mut scores } = r.value {
                let old_score = members.get(&member).copied().unwrap_or(0.0);
                let new_score = old_score + delta;
                zset_add(members, scores, member, new_score);
                ctx.reply.send_bulk_string(new_score.to_string().as_bytes());
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => {
            let mut members = HashMap::new();
            let mut scores = BTreeMap::new();
            zset_add(&mut members, &mut scores, member, delta);
            ctx.shard.db_slice.add_or_update(db, &key, PrimeValue::ZSet { members, scores });
            ctx.reply.send_bulk_string(delta.to_string().as_bytes());
        }
    }
}

fn cmd_zpopmin(mut ctx: CommandContext<'_>) {
    zpop_impl(&mut ctx, false);
}

fn cmd_zpopmax(mut ctx: CommandContext<'_>) {
    zpop_impl(&mut ctx, true);
}

fn zpop_impl(ctx: &mut CommandContext<'_>, rev: bool) {
    let key = arg(ctx, 1);
    let count = if ctx.args.len() > 2 {
        arg_str(ctx, 2).and_then(|s| s.parse::<usize>().ok()).unwrap_or(1)
    } else { 1 };
    let db = ctx.conn_ctx.db_index;

    match ctx.shard.db_slice.find_mut(db, &key) {
        Some(r) if matches!(r.value, PrimeValue::ZSet { .. }) => {
            if let PrimeValue::ZSet { ref mut members, ref mut scores } = r.value {
                let mut popped = Vec::new();
                for _ in 0..count {
                    let entry = if rev {
                        scores.keys().next_back().cloned()
                    } else {
                        scores.keys().next().cloned()
                    };
                    match entry {
                        Some(e) => {
                            scores.remove(&e);
                            members.remove(&e.member);
                            popped.push(e);
                        }
                        None => break,
                    }
                }
                ctx.reply.send_array_len(popped.len() * 2);
                for e in &popped {
                    ctx.reply.send_bulk_string(&e.member);
                    ctx.reply.send_bulk_string(e.score.to_string().as_bytes());
                }
                if members.is_empty() { ctx.shard.db_slice.del(db, &key); }
            }
        }
        Some(_) => ctx.reply.send_wrong_type(""),
        None => ctx.reply.send_array_len(0),
    }
}

fn parse_score_bound(s: &str, default: f64) -> f64 {
    match s {
        "-inf" => f64::NEG_INFINITY,
        "+inf" | "inf" => f64::INFINITY,
        s if s.starts_with('(') => s[1..].parse().unwrap_or(default),
        s => s.parse().unwrap_or(default),
    }
}
