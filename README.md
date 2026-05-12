# 🦅 ShrikeDB

> A Redis-compatible in-memory database written in **Rust** — built on a shared-nothing, multi-threaded architecture for maximum throughput on modern hardware.

[![License: BSL-1.1](https://img.shields.io/badge/License-BSL--1.1-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org/)
[![Redis Compatible](https://img.shields.io/badge/Redis-compatible-red.svg)](https://redis.io/)

---

## ✨ Features

- ⚡ **High-performance** — shared-nothing architecture eliminates lock contention on the hot data path
- 🦀 **Written in Rust** — memory safe, zero-cost abstractions, and no GC pauses
- 🔀 **Multi-threaded sharding** — each shard runs on its own OS thread with a dedicated Tokio runtime
- 🔌 **Redis-compatible** — works with any existing Redis client, no code changes required
- 🗂️ **Hash-tag routing** — `{tag}` keys are co-located on the same shard for multi-key atomicity
- 📦 **Lightweight** — minimal dependencies, single binary, zero configuration required to start

---

## 🚀 Quick Start

### Prerequisites

- [Rust](https://rustup.rs/) 1.75 or later

### Build & Run

```bash
# Clone the repository
git clone https://github.com/RoninAI-HQ/shrikedb.git
cd shrikedb

# Build (optimized release build)
cargo build --release

# Run with defaults (port 6379, auto-detects CPU core count for shards)
./target/release/shrikedb

# Run with a custom port and shard count
SHRIKEDB_PORT=6380 SHRIKEDB_SHARDS=4 ./target/release/shrikedb
```

### Connect

ShrikeDB speaks the Redis Serialization Protocol (RESP), so any Redis client works out of the box:

```bash
# Using redis-cli
redis-cli -p 6380

# Quick smoke test
redis-cli -p 6380 PING
# => PONG
```

---

## 💡 Why ShrikeDB

Redis is fast, simple, and battle-tested — but it was designed around a single-threaded event loop. On a modern 16-core box you get the throughput of one core; the other 15 idle or run a sidecar of independent instances stitched together with cluster mode. ShrikeDB is built around a few principles that change that math:

### 1. Vertical scaling on multi-core hardware

Each ShrikeDB shard is an independent execution unit pinned to its own OS thread, with its own Tokio single-threaded runtime, its own hash table, and its own slice of the keyspace. A 16-core machine runs 16 shards in parallel with no cross-thread coordination on the hot path. Throughput grows roughly linearly with cores up to network and memory-bandwidth limits — a single ShrikeDB process replaces what would otherwise be a cluster of Redis instances.

### 2. Shared-nothing — no locks on the data path

Keys are deterministically routed to shards by a CRC32 hash of the key (or hash-tag `{tag}` if present). Because only the owning shard ever touches a given key's storage, there are no mutexes, no spinlocks, and no atomic read-modify-write contention on individual entries. Single-key commands take exactly one shard hop. Multi-key commands (`MGET`, `MSET`, `DEL`, `EXISTS`) fan out to the relevant shards in parallel and gather replies — still without ever acquiring a lock.

### 3. A modern hash table designed for in-memory workloads

The storage core is a [Dash](https://arxiv.org/pdf/2003.07302.pdf)-style extendible hash table — segments of fixed-size buckets with stash slots, fingerprint-based filtering, and incremental directory doubling. Concretely this gives you:

- **No "stop-the-world" rehash.** Redis's incremental rehash interleaves moves with normal commands but still allocates the full new dictionary up front and copies one bucket at a time. Dash splits one segment at a time, so resizes never cause an allocation spike or a tail-latency cliff.
- **Lower per-key overhead.** Buckets store 8-bit fingerprints alongside slots; lookups skip slots whose fingerprint doesn't match without ever touching the key bytes. Fewer pointer chases, fewer cache misses, less memory per entry.
- **Stateless scans.** `SCAN` cursors are derived from segment IDs, not from a snapshot of internal state, so iteration is correct under concurrent inserts, deletes, and resizes.

### 4. Compact value representation

Short strings, integers, and small collections are stored inline in a 16-byte `CompactObj` — no heap allocation, no separate string header, no allocator metadata. This matters in real workloads: most cache values are short identifiers, counters, JSON blobs in the dozens of bytes, or empty/short hashes. Inline storage removes one allocation per key for the common case and keeps hot data closer in L1/L2.

### 5. Memory safety, without a GC

The whole engine is Rust. The data path is `unsafe`-free in the parts that matter; the few `unsafe` blocks (raw bucket access in Dash, RESP zero-copy slicing) are isolated, audited, and not exposed to the rest of the codebase. There is no garbage collector and no STW pause — memory is reclaimed deterministically when a key is deleted or expires.

### 6. Drop-in compatibility

Clients connect with RESP2. `redis-cli`, `redis-benchmark`, `memtier_benchmark`, `ioredis`, `lettuce`, `go-redis`, `redis-py` — they all work. You don't write code against ShrikeDB; you point your existing Redis client at port 6379 and it just works.

---

## ⚙️ Configuration

ShrikeDB is configured entirely via environment variables — no config file needed.

| Variable        | Default        | Description                                   |
|-----------------|----------------|-----------------------------------------------|
| `SHRIKEDB_PORT` | `6379`         | TCP port to listen on                         |
| `SHRIKEDB_SHARDS` | CPU core count | Number of shard threads                     |
| `RUST_LOG`      | `info`         | Log level: `debug`, `info`, `warn`, `error`   |

---

## 📖 Supported Commands

### Strings

| Command       | Syntax                                                                 |
|---------------|------------------------------------------------------------------------|
| `GET`         | `GET key`                                                              |
| `SET`         | `SET key value [NX\|XX] [EX s\|PX ms\|EXAT ts\|PXAT ms] [KEEPTTL] [GET]` |
| `SETNX`       | `SETNX key value`                                                      |
| `SETEX`       | `SETEX key seconds value`                                              |
| `PSETEX`      | `PSETEX key milliseconds value`                                        |
| `MGET`        | `MGET key1 key2 key3`                                                  |
| `MSET`        | `MSET key1 val1 key2 val2`                                             |
| `MSETNX`      | `MSETNX key1 val1 key2 val2`                                           |
| `GETSET`      | `GETSET key value`                                                     |
| `GETDEL`      | `GETDEL key`                                                           |
| `GETEX`       | `GETEX key [EX s\|PX ms\|EXAT ts\|PXAT ms\|PERSIST]`                  |
| `INCR`        | `INCR counter`                                                         |
| `DECR`        | `DECR counter`                                                         |
| `INCRBY`      | `INCRBY counter 5`                                                     |
| `DECRBY`      | `DECRBY counter 3`                                                     |
| `INCRBYFLOAT` | `INCRBYFLOAT key 1.5`                                                  |
| `APPEND`      | `APPEND key " world"`                                                  |
| `STRLEN`      | `STRLEN key`                                                           |
| `GETRANGE`    | `GETRANGE key 0 4`                                                     |
| `SETRANGE`    | `SETRANGE key 6 "world"`                                               |

### Keys

| Command      | Syntax                                   |
|--------------|------------------------------------------|
| `DEL`        | `DEL key1 key2`                          |
| `UNLINK`     | `UNLINK key1 key2`                       |
| `EXISTS`     | `EXISTS key1 key2`                       |
| `TYPE`       | `TYPE key`                               |
| `RENAME`     | `RENAME oldkey newkey`                   |
| `RENAMENX`   | `RENAMENX oldkey newkey`                 |
| `EXPIRE`     | `EXPIRE key 60`                          |
| `PEXPIRE`    | `PEXPIRE key 60000`                      |
| `EXPIREAT`   | `EXPIREAT key 1735689600`                |
| `PEXPIREAT`  | `PEXPIREAT key 1735689600000`            |
| `TTL`        | `TTL key`                                |
| `PTTL`       | `PTTL key`                               |
| `PERSIST`    | `PERSIST key`                            |
| `KEYS`       | `KEYS user:*`                            |
| `SCAN`       | `SCAN 0 MATCH user:* COUNT 100`          |
| `RANDOMKEY`  | `RANDOMKEY`                              |

### Server

| Command    | Syntax                     |
|------------|----------------------------|
| `PING`     | `PING [message]`           |
| `ECHO`     | `ECHO "hello"`             |
| `SELECT`   | `SELECT 0` (databases 0–15)|
| `DBSIZE`   | `DBSIZE`                   |
| `FLUSHDB`  | `FLUSHDB`                  |
| `FLUSHALL` | `FLUSHALL`                 |
| `INFO`     | `INFO`                     |
| `COMMAND`  | `COMMAND`                  |
| `QUIT`     | `QUIT`                     |

---

## 🧪 Try It Out

```bash
# Start the server in the background
SHRIKEDB_PORT=6380 ./target/release/shrikedb &

# Basic key-value
redis-cli -p 6380 SET greeting "hello world"
redis-cli -p 6380 GET greeting

# Atomic counter
redis-cli -p 6380 SET visits 0
redis-cli -p 6380 INCR visits
redis-cli -p 6380 INCR visits
redis-cli -p 6380 GET visits        # => "2"

# Key expiration
redis-cli -p 6380 SET session abc123 EX 30
redis-cli -p 6380 TTL session

# Bulk operations
redis-cli -p 6380 MSET name "Alice" age "30" city "NYC"
redis-cli -p 6380 MGET name age city

# Pattern search
redis-cli -p 6380 KEYS '*'

# Interactive session
redis-cli -p 6380
> SET user:1:name "Bob"
> SET user:1:email "bob@example.com"
> KEYS user:*
> DEL user:1:name user:1:email
> DBSIZE

# Run the Redis benchmark suite
redis-benchmark -p 6380 -t set,get -n 100000 -c 50 -q

# Stop the server
kill %1
```

---

## 🏗️ Architecture

ShrikeDB uses a **shared-nothing, multi-shard** design. Incoming connections are handled by a multi-threaded Tokio listener, and each request is routed to the appropriate shard by hashing the key.

```
                    ┌──────────┐
   Clients ───────▶ │ Listener │  (tokio multi-thread)
                    └────┬─────┘
                         │ route by CRC32(key)
            ┌────────────┼────────────┐
            ▼            ▼            ▼
       ┌─────────┐  ┌─────────┐  ┌─────────┐
       │ Shard 0 │  │ Shard 1 │  │ Shard N │   (one OS thread each)
       │ DbSlice │  │ DbSlice │  │ DbSlice │
       │ DashTbl │  │ DashTbl │  │ DashTbl │
       └─────────┘  └─────────┘  └─────────┘
```

**Key design properties:**

- Each shard runs on its own OS thread with a dedicated single-threaded Tokio runtime
- Keys are assigned to shards via **CRC32 hash** — hash-tags (`{tag}`) are supported for co-location
- **Zero locks on the hot path** — each shard exclusively owns its data
- Multi-key commands (`MGET`, `MSET`, `DEL`, etc.) automatically fan out and aggregate across shards

---

## 📁 Project Structure

```
shrikedb/
├── Cargo.toml                  # Workspace manifest
└── crates/
    ├── shrikedb-core/            # Core data structures: DashTable, CompactObj
    ├── shrikedb-facade/          # Networking: RESP parser, connection handler, TCP listener
    ├── shrikedb-server/          # Storage engine, command dispatch, sharding, transactions
    ├── shrikedb-persistence/     # RDB save/load (planned)
    └── shrikedb-bin/             # Binary entry point
```

---

## 🔨 Building & Testing

```bash
# Debug build
cargo build

# Optimized release build
cargo build --release

# Run all tests
cargo test

# Run with debug-level logging
RUST_LOG=debug SHRIKEDB_PORT=6380 cargo run --bin shrike
```

---

## 🗺️ Roadmap

- [x] RESP protocol parsing
- [x] String commands
- [x] Key expiration (TTL / PTTL)
- [x] Multi-shard fan-out for multi-key commands
- [x] Hash-tag key co-location
- [ ] Hash, List, Set, Sorted Set data types
- [ ] RDB persistence (`shrikedb-persistence`)
- [ ] AOF / append-only log
- [ ] Pub/Sub
- [ ] Lua scripting
- [ ] Cluster mode

---

## 🤝 Contributing

Contributions are welcome! Please open an issue or pull request on [GitHub](https://github.com/RoninAI-HQ/shrikedb).

1. Fork the repository
2. Create a feature branch (`git checkout -b feat/my-feature`)
3. Commit your changes (`git commit -m 'feat: add my feature'`)
4. Push to the branch (`git push origin feat/my-feature`)
5. Open a Pull Request

---

## 📜 License

ShrikeDB is licensed under the **Business Source License 1.1 (BSL-1.1)**.  
See [LICENSE](LICENSE) for details.
