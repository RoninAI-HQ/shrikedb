# FalconDB

A Redis-compatible in-memory database written in Rust, with a shared-nothing, multi-threaded architecture.

## Quick Start

```bash
# Build
cargo build --release

# Run (defaults to port 6379, auto-detects CPU cores for shard count)
./target/release/falcondb

# Run with custom port and shard count
FALCONDB_PORT=6380 FALCONDB_SHARDS=4 ./target/release/falcondb
```

Connect with any Redis client:

```bash
redis-cli -p 6380
```

## Configuration

FalconDB is configured via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `FALCONDB_PORT` | `6379` | TCP port to listen on |
| `FALCONDB_SHARDS` | CPU core count | Number of shard threads |
| `RUST_LOG` | `info` | Log level (`debug`, `info`, `warn`, `error`) |

## Supported Commands

### Strings

| Command | Example |
|---------|---------|
| `GET` | `GET key` |
| `SET` | `SET key value [NX\|XX] [EX s\|PX ms\|EXAT ts\|PXAT ms] [KEEPTTL] [GET]` |
| `SETNX` | `SETNX key value` |
| `SETEX` | `SETEX key seconds value` |
| `PSETEX` | `PSETEX key milliseconds value` |
| `MGET` | `MGET key1 key2 key3` |
| `MSET` | `MSET key1 val1 key2 val2` |
| `MSETNX` | `MSETNX key1 val1 key2 val2` |
| `GETSET` | `GETSET key value` |
| `GETDEL` | `GETDEL key` |
| `GETEX` | `GETEX key [EX s\|PX ms\|EXAT ts\|PXAT ms\|PERSIST]` |
| `INCR` | `INCR counter` |
| `DECR` | `DECR counter` |
| `INCRBY` | `INCRBY counter 5` |
| `DECRBY` | `DECRBY counter 3` |
| `INCRBYFLOAT` | `INCRBYFLOAT key 1.5` |
| `APPEND` | `APPEND key " world"` |
| `STRLEN` | `STRLEN key` |
| `GETRANGE` | `GETRANGE key 0 4` |
| `SETRANGE` | `SETRANGE key 6 "world"` |

### Keys

| Command | Example |
|---------|---------|
| `DEL` | `DEL key1 key2` |
| `UNLINK` | `UNLINK key1 key2` |
| `EXISTS` | `EXISTS key1 key2` |
| `TYPE` | `TYPE key` |
| `RENAME` | `RENAME oldkey newkey` |
| `RENAMENX` | `RENAMENX oldkey newkey` |
| `EXPIRE` | `EXPIRE key 60` |
| `PEXPIRE` | `PEXPIRE key 60000` |
| `EXPIREAT` | `EXPIREAT key 1735689600` |
| `PEXPIREAT` | `PEXPIREAT key 1735689600000` |
| `TTL` | `TTL key` |
| `PTTL` | `PTTL key` |
| `PERSIST` | `PERSIST key` |
| `KEYS` | `KEYS user:*` |
| `SCAN` | `SCAN 0 MATCH user:* COUNT 100` |
| `RANDOMKEY` | `RANDOMKEY` |

### Server

| Command | Example |
|---------|---------|
| `PING` | `PING [message]` |
| `ECHO` | `ECHO "hello"` |
| `SELECT` | `SELECT 0` (databases 0-15) |
| `DBSIZE` | `DBSIZE` |
| `FLUSHDB` | `FLUSHDB` |
| `FLUSHALL` | `FLUSHALL` |
| `INFO` | `INFO` |
| `COMMAND` | `COMMAND` |
| `QUIT` | `QUIT` |

## Try It Out

```bash
# Start the server
FALCONDB_PORT=6380 ./target/release/falcondb &

# Basic key-value operations
redis-cli -p 6380 SET greeting "hello world"
redis-cli -p 6380 GET greeting

# Atomic counter
redis-cli -p 6380 SET visits 0
redis-cli -p 6380 INCR visits
redis-cli -p 6380 INCR visits
redis-cli -p 6380 GET visits

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

# Run a benchmark
redis-benchmark -p 6380 -t set,get -n 100000 -c 50 -q

# Stop the server
kill %1
```

## Architecture

FalconDB uses a **shared-nothing, multi-shard** architecture:

```
                    ┌──────────┐
   Clients ───────▶│ Listener │ (tokio multi-thread)
                    └────┬─────┘
                         │ route by key hash
            ┌────────────┼────────────┐
            ▼            ▼            ▼
       ┌─────────┐ ┌─────────┐ ┌─────────┐
       │ Shard 0 │ │ Shard 1 │ │ Shard N │  (each: tokio current_thread)
       │ DbSlice │ │ DbSlice │ │ DbSlice │
       │ DashTbl │ │ DashTbl │ │ DashTbl │
       └─────────┘ └─────────┘ └─────────┘
```

- Each shard runs on its own OS thread with a dedicated tokio single-threaded runtime
- Keys are assigned to shards via CRC32 hash (hash-tags `{tag}` supported)
- No locks on the data path — each shard exclusively owns its data
- Multi-key commands (MGET, MSET, DEL) automatically fan out across shards

## Project Structure

```
crates/
  falcon-core/       # Data structures: DashTable, CompactObj
  falcon-facade/     # Networking: RESP parser, connection handler, TCP listener
  falcon-server/     # Storage engine, command dispatch, sharding, transactions
  falcon-persistence/ # RDB save/load (planned)
  falcon-bin/        # Binary entry point
```

## Building & Testing

```bash
# Build debug
cargo build

# Build release (optimized)
cargo build --release

# Run all tests
cargo test

# Run with debug logging
RUST_LOG=debug FALCONDB_PORT=6380 cargo run --bin falcondb
```

## License

BSL-1.1
