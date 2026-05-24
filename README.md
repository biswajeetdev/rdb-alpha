# rdb-alpha

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![asyncio](https://img.shields.io/badge/asyncio-3776AB?style=flat&logo=python&logoColor=white)
![Redis Protocol](https://img.shields.io/badge/RESP3-DC382D?style=flat&logo=redis&logoColor=white)

A Redis-protocol-compatible in-memory key-value store built from scratch in Python — no dependencies beyond the standard library.

Works with `redis-cli`, any Redis client library, and the included test suite.

## Supported commands

| Category | Commands |
|---|---|
| Basic | `PING`, `ECHO`, `SET [EX]`, `GET`, `DEL`, `EXISTS` |
| Expiry | `EXPIRE`, `TTL` |
| Counters | `INCR`, `DECR` |
| Lists | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE` |
| Hashes | `HSET`, `HGET`, `HGETALL`, `HDEL` |
| Admin | `KEYS`, `DBSIZE`, `FLUSHALL`, `SAVE`, `INFO` |

## Usage

```bash
# Start the server (default: 127.0.0.1:6380)
python server.py

# Custom host/port
python server.py --host 0.0.0.0 --port 6380

# Disable persistence
python server.py --dump ""

# Connect with redis-cli
redis-cli -p 6380 PING
redis-cli -p 6380 SET name "Biswajeet" EX 3600
redis-cli -p 6380 GET name
redis-cli -p 6380 KEYS "*"
```

## Run tests

```bash
# In one terminal
python server.py

# In another
python test_server.py
```

## How it works

- **RESP3 protocol** — the same wire format Redis uses; any Redis client connects unmodified
- **Passive expiry** — keys are evicted lazily on access, O(1) per operation
- **JSON snapshots** — `SAVE` writes an atomic snapshot; auto-save runs every 60s
- **Async TCP** — `asyncio.start_server` handles concurrent clients on a single thread
- **Zero dependencies** — stdlib only (`asyncio`, `json`, `fnmatch`, `signal`)

## Design decisions

**Why RESP3 over a custom protocol?** Compatibility — any Redis client in any language works without modification.

**Why passive expiry?** O(1) per access, no background scan loop. Same trade-off Redis makes.

**Why JSON for persistence?** Human-readable and dependency-free. A natural extension is a compact binary format (like Redis RDB) for production use.
