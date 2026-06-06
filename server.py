"""
rdb-alpha — a Redis-protocol-compatible in-memory key-value store.

Implements a subset of the RESP3 protocol so it works with redis-cli
and any Redis client library out of the box.

Supported commands:
  PING, ECHO
  SET key value [EX seconds]
  GET key
  DEL key [key ...]
  EXISTS key [key ...]
  EXPIRE key seconds
  TTL key
  KEYS pattern
  INCR key / DECR key
  LPUSH key value / RPUSH key value
  LPOP key / RPOP key
  LLEN key / LRANGE key start stop
  HSET key field value / HGET key field / HGETALL key / HDEL key field
  FLUSHALL
  DBSIZE
  SAVE (persist snapshot to disk)
  INFO

Usage:
  python server.py [--host 127.0.0.1] [--port 6380] [--dump rdb.json]
"""
from __future__ import annotations

import argparse
import asyncio
import fnmatch
import json
import logging
import signal
import time
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("rdb")

# ── In-memory store ────────────────────────────────────────────────────────────

class Store:
    def __init__(self):
        self._data: dict[str, Any] = {}
        self._expiry: dict[str, float] = {}  # key → unix timestamp when it expires

    def _expired(self, key: str) -> bool:
        exp = self._expiry.get(key)
        if exp is None:
            return False
        if time.time() > exp:
            del self._data[key]
            del self._expiry[key]
            return True
        return False

    def get(self, key: str) -> Any:
        if self._expired(key):
            return None
        return self._data.get(key)

    def set(self, key: str, value: Any, ex: int | None = None) -> None:
        self._data[key] = value
        if ex is not None:
            self._expiry[key] = time.time() + ex
        elif key in self._expiry:
            del self._expiry[key]

    def delete(self, *keys: str) -> int:
        count = 0
        for k in keys:
            if k in self._data and not self._expired(k):
                del self._data[k]
                self._expiry.pop(k, None)
                count += 1
        return count

    def exists(self, *keys: str) -> int:
        return sum(1 for k in keys if not self._expired(k) and k in self._data)

    def expire(self, key: str, seconds: int) -> int:
        if self._expired(key) or key not in self._data:
            return 0
        self._expiry[key] = time.time() + seconds
        return 1

    def ttl(self, key: str) -> int:
        if self._expired(key) or key not in self._data:
            return -2
        exp = self._expiry.get(key)
        if exp is None:
            return -1
        remaining = int(exp - time.time())
        return max(0, remaining)

    def keys(self, pattern: str = "*") -> list[str]:
        # Evict expired keys first
        for k in list(self._data):
            self._expired(k)
        return [k for k in self._data if fnmatch.fnmatch(k, pattern)]

    def dbsize(self) -> int:
        for k in list(self._data):
            self._expired(k)
        return len(self._data)

    def flushall(self) -> None:
        self._data.clear()
        self._expiry.clear()

    def snapshot(self) -> dict:
        now = time.time()
        return {
            "data": {k: v for k, v in self._data.items() if not self._expired(k)},
            "expiry": {k: v for k, v in self._expiry.items() if v > now},
        }

    def load(self, snap: dict) -> None:
        self._data = snap.get("data", {})
        self._expiry = snap.get("expiry", {})


store = Store()
_start_time = time.time()
_commands_processed = 0

# ── RESP protocol parser / writer ──────────────────────────────────────────────

def encode(value: Any) -> bytes:
    """Encode a Python value as RESP3."""
    if value is None:
        return b"$-1\r\n"
    if isinstance(value, bool):
        return b"+OK\r\n" if value else b"-ERR\r\n"
    if isinstance(value, int):
        return f":{value}\r\n".encode()
    if isinstance(value, str):
        encoded = value.encode()
        return f"${len(encoded)}\r\n".encode() + encoded + b"\r\n"
    if isinstance(value, bytes):
        return f"${len(value)}\r\n".encode() + value + b"\r\n"
    if isinstance(value, list):
        parts = [f"*{len(value)}\r\n".encode()]
        for item in value:
            parts.append(encode(item))
        return b"".join(parts)
    if isinstance(value, Exception):
        return f"-ERR {value}\r\n".encode()
    return encode(str(value))


def encode_ok() -> bytes:
    return b"+OK\r\n"


def encode_err(msg: str) -> bytes:
    return f"-ERR {msg}\r\n".encode()


class RespParser:
    """Incremental RESP parser — handles pipelining."""

    def __init__(self):
        self._buf = b""

    def feed(self, data: bytes) -> list[list[str]]:
        self._buf += data
        commands = []
        while self._buf:
            cmd, self._buf = self._parse_one(self._buf)
            if cmd is None:
                break
            commands.append(cmd)
        return commands

    def _parse_one(self, buf: bytes):
        if not buf:
            return None, buf
        if buf[0:1] == b"*":
            end = buf.find(b"\r\n")
            if end == -1:
                return None, buf
            count = int(buf[1:end])
            buf = buf[end + 2:]
            args = []
            for _ in range(count):
                arg, buf = self._read_bulk(buf)
                if arg is None:
                    return None, self._buf
                args.append(arg)
            return args, buf
        # Inline command
        end = buf.find(b"\r\n")
        if end == -1:
            return None, buf
        line = buf[:end].decode(errors="replace")
        return line.split(), buf[end + 2:]

    def _read_bulk(self, buf: bytes):
        if not buf or buf[0:1] != b"$":
            return None, buf
        end = buf.find(b"\r\n")
        if end == -1:
            return None, buf
        length = int(buf[1:end])
        if length > 512 * 1024 * 1024:
            raise ValueError("value too large (max 512 MB)")
        buf = buf[end + 2:]
        if len(buf) < length + 2:
            return None, buf
        return buf[:length].decode(errors="replace"), buf[length + 2:]


# ── Command dispatch ───────────────────────────────────────────────────────────

def handle(args: list[str], dump_path: str | None) -> bytes:
    global _commands_processed
    _commands_processed += 1

    if not args:
        return encode_err("empty command")

    cmd = args[0].upper()

    try:
        if cmd == "PING":
            return b"+PONG\r\n" if len(args) == 1 else encode(args[1])

        if cmd == "ECHO":
            return encode(args[1]) if len(args) > 1 else encode_err("wrong number of arguments")

        if cmd == "SET":
            if len(args) < 3:
                return encode_err("wrong number of arguments for SET")
            ex = None
            if len(args) >= 5 and args[3].upper() == "EX":
                ex = int(args[4])
            store.set(args[1], args[2], ex=ex)
            return encode_ok()

        if cmd == "GET":
            if len(args) != 2:
                return encode_err("wrong number of arguments for GET")
            val = store.get(args[1])
            return encode(val)

        if cmd == "DEL":
            if len(args) < 2:
                return encode_err("wrong number of arguments for DEL")
            return encode(store.delete(*args[1:]))

        if cmd == "EXISTS":
            if len(args) < 2:
                return encode_err("wrong number of arguments for EXISTS")
            return encode(store.exists(*args[1:]))

        if cmd == "EXPIRE":
            if len(args) != 3:
                return encode_err("wrong number of arguments for EXPIRE")
            return encode(store.expire(args[1], int(args[2])))

        if cmd == "TTL":
            if len(args) != 2:
                return encode_err("wrong number of arguments for TTL")
            return encode(store.ttl(args[1]))

        if cmd == "KEYS":
            pattern = args[1] if len(args) > 1 else "*"
            return encode(store.keys(pattern))

        if cmd == "INCR":
            val = store.get(args[1])
            if val is not None and not isinstance(val, str):
                return encode_err("value is not an integer or out of range")
            try:
                n = (int(val) if val is not None else 0) + 1
            except ValueError:
                return encode_err("value is not an integer or out of range")
            store.set(args[1], str(n))
            return encode(n)

        if cmd == "DECR":
            val = store.get(args[1])
            if val is not None and not isinstance(val, str):
                return encode_err("value is not an integer or out of range")
            try:
                n = (int(val) if val is not None else 0) - 1
            except ValueError:
                return encode_err("value is not an integer or out of range")
            store.set(args[1], str(n))
            return encode(n)

        if cmd in ("LPUSH", "RPUSH"):
            if len(args) < 3:
                return encode_err(f"wrong number of arguments for {cmd}")
            lst = store.get(args[1]) or []
            if not isinstance(lst, list):
                return encode_err("WRONGTYPE key holds a non-list value")
            for v in args[2:]:
                if cmd == "LPUSH":
                    lst.insert(0, v)
                else:
                    lst.append(v)
            store.set(args[1], lst)
            return encode(len(lst))

        if cmd in ("LPOP", "RPOP"):
            lst = store.get(args[1])
            if lst is None:
                return encode(None)
            if not isinstance(lst, list):
                return encode_err("WRONGTYPE key holds a non-list value")
            val = lst.pop(0) if cmd == "LPOP" else lst.pop()
            store.set(args[1], lst)
            return encode(val)

        if cmd == "LLEN":
            lst = store.get(args[1])
            if lst is None:
                return encode(0)
            if not isinstance(lst, list):
                return encode_err("WRONGTYPE Operation against a key holding the wrong kind of value")
            return encode(len(lst))

        if cmd == "LRANGE":
            if len(args) != 4:
                return encode_err("wrong number of arguments for LRANGE")
            lst = store.get(args[1])
            if lst is None:
                return encode([])
            if not isinstance(lst, list):
                return encode_err("WRONGTYPE Operation against a key holding the wrong kind of value")
            length = len(lst)
            start, stop = int(args[2]), int(args[3])
            # Redis negative-index semantics: -1 = last element, -2 = second-to-last, etc.
            if start < 0:
                start = max(0, length + start)
            if stop < 0:
                stop = length + stop
            if start > stop or start >= length:
                return encode([])
            return encode(lst[start:stop + 1])

        if cmd == "HSET":
            if len(args) < 4 or len(args) % 2 != 0:
                return encode_err("wrong number of arguments for HSET")
            h = store.get(args[1]) or {}
            if not isinstance(h, dict):
                return encode_err("WRONGTYPE key holds a non-hash value")
            count = 0
            for i in range(2, len(args), 2):
                if args[i] not in h:
                    count += 1
                h[args[i]] = args[i + 1]
            store.set(args[1], h)
            return encode(count)

        if cmd == "HGET":
            h = store.get(args[1])
            if h is None:
                return encode(None)
            if not isinstance(h, dict):
                return encode_err("WRONGTYPE key holds a non-hash value")
            return encode(h.get(args[2]))

        if cmd == "HGETALL":
            h = store.get(args[1])
            if h is None:
                return encode([])
            if not isinstance(h, dict):
                return encode_err("WRONGTYPE Operation against a key holding the wrong kind of value")
            flat = []
            for k, v in h.items():
                flat.extend([k, v])
            return encode(flat)

        if cmd == "HDEL":
            h = store.get(args[1]) or {}
            count = sum(1 for f in args[2:] if h.pop(f, None) is not None)
            store.set(args[1], h)
            return encode(count)

        if cmd == "FLUSHALL":
            store.flushall()
            return encode_ok()

        if cmd == "DBSIZE":
            return encode(store.dbsize())

        if cmd == "SAVE":
            if dump_path:
                _persist(dump_path)
                return encode_ok()
            return encode_err("no dump path configured")

        if cmd == "INFO":
            uptime = int(time.time() - _start_time)
            info = (
                f"# Server\r\nrdb_version:0.1.0\r\nuptime_in_seconds:{uptime}\r\n"
                f"# Stats\r\ntotal_commands_processed:{_commands_processed}\r\n"
                f"# Keyspace\r\ndb0:keys={store.dbsize()}\r\n"
            )
            return encode(info)

        return encode_err(f"unknown command '{cmd}'")

    except (ValueError, IndexError) as e:
        return encode_err(str(e))


# ── Persistence ────────────────────────────────────────────────────────────────

def _persist(path: str) -> None:
    snap = store.snapshot()
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(snap, f)
    import os
    os.replace(tmp, path)
    log.info("Saved snapshot to %s (%d keys)", path, len(snap["data"]))


def _load(path: str) -> None:
    try:
        with open(path) as f:
            snap = json.load(f)
        store.load(snap)
        log.info("Loaded snapshot from %s (%d keys)", path, len(snap.get("data", {})))
    except FileNotFoundError:
        log.info("No snapshot found at %s — starting fresh", path)


# ── Async TCP server ───────────────────────────────────────────────────────────

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, dump_path: str | None):
    addr = writer.get_extra_info("peername")
    log.info("Client connected: %s", addr)
    parser = RespParser()
    try:
        while True:
            data = await reader.read(4096)
            if not data:
                break
            for cmd_args in parser.feed(data):
                response = handle(cmd_args, dump_path)
                writer.write(response)
            await writer.drain()
    except (ConnectionResetError, asyncio.IncompleteReadError):
        pass
    finally:
        log.info("Client disconnected: %s", addr)
        writer.close()


async def main(host: str, port: int, dump_path: str | None):
    if dump_path:
        _load(dump_path)

    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, dump_path),
        host, port,
    )

    loop = asyncio.get_running_loop()
    if dump_path:
        # Auto-save every 60 seconds
        async def auto_save():
            while True:
                await asyncio.sleep(60)
                _persist(dump_path)
        loop.create_task(auto_save())

    # Background expiry sweep — purge keys that expired but were never accessed
    async def expiry_sweep():
        while True:
            await asyncio.sleep(10)
            for k in list(store._data):
                store._expired(k)
    loop.create_task(expiry_sweep())

    def shutdown():
        log.info("Shutting down...")
        if dump_path:
            _persist(dump_path)
        server.close()

    loop.add_signal_handler(signal.SIGINT, shutdown)
    loop.add_signal_handler(signal.SIGTERM, shutdown)

    log.info("rdb-alpha listening on %s:%d", host, port)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="rdb-alpha: in-memory key-value store")
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6380)
    ap.add_argument("--dump", default="rdb.json", help="Snapshot file path (set to empty string to disable)")
    args = ap.parse_args()
    asyncio.run(main(args.host, args.port, args.dump or None))
