"""
Basic test suite — runs against a live rdb-alpha server.
Start the server first: python server.py
Then: python test_server.py
"""
import socket
import time


def send(sock, *args):
    cmd = f"*{len(args)}\r\n"
    for a in args:
        a = str(a)
        cmd += f"${len(a)}\r\n{a}\r\n"
    sock.sendall(cmd.encode())
    return sock.recv(4096).decode()


def test(label, got, expected):
    ok = expected in got
    print(f"{'OK' if ok else 'FAIL':4}  {label}")
    if not ok:
        print(f"      expected {expected!r}, got {got!r}")
    return ok


def run():
    s = socket.create_connection(("127.0.0.1", 6380), timeout=2)

    passed = failed = 0

    checks = [
        ("PING",           send(s, "PING"),                        "+PONG"),
        ("SET/GET",        send(s, "SET", "k", "hello"),           "+OK"),
        ("GET value",      send(s, "GET", "k"),                    "hello"),
        ("DEL",            send(s, "DEL", "k"),                    ":1"),
        ("GET missing",    send(s, "GET", "k"),                    "$-1"),
        ("INCR",           send(s, "INCR", "counter"),             ":1"),
        ("INCR again",     send(s, "INCR", "counter"),             ":2"),
        ("DECR",           send(s, "DECR", "counter"),             ":1"),
        ("SET EX",         send(s, "SET", "tmp", "bye", "EX", "1"), "+OK"),
        ("TTL positive",   send(s, "TTL", "tmp"),                  ":1"),
        ("EXPIRE",         send(s, "EXPIRE", "tmp", "100"),        ":1"),
        ("TTL after ext",  send(s, "TTL", "tmp"),                  ":"),  # some positive int
        ("EXISTS yes",     send(s, "EXISTS", "tmp"),               ":1"),
        ("EXISTS no",      send(s, "EXISTS", "nope"),              ":0"),
        ("LPUSH",          send(s, "LPUSH", "lst", "a", "b"),      ":2"),
        ("RPUSH",          send(s, "RPUSH", "lst", "c"),           ":3"),
        ("LLEN",           send(s, "LLEN", "lst"),                 ":3"),
        ("LPOP",           send(s, "LPOP", "lst"),                 "b"),
        ("HSET",           send(s, "HSET", "h", "f1", "v1"),       ":1"),
        ("HGET",           send(s, "HGET", "h", "f1"),             "v1"),
        ("HDEL",           send(s, "HDEL", "h", "f1"),             ":1"),
        ("DBSIZE",         send(s, "DBSIZE"),                       ":"),
        ("FLUSHALL",       send(s, "FLUSHALL"),                    "+OK"),
        ("DBSIZE 0",       send(s, "DBSIZE"),                      ":0"),
        ("UNKNOWN cmd",    send(s, "BADCMD"),                      "-ERR"),
    ]

    for label, got, expected in checks:
        if test(label, got, expected):
            passed += 1
        else:
            failed += 1

    s.close()
    print(f"\n{passed}/{passed+failed} tests passed")


if __name__ == "__main__":
    run()
