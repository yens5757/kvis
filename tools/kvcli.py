"""
Tiny RESP client for testing kvis. Run from the repo root:

  python tools/kvcli.py PING               send one command and print the reply
  python tools/kvcli.py -p 6380 GET name   same, on another port
  python tools/kvcli.py                    interactive prompt (type 'exit' to quit)
  python tools/kvcli.py --raw '*1\\r\\n$4\\r\\nPING\\r\\n' [--chunk 1]
                                           send raw bytes (\\r\\n escapes allowed), optionally a
                                           few bytes at a time, and print everything received
  python tools/kvcli.py --big 8            SET an 8 MB value, GET it back, print timings
"""
import argparse
import codecs
import json
import shlex
import socket
import sys
import time


class Status(str):
    """Simple-string reply, e.g. OK or PONG."""


class Error(str):
    """Error reply."""


def encode(args):
    out = f"*{len(args)}\r\n".encode()
    for arg in args:
        data = str(arg).encode()
        out += b"$" + str(len(data)).encode() + b"\r\n" + data + b"\r\n"
    return out


def read_reply(f):
    line = f.readline()
    if not line:
        raise ConnectionError("server closed the connection")
    kind, body = line[:1], line[1:-2]
    if kind == b"+":
        return Status(body.decode())
    if kind == b"-":
        return Error(body.decode())
    if kind == b":":
        return int(body)
    if kind == b"$":
        n = int(body)
        return None if n == -1 else f.read(n + 2)[:-2].decode()
    if kind == b"*":
        n = int(body)
        return None if n == -1 else [read_reply(f) for _ in range(n)]
    raise ValueError(f"unexpected reply: {line!r}")


def show(reply, indent=""):
    if reply is None:
        return "(nil)"
    if isinstance(reply, Error):
        return f"(error) {reply}"
    if isinstance(reply, Status):
        return str(reply)
    if isinstance(reply, int):
        return f"(integer) {reply}"
    if isinstance(reply, list):
        if not reply:
            return "(empty array)"
        return "\n".join(f"{indent}{i}) {show(item, indent + '   ')}" for i, item in enumerate(reply, 1))
    return json.dumps(reply, ensure_ascii=False)


def command(*args, port=6379, host="localhost"):
    """Send one command on a new connection and return the parsed reply."""
    with socket.create_connection((host, port)) as s:
        s.sendall(encode(args))
        return read_reply(s.makefile("rb"))


def send_raw(data, chunk=0, port=6379, host="localhost"):
    """Send raw bytes (optionally 'chunk' bytes at a time) and return everything received."""
    with socket.create_connection((host, port)) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        step = chunk or len(data)
        for i in range(0, len(data), step):
            s.sendall(data[i:i + step])
            if chunk:
                time.sleep(0.01)
        s.settimeout(0.5)
        received = b""
        try:
            while True:
                d = s.recv(65536)
                if not d:
                    return received, True
                received += d
        except socket.timeout:
            return received, False
        except ConnectionError:
            return received, True


def main():
    p = argparse.ArgumentParser(description="Tiny RESP client for kvis")
    p.add_argument("-p", "--port", type=int, default=6379)
    p.add_argument("--host", default="localhost")
    p.add_argument("--raw", help="raw bytes to send, with \\r\\n style escapes")
    p.add_argument("--chunk", type=int, default=0, help="with --raw: bytes per send")
    p.add_argument("--big", type=int, help="SET and GET a value of this many MB")
    p.add_argument("command", nargs="*")
    a = p.parse_args()

    try:
        run(a)
    except ConnectionRefusedError:
        sys.exit(f"Could not connect to {a.host}:{a.port}. Is the server running?")


def run(a):
    if a.raw is not None:
        received, closed = send_raw(codecs.escape_decode(a.raw.encode())[0], a.chunk, a.port, a.host)
        print(f"received: {received!r}")
        if closed:
            print("(connection closed by server)")
    elif a.big:
        value = "v" * (a.big * 1024 * 1024)
        start = time.time()
        reply = command("SET", "big", value, port=a.port, host=a.host)
        set_time = time.time() - start
        start = time.time()
        intact = command("GET", "big", port=a.port, host=a.host) == value
        print(f"SET {a.big} MB: {show(reply)} in {set_time:.2f} s, "
              f"GET {time.time() - start:.2f} s, value intact: {intact}")
    elif a.command:
        print(show(command(*a.command, port=a.port, host=a.host)))
    else:
        with socket.create_connection((a.host, a.port)) as s:
            f = s.makefile("rb")
            while True:
                try:
                    line = input(f"{a.host}:{a.port}> ")
                except (EOFError, KeyboardInterrupt):
                    break
                if line.strip().lower() in ("exit", "quit"):
                    break
                if line.strip():
                    s.sendall(encode(shlex.split(line)))
                    print(show(read_reply(f)))


if __name__ == "__main__":
    main()
