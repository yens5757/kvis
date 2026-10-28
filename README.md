# KVIS

A small Redis-compatible key-value server written in Python, using asyncio and only the standard library. It speaks the Redis protocol (RESP) over TCP and supports:

- string keys with optional millisecond expiry
- loading a Redis RDB snapshot at startup
- master/replica replication, with `WAIT` to confirm replicas are up to date

It implements a small subset of Redis. See [Limitations](#limitations).

## Requirements

Python 3.8 or newer (tested on 3.13). No packages to install.

## Quick start

```
git clone https://github.com/yens5757/kvis
cd kvis
python app/main.py
```

The server listens on `localhost:6379`. In a second terminal, use the bundled client:

```
python tools/kvcli.py PING
PONG
python tools/kvcli.py SET greeting hi
OK
python tools/kvcli.py GET greeting
"hi"
```

Run all commands from the repo root. `tools/kvcli.py` is a minimal client included for testing. Official Redis clients haven't been tested; they send setup commands such as `COMMAND` or `CLIENT` that KVIS doesn't implement.

## Command-line options

```
python app/main.py [--port PORT] [--dir DIR] [--dbfilename FILE] [--replicaof "HOST PORT"]
```

| Option | Default | Meaning |
|---|---|---|
| `--port` | `6379` | Port to listen on. The server only accepts connections from this machine. |
| `--dir` | none | Directory containing the RDB file to load at startup. |
| `--dbfilename` | none | RDB file in `--dir` to load at startup, if it exists. The file is only read, never written. |
| `--replicaof` | none | Run as a replica of the master at `HOST PORT`, e.g. `"localhost 6379"`. |

## Commands

| Command | Reply |
|---|---|
| `PING` | `PONG` |
| `ECHO message` | The message |
| `SET key value [PX milliseconds]` | `OK`. With `PX`, the key expires after that many milliseconds. A `SET` without `PX` removes any earlier expiry. No other options (`EX`, `NX`, ...) are supported. |
| `GET key` | The value, or nil if the key is missing or expired |
| `KEYS pattern` | Keys matching a glob pattern (`*`, `?`, `[abc]`), excluding expired keys |
| `CONFIG GET dir` / `CONFIG GET dbfilename` | The setting's name and value. Any other setting returns an empty list. |
| `INFO` | On a master: `role:master`, `master_replid` and `master_repl_offset`. On a replica: `role:slave`. |
| `WAIT numreplicas timeout` | The number of replicas that have received every write so far. Waits up to `timeout` milliseconds (`0` waits forever) for `numreplicas` of them. |
| `REPLCONF`, `PSYNC` | Used by replicas during the replication handshake |

Command names are case-insensitive; keys are case-sensitive. A command with missing or invalid arguments gets an `ERR` reply and the connection stays open. Input that isn't valid RESP gets `ERR Protocol error` and the connection is closed. Values must be valid UTF-8, and a single value can be up to 512 MB.

Expired keys are removed when they are next read (by `GET` or `KEYS`), not in the background.

## Loading an RDB file

KVIS can load a snapshot in Redis's RDB format at startup. It supports string values (including integer-encoded ones), keys with and without expiry in any order, and metadata fields. It stops loading, with a message, at value types it doesn't support (lists, hashes, etc.) or LZF-compressed strings. Keys whose expiry time has already passed are treated as missing.

To try it, `tools/make_rdb.py` writes a small test file:

```
python tools/make_rdb.py data/dump.rdb
python app/main.py --dir data --dbfilename dump.rdb
python tools/kvcli.py KEYS '*'
```

KVIS never writes RDB files: all data is lost when the server stops.

## Replication

Start a master and one or more replicas:

```
python app/main.py
python app/main.py --port 6380 --replicaof "localhost 6379"
```

The replica connects to the master and performs the handshake (`PING`, `REPLCONF`, `PSYNC`). From then on, every `SET` on the master is forwarded to it:

```
python tools/kvcli.py SET user alice
python tools/kvcli.py -p 6380 GET user
"alice"
python tools/kvcli.py WAIT 1 1000
(integer) 1
```

`WAIT` asks the replicas how much of the replication stream they have processed (`REPLCONF GETACK`) and counts those that have every write so far.

Note that a replica only receives writes made **after** it connects. The master sends an empty snapshot during the handshake, not its current data.

## Testing tools

`tools/kvcli.py` is a small RESP client:

| Usage | What it does |
|---|---|
| `python tools/kvcli.py GET key` | Send one command and print the reply |
| `python tools/kvcli.py -p 6380 GET key` | Same, against another port |
| `python tools/kvcli.py` | Interactive prompt; type `exit` to quit |
| `python tools/kvcli.py --raw '*1\r\n$4\r\nPING\r\n'` | Send raw protocol bytes and print everything received |
| `python tools/kvcli.py --raw '...' --chunk 1` | Same, sending one byte at a time to test split network reads |
| `python tools/kvcli.py --big 8` | `SET` an 8 MB value, read it back, and print the timings |

`tools/make_rdb.py` writes a test RDB file containing a plain key, an integer-encoded value, a key that expires in an hour, an already-expired key, and a key longer than 64 bytes.

Quote arguments that contain `*`, `?`, `$` or `\` with single quotes, as above. That works in both PowerShell and bash.

## Project layout

| File | Purpose |
|---|---|
| `app/main.py` | Entry point: parses options, loads the RDB file, starts the server |
| `app/config.py` | Settings from the command line |
| `app/server.py` | TCP server, per-connection loop, command handling |
| `app/resp.py` | Reading and writing the Redis protocol |
| `app/store.py` | The in-memory data and key expiry |
| `app/rdb.py` | RDB file parsing |
| `app/replication.py` | Master side (replicas, offsets, `PSYNC`, `WAIT`) and replica side (connecting to a master) |
| `tools/` | Test client and RDB file generator |

Everything runs on a single asyncio event loop, so commands never run concurrently and the store needs no locks.

## Limitations

- Only string values, and only the commands listed above.
- No saving to disk; RDB files are only loaded.
- A replica doesn't receive data that existed on the master before it connected.
- Replicas accept writes from clients, which are not sent back to the master.
- A replica doesn't reconnect if its master goes away, and there is no partial resynchronisation.
- No authentication; the server only listens on `localhost`.
- Values must be valid UTF-8 text.

## License

MIT. See [LICENSE](LICENSE).
