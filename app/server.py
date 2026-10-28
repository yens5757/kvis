import asyncio
import fnmatch

from resp import read_value, encode_bulk_string, encode_array
from replication import (
    MASTER_REPLID, connect_to_master, get_repl_offset, start_full_resync, wait_for_slaves,
    write_to_slave,
)
from config import server_config
from store import apply_set, get_value, live_keys


async def handle_client(reader, writer):
    """
    Handle an individual client connection.
    """
    try:
        while True:
            result, raw = await read_value(reader)
            if not isinstance(result, list) or not result:
                continue

            try:
                became_slave = await handle_command(result, raw, reader, writer)
            except (IndexError, ValueError):
                writer.write(f"-ERR wrong arguments for '{result[0]}' command\r\n".encode())
                await writer.drain()
                continue

            if became_slave:
                # The replication code now owns this connection
                return

    except asyncio.IncompleteReadError:
        print("Client disconnected")
    except (ValueError, asyncio.LimitOverrunError) as e:
        writer.write(b"-ERR Protocol error\r\n")
        print(f"Protocol error from client: {e}")
    except ConnectionError:
        print("Client connection lost")

    writer.close()


async def handle_command(result, raw, reader, writer):
    """
    Execute one parsed command and write the reply.
    'raw' is the command's original bytes, for forwarding to slaves.
    Returns True if the connection has become a slave's replication link.
    """
    cmd = str(result[0]).upper()

    # ECHO
    if cmd == "ECHO":
        writer.write(encode_bulk_string(result[1]))
        await writer.drain()

    # PING
    elif cmd == "PING":
        writer.write(b"+PONG\r\n")
        await writer.drain()

    # SET
    elif cmd == "SET":
        apply_set(result)
        await write_to_slave(raw)
        writer.write(b"+OK\r\n")
        await writer.drain()

    # GET
    elif cmd == "GET":
        writer.write(encode_bulk_string(get_value(result[1])))
        await writer.drain()

    # CONFIG GET
    elif cmd == "CONFIG":
        if result[1].upper() != "GET":
            writer.write(f"-ERR unknown subcommand '{result[1]}'\r\n".encode())
        elif result[2].lower() in ("dir", "dbfilename"):
            param = result[2].lower()
            writer.write(encode_array([param, server_config[param] or ""]))
        else:
            writer.write(b"*0\r\n")
        await writer.drain()

    # KEYS
    elif cmd == "KEYS":
        keys = [k for k in live_keys() if fnmatch.fnmatchcase(k, result[1])]
        writer.write(encode_array(keys))
        await writer.drain()

    # INFO
    elif cmd == "INFO":
        if server_config["replicaof"]:
            # Slave info
            info = "role:slave"
        else:
            # Master info
            info = f"role:master\r\nmaster_replid:{MASTER_REPLID}\r\nmaster_repl_offset:{get_repl_offset()}"
        writer.write(encode_bulk_string(info))
        await writer.drain()

    # REPLCONF
    elif cmd == "REPLCONF":
        writer.write(b"+OK\r\n")
        await writer.drain()

    # PSYNC
    elif cmd == "PSYNC":
        await start_full_resync(reader, writer)
        return True

    # WAIT
    elif cmd == "WAIT":
        acked = await wait_for_slaves(int(result[1]), int(result[2]))
        writer.write(f":{acked}\r\n".encode())
        await writer.drain()

    else:
        writer.write(f"-ERR unknown command '{result[0]}'\r\n".encode())
        await writer.drain()

    return False


async def start_server():
    """
    Creates and starts the asyncio server using the configured port.
    """
    port = 6379
    if server_config["port"]:
        port = int(server_config["port"])

    srv = await asyncio.start_server(handle_client, "localhost", port)
    address = srv.sockets[0].getsockname()
    print(f"Server running on {address}")

    async with srv:
        if server_config["replicaof"]:
            host, master_port = server_config["replicaof"].split()
            asyncio.create_task(connect_to_master(host, int(master_port), port))
        await srv.serve_forever()
