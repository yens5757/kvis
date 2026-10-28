import asyncio
import fnmatch

from parsers import parse_input, IncompleteError, encode_bulk_string, encode_array
from replication import write_to_slave, slave_read_loop, wait_for_slaves
from globals import slaves, slaves_write_count
from config import server_config
from store import apply_set, get_value, live_keys

MASTER_REPLID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"

# Empty RDB file sent to slaves on a full resync
EMPTY_RDB = bytes.fromhex(
    "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040"
    "fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e"
    "3bfec0ff5aa2"
)


async def handle_client(reader, writer):
    """
    Handle an individual client connection.
    """
    buffer = b""
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                print("Client disconnected")
                break
            buffer += data

            # One read can hold several commands, or only part of one
            while buffer:
                try:
                    result, leftover = parse_input(buffer)
                except IncompleteError:
                    break
                raw = buffer[:len(buffer) - len(leftover)]
                buffer = leftover

                if not isinstance(result, list) or not result:
                    continue

                try:
                    became_slave = await handle_command(result, raw, reader, writer)
                except (IndexError, ValueError):
                    writer.write(f"-ERR wrong arguments for '{result[0]}' command\r\n".encode())
                    await writer.drain()
                    continue

                if became_slave:
                    # This connection now belongs to a slave; hand it over with any unread bytes
                    asyncio.create_task(slave_read_loop(reader, writer, buffer))
                    return

    except ValueError as e:
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
            info = f"role:master\r\nmaster_replid:{MASTER_REPLID}\r\nmaster_repl_offset:0"
        writer.write(encode_bulk_string(info))
        await writer.drain()

    # REPLCONF
    elif cmd == "REPLCONF":
        writer.write(b"+OK\r\n")
        await writer.drain()
        if result[1].lower() == "listening-port" and len(result) > 2:
            slaves[(reader, writer)] = 1

    # PSYNC
    elif cmd == "PSYNC":
        # Full sync scenario
        writer.write(f"+FULLRESYNC {MASTER_REPLID} 0\r\n".encode())
        await writer.drain()

        writer.write(f"${len(EMPTY_RDB)}\r\n".encode() + EMPTY_RDB)
        await writer.drain()
        return True

    # WAIT
    elif cmd == "WAIT":
        await wait_for_slaves(int(result[1]), int(result[2]))
        writer.write(f":{slaves_write_count}\r\n".encode())
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
            from replication import connect_to_master
            host, master_port = server_config["replicaof"].split()
            asyncio.create_task(connect_to_master(host, int(master_port), port))
        await srv.serve_forever()
