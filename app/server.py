import asyncio

from parsers import parse_input, IncompleteError
from replication import write_to_slave, slave_read_loop, wait_for_slaves
from globals import slaves, slaves_write_count
from config import server_config
from store import apply_set, get_value, live_keys


async def handle_client(reader, writer):
    """
    Handle an individual client connection.
    """
    buffer = b""
    while True:
        data = await reader.read(1024)
        if not data:
            print("Client disconnected")
            writer.close()
            await writer.wait_closed()
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

            if await handle_command(result, raw, reader, writer):
                # This connection now belongs to a slave; hand it over with any unread bytes
                asyncio.create_task(slave_read_loop(reader, writer, buffer))
                return


async def handle_command(result, raw, reader, writer):
    """
    Execute one parsed command and write the reply.
    'raw' is the command's original bytes, for forwarding to slaves.
    Returns True if the connection has become a slave's replication link.
    """
    cmd = str(result[0]).upper()

    # ECHO
    if cmd == "ECHO":
        writer.write(f"+{result[1]}\r\n".encode())
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
        val = get_value(result[1])
        if val is not None:
            writer.write(f"${len(val)}\r\n{val}\r\n".encode())
        else:
            writer.write(b"$-1\r\n")
        await writer.drain()

    # CONFIG GET
    elif cmd == "CONFIG" and result[1].upper() == "GET":
        if result[2].lower() == "dir":
            d = server_config["dir"] or ""
            writer.write(f"*2\r\n$3\r\ndir\r\n${len(d)}\r\n{d}\r\n".encode())
        elif result[2].lower() == "dbfilename":
            db = server_config["dbfilename"] or ""
            writer.write(f"*2\r\n$10\r\ndbfilename\r\n${len(db)}\r\n{db}\r\n".encode())
        await writer.drain()

    # KEYS
    elif cmd == "KEYS":
        if result[1] == "*":
            keys = live_keys()
            resp = f"*{len(keys)}\r\n"
            for k in keys:
                resp += f"${len(k)}\r\n{k}\r\n"
            writer.write(resp.encode())
            await writer.drain()

    # INFO
    elif cmd == "INFO":
        if server_config["replicaof"]:
            # Slave info
            writer.write(b"$10\r\nrole:slave\r\n")
        else:
            # Master info
            writer.write(
                b"$87\r\nrole:master:master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb:master_repl_offset:0\r\n"
            )
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
        writer.write(b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n")
        await writer.drain()

        # Sample RDB payload
        resp = b"$88\r\n"
        resp += bytes.fromhex(
            "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040"
            "fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e"
            "3bfec0ff5aa2"
        )
        writer.write(resp)
        await writer.drain()
        return True

    # WAIT
    elif cmd == "WAIT":
        await wait_for_slaves(int(result[1]), int(result[2]))
        writer.write(f":{slaves_write_count}\r\n".encode())
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
            asyncio.create_task(connect_to_master(host, int(master_port)))
        await srv.serve_forever()
