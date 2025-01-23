import asyncio
import time

from parsers import parse_input
from replication import write_to_slave, slave_read_loop, wait_for_slaves
from globals import global_hashmap, expiry_hashmap, slaves, server_config, slaves_write_count


async def handle_client(reader, writer):
    """
    Handle an individual client connection.
    """
    while True:
        data = await reader.read(1024)
        if not data:
            print("Client disconnected")
            writer.close()
            await writer.wait_closed()
            break

        result, _ = parse_input(data)
        if not result:
            continue

        cmd = result[0].upper()

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
            await write_to_slave(data)
            global_hashmap[result[1]] = result[2]
            writer.write(b"+OK\r\n")
            await writer.drain()

            # If expiration is set
            if len(result) > 3 and result[3].lower() == "px" and result[1] in global_hashmap:
                expiry_hashmap[result[1]] = time.time() + (int(result[4]) / 1000)

        # GET
        elif cmd == "GET":
            if result[1] in global_hashmap:
                now = time.time()
                if (result[1] not in expiry_hashmap) or (expiry_hashmap[result[1]] > now):
                    val = global_hashmap[result[1]]
                    writer.write(f"${len(val)}\r\n{val}\r\n".encode())
                else:
                    # Key is expired
                    writer.write(b"$-1\r\n")
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
                keys = list(global_hashmap.keys())
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

            asyncio.create_task(slave_read_loop(reader, writer))
            break

        # WAIT
        elif cmd == "WAIT":
            await wait_for_slaves(int(result[1]), int(result[2]))
            writer.write(f":{slaves_write_count}\r\n".encode())
            await writer.drain()


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
