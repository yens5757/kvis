import asyncio
import time


from globals import global_hashmap, expiry_hashmap, slaves, slaves_write_count
from config import server_config
from parsers import parse_input
from replication import write_to_slave, slave_read_loop, wait_for_slaves

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

        try:
            result, _ = parse_input(data)
        except ValueError as ve:
            # If parse fails, send an error
            writer.write(f"-ERR parse error: {ve}\r\n".encode())
            await writer.drain()
            continue

        if not result:
            # Could be partial read or unknown data
            writer.write(b"-ERR empty or invalid command\r\n")
            await writer.drain()
            continue

        # If the parsed result is a single string or error dict, it might not be a command array
        if isinstance(result, dict) and "error" in result:
            writer.write(f"-ERR {result['error']}\r\n".encode())
            await writer.drain()
            continue
        if isinstance(result, str):
            writer.write(f"-ERR unknown input: {result}\r\n".encode())
            await writer.drain()
            continue

        if len(result) == 0:
            writer.write(b"-ERR empty command array\r\n")
            await writer.drain()
            continue

        cmd = result[0].upper()

        # ECHO
        if cmd == "ECHO" and len(result) >= 2:
            writer.write(f"+{result[1]}\r\n".encode())
            await writer.drain()

        # PING
        elif cmd == "PING":
            writer.write(b"+PONG\r\n")
            await writer.drain()

        # SET
        elif cmd == "SET" and len(result) >= 3:
            key = result[1]
            value = result[2]
            await write_to_slave(data)  # replicate
            global_hashmap[key] = value
            writer.write(b"+OK\r\n")
            await writer.drain()

            # check for px argument
            if len(result) > 3 and result[3].lower() == "px":
                if len(result) > 4:
                    # set expiry
                    try:
                        px_time = int(result[4])
                        expiry_hashmap[key] = time.time() + (px_time / 1000)
                    except ValueError:
                        pass

        # GET
        elif cmd == "GET" and len(result) >= 2:
            key = result[1]
            if key in global_hashmap:
                now = time.time()
                if key not in expiry_hashmap or expiry_hashmap[key] > now:
                    val = global_hashmap[key]
                    writer.write(f"${len(val)}\r\n{val}\r\n".encode())
                    await writer.drain()
                else:
                    # expired
                    writer.write(b"$-1\r\n")
                    await writer.drain()
            else:
                writer.write(b"$-1\r\n")
                await writer.drain()

        # CONFIG GET
        elif cmd == "CONFIG" and len(result) >= 3 and result[1].upper() == "GET":
            param = result[2].lower()
            if param == "dir":
                dir_str = server_config["dir"] or ""
                resp = f"*2\r\n$3\r\ndir\r\n${len(dir_str)}\r\n{dir_str}\r\n"
                writer.write(resp.encode())
            elif param == "dbfilename":
                dbf_str = server_config["dbfilename"] or ""
                resp = f"*2\r\n$10\r\ndbfilename\r\n${len(dbf_str)}\r\n{dbf_str}\r\n"
                writer.write(resp.encode())
            else:
                # If they ask for something else, return empty array
                writer.write(b"*0\r\n")
            await writer.drain()

        # KEYS
        elif cmd == "KEYS" and len(result) >= 2:
            pattern = result[1]
            if pattern == "*":
                keys = list(global_hashmap.keys())
                resp = f"*{len(keys)}\r\n"
                for k in keys:
                    resp += f"${len(k)}\r\n{k}\r\n"
                writer.write(resp.encode())
            else:
                # Not implementing advanced pattern matching, just return empty
                writer.write(b"*0\r\n")
            await writer.drain()

        # INFO
        elif cmd == "INFO":
            if server_config["replicaof"]:
                writer.write(b"$10\r\nrole:slave\r\n")
            else:
                writer.write(b"$87\r\nrole:master:master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb:master_repl_offset:0\r\n")
            await writer.drain()

        # REPLCONF
        elif cmd == "REPLCONF":
            writer.write(b"+OK\r\n")
            await writer.drain()
            # If it's 'listening-port', store the slave
            if len(result) >= 3 and result[1].lower() == "listening-port":
                slaves[(reader, writer)] = 1

        # PSYNC
        elif cmd == "PSYNC":
            # respond with FULLRESYNC
            writer.write(b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n")
            await writer.drain()
            # in your code, you send some sort of RDB bytes
            rdb_mock = b"$88\r\n" + bytes.fromhex("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
            writer.write(rdb_mock)
            await writer.drain()
            # spawn a dedicated task to handle further REPLCONF from this slave
            asyncio.create_task(slave_read_loop(reader, writer))
            # we exit handle_client so we don't keep reading normal commands
            break

        # WAIT
        elif cmd == "WAIT" and len(result) >= 3:
            try:
                num_slaves = int(result[1])
                timeout_ms = int(result[2])
                await wait_for_slaves(num_slaves, timeout_ms)
                writer.write(f":{slaves_write_count}\r\n".encode())
                await writer.drain()
            except ValueError:
                writer.write(b":0\r\n")
                await writer.drain()

        else:
            writer.write(b"-ERR unknown command\r\n")
            await writer.drain()


async def main():
    """
    The main async entrypoint. Starts the server.
    """
    port = 6379
    if server_config["port"]:
        port = int(server_config["port"])

    server = await asyncio.start_server(handle_client, "localhost", port)
    address = server.sockets[0].getsockname()
    print(f"Server running on {address}")

    # If configured as a replica, also connect to master
    if server_config["replicaof"]:
        host, port_str = server_config["replicaof"].split()

    async with server:
        await server.serve_forever()
