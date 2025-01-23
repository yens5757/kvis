import asyncio
import time

from config import server_config
from globals import global_hashmap, expiry_hashmap, slaves, slaves_write_count
from parsers import parse_input
from replication import write_to_slave, slave_read_loop, wait_for_slaves
from rdb import parse_metadata

async def handle_client(reader, writer):
    """
    Handle an individual client connection.
    """
    while True:
        data = await reader.read(1024)
        if not data:
            print("[server] Client disconnected.")
            writer.close()
            await writer.wait_closed()
            break

        try:
            result, _ = parse_input(data)
        except ValueError as ve:
            writer.write(f"-ERR parse error: {ve}\r\n".encode())
            await writer.drain()
            continue

        if not result:
            writer.write(b"-ERR empty or invalid command\r\n")
            await writer.drain()
            continue

        # If the parsed result is a single string or an error dict:
        if isinstance(result, dict) and "error" in result:
            writer.write(f"-ERR {result['error']}\r\n".encode())
            await writer.drain()
            continue
        if isinstance(result, str):
            writer.write(f"-ERR unknown input: {result}\r\n".encode())
            await writer.drain()
            continue

        # We assume 'result' is a list: [COMMAND, ARGS...]
        if len(result) == 0:
            writer.write(b"-ERR empty command array\r\n")
            await writer.drain()
            continue

        cmd = result[0].upper()

        if cmd == "ECHO" and len(result) >= 2:
            writer.write(f"+{result[1]}\r\n".encode())
            await writer.drain()

        elif cmd == "PING":
            writer.write(b"+PONG\r\n")
            await writer.drain()

        elif cmd == "SET" and len(result) >= 3:
            key = result[1]
            value = result[2]
            # replicate to slaves
            await write_to_slave(data)
            global_hashmap[key] = value
            writer.write(b"+OK\r\n")
            await writer.drain()

            # if there's a 'px' argument, handle expiry
            if len(result) >= 5 and result[3].lower() == "px":
                try:
                    px_ms = int(result[4])
                    expiry_hashmap[key] = time.time() + (px_ms / 1000.0)
                except ValueError:
                    pass

        elif cmd == "GET" and len(result) >= 2:
            key = result[1]
            now = time.time()
            if key in global_hashmap:
                if key not in expiry_hashmap or expiry_hashmap[key] > now:
                    val = global_hashmap[key]
                    writer.write(f"${len(val)}\r\n{val}\r\n".encode())
                else:
                    # expired
                    del global_hashmap[key]
                    del expiry_hashmap[key]
                    writer.write(b"$-1\r\n")
            else:
                writer.write(b"$-1\r\n")
            await writer.drain()

        elif cmd == "CONFIG" and len(result) >= 3 and result[1].upper() == "GET":
            param = result[2].lower()
            if param == "dir":
                dir_val = server_config["dir"] or ""
                resp = f"*2\r\n$3\r\ndir\r\n${len(dir_val)}\r\n{dir_val}\r\n"
                writer.write(resp.encode())
            elif param == "dbfilename":
                dbf_val = server_config["dbfilename"] or ""
                resp = f"*2\r\n$10\r\ndbfilename\r\n${len(dbf_val)}\r\n{dbf_val}\r\n"
                writer.write(resp.encode())
            else:
                writer.write(b"*0\r\n")
            await writer.drain()

        elif cmd == "KEYS" and len(result) >= 2:
            pattern = result[1]
            if pattern == "*":
                keys = list(global_hashmap.keys())
                resp = f"*{len(keys)}\r\n"
                for k in keys:
                    resp += f"${len(k)}\r\n{k}\r\n"
                writer.write(resp.encode())
            else:
                writer.write(b"*0\r\n")
            await writer.drain()

        elif cmd == "INFO":
            if server_config["replicaof"]:
                writer.write(b"$10\r\nrole:slave\r\n")
            else:
                writer.write(b"$87\r\nrole:master:master_replid:8371b4fb1155b71f4a0:master_repl_offset:0\r\n")
            await writer.drain()

        elif cmd == "REPLCONF":
            writer.write(b"+OK\r\n")
            await writer.drain()
            # If it's 'listening-port', store the slave
            if len(result) >= 3 and result[1].lower() == "listening-port":
                slaves[(reader, writer)] = 1

        elif cmd == "PSYNC":
            # Respond with FULLRESYNC
            writer.write(b"+FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0\r\n")
            await writer.drain()
            rdb_mock = (b"$88\r\n" + bytes.fromhex("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"))
            writer.write(rdb_mock)
            await writer.drain()
            # spawn a dedicated task to handle further REPLCONF from this slave
            asyncio.create_task(slave_read_loop(reader, writer))
            break

        elif cmd == "WAIT" and len(result) >= 3:
            try:
                num_slaves = int(result[1])
                timeout_ms = int(result[2])
                await wait_for_slaves(num_slaves, timeout_ms)
                writer.write(f":{slaves_write_count}\r\n".encode())
            except ValueError:
                writer.write(b":0\r\n")
            await writer.drain()

        else:
            writer.write(b"-ERR unknown command\r\n")
            await writer.drain()


async def connect_to_master(host, port):
    """
    If configured as a replica, connect to the master and do a PSYNC handshake.
    Then get the data dump from master(optional). Then simply process any other data that sent by master.
    """
    print(f"[Replica] Attempting to connect to master at {host}:{port}")
    try:
        reader, writer = await asyncio.open_connection(host, port)

        # 1) Basic handshake: send PING
        writer.write(b"*1\r\n$4\r\nPING\r\n")
        await writer.drain()
        response = await reader.read(1024)
        data, _ = parse_input(response)
        if data != "PONG":
            print("[Replica] Master did not respond PONG; aborting.")
            writer.close()
            await writer.wait_closed()
            return

        # 2) REPLCONF listening-port
        local_port = server_config["port"] or "6379"
        replconf_listening = (f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n${len(local_port)}\r\n{local_port}\r\n").encode()
        writer.write(replconf_listening)
        await writer.drain()
        response = await reader.read(1024)
        data, _ = parse_input(response)
        if data != "OK":
            print("[Replica] Master didn't accept REPLCONF listening-port.")
            writer.close()
            await writer.wait_closed()
            return

        # 3) REPLCONF capa psync2
        writer.write(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
        await writer.drain()
        response = await reader.read(1024)
        data, _ = parse_input(response)
        if data != "OK":
            print("[Replica] Master didn't accept REPLCONF capa psync2.")
            writer.close()
            await writer.wait_closed()
            return

        # 4) PSYNC ? -1
        writer.write(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        await writer.drain()

        # 5) Read initial data from master. Expect +FULLRESYNC ... + possible RDB chunk
        rdb_chunk = await reader.read(4096)
        if not rdb_chunk:
            print("[Replica] No data received after PSYNC request.")
            writer.close()
            await writer.wait_closed()
            return

        # Look for the "REDIS" header in rdb_chunk
        redis_header_index = rdb_chunk.find(b"REDIS")
        if redis_header_index == -1:
            print("[Replica] No REDIS magic found in the initial chunk.")
            writer.close()
            await writer.wait_closed()
            return

        if redis_header_index + 9 > len(rdb_chunk):
            print("[Replica] Not enough data for REDISxxxx header.")
            writer.close()
            await writer.wait_closed()
            return

        header = rdb_chunk[redis_header_index : redis_header_index + 9]
        magic, version = header[:5].decode("ascii"), header[5:].decode("ascii")
        if magic != "REDIS" or not version.isdigit():
            print("[Replica] The chunk's header is invalid.")
            writer.close()
            await writer.wait_closed()
            return

        print(f"[Replica] Detected RDB header: {magic}{version}")
        # parse the body with parse_metadata
        rdb_body = rdb_chunk[redis_header_index + 9:]
        leftover = parse_metadata(rdb_body)
        if leftover:
            print(f"[Replica] {len(leftover)} leftover bytes after parse_metadata.")

        # Keep reading further commands from master.
        get_ack_offset = len(rdb_chunk)
        while True:
            new_data = await reader.read(1024)
            if not new_data:
                print("[Replica] Master closed connection.")
                writer.close()
                await writer.wait_closed()
                break

            get_ack_offset += len(new_data)
            leftover2 = new_data
            while leftover2:
                parsed, leftover2 = parse_input(leftover2)
                if not parsed:
                    break
                if isinstance(parsed, list) and len(parsed) > 0:
                    sub_cmd = parsed[0].upper()
                    if sub_cmd == "REPLCONF" and len(parsed) >= 2 and parsed[1].upper() == "ACK":
                        ack_str = (
                            f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n"
                            f"${len(str(get_ack_offset))}\r\n{get_ack_offset}\r\n"
                        )
                        writer.write(ack_str.encode())
                        await writer.drain()
                    elif sub_cmd == "SET" and len(parsed) >= 3:
                        # "SET key value px <ms>"
                        key, value = parsed[1], parsed[2]
                        global_hashmap[key] = value
                        if len(parsed) >= 5 and parsed[3].lower() == "px":
                            px_ms = int(parsed[4])
                            expiry_hashmap[key] = time.time() + px_ms / 1000.0
                        print(f"[Replica] SET {key} => {value}")

    except Exception as e:
        print(f"[Replica] Error in replication: {e}")

async def main():
    """
    Starts the server, and if we have a replica config, starts replication.
    """
    port = int(server_config["port"] or 6379)
    server = await asyncio.start_server(handle_client, "localhost", port)
    address = server.sockets[0].getsockname()
    print(f"[server] Running on {address}")

    # If we are a replica of some master, connect
    if server_config["replicaof"]:
        parts = server_config["replicaof"].split()
        if len(parts) == 2:
            host, master_port = parts[0], int(parts[1])
            asyncio.create_task(connect_to_master(host, master_port))
        else:
            print("[server] Invalid --replicaof. Must be 'host port'")

    async with server:
        await server.serve_forever()
