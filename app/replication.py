import asyncio
import time

from parsers import parse_input
from globals import slaves, slaves_write_count, global_hashmap, expiry_hashmap
from config import server_config


async def slave_read_loop(reader, writer):
    """
    Continuously reads data from a slave connection.
    """
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                print("Slave disconnected")
                break

            leftover = data
            while leftover:
                parsed, leftover = parse_input(leftover)
                if isinstance(parsed, list) and len(parsed) >= 2:
                    cmd = parsed[0].upper()
                    if cmd == "REPLCONF" and parsed[1].upper() == "ACK":
                        if (reader, writer) in slaves:
                            slaves[(reader, writer)] += 1
    except Exception as e:
        print(f"Error in slave read loop: {e}")
    finally:
        if (reader, writer) in slaves:
            del slaves[(reader, writer)]
        writer.close()
        await writer.wait_closed()


async def write_to_slave(data):
    """
    Forward the given 'data' to all connected slaves.
    """
    global slaves_write_count
    for key in slaves:
        slaves[key] = 0  # reset acknowledgments

    for reader, writer in list(slaves.keys()):
        try:
            writer.write(data)
            await writer.drain()
        except Exception as e:
            print(f"Error forwarding to slave: {e}")
            del slaves[(reader, writer)]


async def wait_for_slaves(count, timeout):
    """
    Wait for 'count' slaves to ACK or until 'timeout' ms passes.
    """
    global slaves_write_count
    start_time = time.time()

    # Ask each slave for ACK
    for reader, writer in slaves:
        writer.write(b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n")
        await writer.drain()

    while True:
        acks = sum(1 for v in slaves.values() if v >= 1)
        slaves_write_count = acks
        if slaves_write_count >= count:
            print("ok")
            return
        if time.time() - start_time >= timeout / 1000:
            return
        await asyncio.sleep(0.01)


async def connect_to_master(host, port):
    """
    Connect to the master for replication (PSYNC, etc.).
    """
    try:
        reader, writer = await asyncio.open_connection(host, port)

        # PING
        writer.write(b"*1\r\n$4\r\nPING\r\n")
        await writer.drain()
        response = await reader.read(1024)
        data, _ = parse_input(response)

        if data == "PONG":
            # REPLCONF listening-port
            resp = f"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{server_config['port']}\r\n"
            writer.write(resp.encode())
            await writer.drain()

            response = await reader.read(1024)
            data, _ = parse_input(response)

            if data == "OK":
                # REPLCONF capa psync2
                writer.write(b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
                await writer.drain()

                response = await reader.read(1024)
                data, _ = parse_input(response)

                if data == "OK":
                    # PSYNC ? -1
                    writer.write(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
                    await writer.drain()

                    response = await reader.read(4096)
                    print(response)

                    meta_pos = response.find(b'$')
                    if meta_pos != -1:
                        # Attempt to find R in "REDIS" header
                        meta_pos += response[meta_pos:].find(b"R")
                        meta = response[meta_pos:]
                        header = meta[:9].decode('ascii')
                        magic, version = header[:5], header[5:]
                        print(f"Magic: {magic}, Version: {version}")

                        if magic != "REDIS" or not version.isdigit():
                            print("Invalid RDB header from master")
                            return

                        # We could parse initial RDB data here if needed.
                        leftover_data = meta[9:]
                        get_ack_offset = 0

                        if leftover_data:
                            unprocessed_part, _ = parse_input(leftover_data)
                            if isinstance(unprocessed_part, list) and len(unprocessed_part) > 0:
                                if unprocessed_part[0] == "REPLCONF":
                                    resp = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n"
                                    writer.write(resp.encode())
                                    await writer.drain()

                        print(get_ack_offset)

                        # Keep reading subsequent replication commands
                        from parsers import parse_input  # local import to avoid issues
                        from globals import global_hashmap, expiry_hashmap
                        import time

                        while True:
                            response = await reader.read(1024)
                            get_ack_offset += len(response)
                            print(response)

                            if not response:
                                print("Client (master) disconnected")
                                writer.close()
                                await writer.wait_closed()
                                break

                            result, buffer = parse_input(response)
                            print(get_ack_offset)

                            while result:
                                print(result)
                                if isinstance(result, list):
                                    cmd = result[0].upper()
                                    if cmd == "REPLCONF":
                                        resp = (
                                            f"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n"
                                            f"${len(str(get_ack_offset))}\r\n{get_ack_offset}\r\n"
                                        )
                                        writer.write(resp.encode())
                                        await writer.drain()

                                    elif cmd == "SET":
                                        global_hashmap[result[1]] = result[2]
                                        if len(result) > 3 and result[3] == "px" and result[1] in global_hashmap:
                                            expiry_hashmap[result[1]] = time.time() + (int(result[4]) / 1000)

                                if buffer:
                                    result, buffer = parse_input(buffer)
                                else:
                                    break

    except Exception as e:
        print(f"Error connecting to master: {e}")
