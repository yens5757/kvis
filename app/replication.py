import asyncio
import time

from parsers import parse_input, parse_rdb_payload, encode_array, IncompleteError
from globals import slaves, slaves_write_count
from rdb import load_rdb
from store import apply_set


async def slave_read_loop(reader, writer, buffer=b""):
    """
    Continuously reads data from a slave connection.
    'buffer' holds any bytes already read from the connection but not yet parsed.
    """
    try:
        while True:
            while buffer:
                try:
                    parsed, buffer = parse_input(buffer)
                except IncompleteError:
                    break
                if isinstance(parsed, list) and len(parsed) >= 2:
                    cmd = str(parsed[0]).upper()
                    if cmd == "REPLCONF" and str(parsed[1]).upper() == "ACK":
                        if (reader, writer) in slaves:
                            slaves[(reader, writer)] += 1

            data = await reader.read(1024)
            if not data:
                print("Slave disconnected")
                break
            buffer += data
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


async def read_reply(reader, buffer, parser=parse_input):
    """
    Read from 'reader' until 'parser' can parse a complete value out of 'buffer'.
    Returns (value, leftover_bytes).
    """
    while True:
        try:
            return parser(buffer)
        except IncompleteError:
            data = await reader.read(1024)
            if not data:
                raise ConnectionError("Master closed the connection")
            buffer += data


async def connect_to_master(host, port, own_port):
    """
    Connect to the master for replication (PSYNC, etc.).
    """
    writer = None
    try:
        reader, writer = await asyncio.open_connection(host, port)
        buffer = b""

        handshake = [
            (["PING"], "PONG"),
            (["REPLCONF", "listening-port", str(own_port)], "OK"),
            (["REPLCONF", "capa", "psync2"], "OK"),
        ]
        for command, expected in handshake:
            writer.write(encode_array(command))
            await writer.drain()
            reply, buffer = await read_reply(reader, buffer)
            if reply != expected:
                print(f"Unexpected reply to {command[0]} from master: {reply}")
                return

        writer.write(encode_array(["PSYNC", "?", "-1"]))
        await writer.drain()
        reply, buffer = await read_reply(reader, buffer)
        if not isinstance(reply, str) or not reply.startswith("FULLRESYNC"):
            print(f"Unexpected reply to PSYNC from master: {reply}")
            return

        rdb, buffer = await read_reply(reader, buffer, parse_rdb_payload)
        load_rdb(rdb)

        # Bytes of the replication stream processed so far, reported back on GETACK
        offset = 0

        # Keep reading subsequent replication commands
        while True:
            try:
                command, leftover = parse_input(buffer)
            except IncompleteError:
                data = await reader.read(1024)
                if not data:
                    print("Master disconnected")
                    return
                buffer += data
                continue

            size = len(buffer) - len(leftover)
            buffer = leftover

            if isinstance(command, list) and command:
                cmd = str(command[0]).upper()
                if cmd == "REPLCONF" and len(command) > 1 and str(command[1]).upper() == "GETACK":
                    # Report the offset from before this GETACK, as Redis does
                    writer.write(encode_array(["REPLCONF", "ACK", str(offset)]))
                    await writer.drain()
                elif cmd == "SET":
                    try:
                        apply_set(command)
                    except (IndexError, ValueError):
                        print(f"Ignoring invalid SET from master: {command}")

            offset += size

    except Exception as e:
        print(f"Error in replication with master: {e}")
    finally:
        if writer:
            writer.close()
