import asyncio
import time

from parsers import parse_input, parse_rdb_payload, encode_array, IncompleteError
from globals import slaves
from rdb import load_rdb
from store import apply_set

# Total bytes this master has propagated to its slaves.
repl_offset = 0


def get_repl_offset():
    return repl_offset


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
                if isinstance(parsed, list) and len(parsed) >= 3:
                    cmd = str(parsed[0]).upper()
                    if cmd == "REPLCONF" and str(parsed[1]).upper() == "ACK":
                        if (reader, writer) in slaves:
                            slaves[(reader, writer)] = int(parsed[2])

            data = await reader.read(1024)
            if not data:
                print("Slave disconnected")
                break
            buffer += data
    except Exception as e:
        print(f"Error in slave read loop: {e}")
    finally:
        slaves.pop((reader, writer), None)
        writer.close()


async def write_to_slave(data):
    """
    Forward the given 'data' to all connected slaves.
    """
    global repl_offset
    repl_offset += len(data)

    for reader, writer in list(slaves.keys()):
        try:
            writer.write(data)
            await writer.drain()
        except Exception as e:
            print(f"Error forwarding to slave: {e}")
            slaves.pop((reader, writer), None)


async def wait_for_slaves(count, timeout):
    """
    Wait until 'count' slaves have acknowledged every write so far, or until 'timeout' ms
    passes (0 waits forever). Returns the number of slaves that acknowledged.
    """
    target = repl_offset

    def acked():
        return sum(1 for offset in slaves.values() if offset >= target)

    if acked() >= count:
        return acked()

    # Ask each slave for its offset. GETACK is part of the replication stream itself,
    # so it advances the offset, but slaves report the offset from before it.
    await write_to_slave(encode_array(["REPLCONF", "GETACK", "*"]))

    deadline = time.time() + timeout / 1000
    while acked() < count and (timeout == 0 or time.time() < deadline):
        await asyncio.sleep(0.01)
    return acked()


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

        # FULLRESYNC <replid> <offset>: the stream continues from the master's offset
        offset = int(reply.split()[2])

        rdb, buffer = await read_reply(reader, buffer, parse_rdb_payload)
        load_rdb(rdb)

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
