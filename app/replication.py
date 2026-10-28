import asyncio
import time

from parsers import read_value, read_rdb_payload, encode_array
from globals import slaves
from rdb import load_rdb
from store import apply_set

# Total bytes this master has propagated to its slaves.
repl_offset = 0


def get_repl_offset():
    return repl_offset


async def slave_read_loop(reader, writer):
    """
    Continuously reads data from a slave connection.
    """
    try:
        while True:
            parsed, _ = await read_value(reader)
            if isinstance(parsed, list) and len(parsed) >= 3:
                cmd = str(parsed[0]).upper()
                if cmd == "REPLCONF" and str(parsed[1]).upper() == "ACK":
                    if (reader, writer) in slaves:
                        slaves[(reader, writer)] = int(parsed[2])
    except asyncio.IncompleteReadError:
        print("Slave disconnected")
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


async def connect_to_master(host, port, own_port):
    """
    Connect to the master for replication (PSYNC, etc.).
    """
    writer = None
    try:
        reader, writer = await asyncio.open_connection(host, port)

        handshake = [
            (["PING"], "PONG"),
            (["REPLCONF", "listening-port", str(own_port)], "OK"),
            (["REPLCONF", "capa", "psync2"], "OK"),
        ]
        for command, expected in handshake:
            writer.write(encode_array(command))
            await writer.drain()
            reply, _ = await read_value(reader)
            if reply != expected:
                print(f"Unexpected reply to {command[0]} from master: {reply}")
                return

        writer.write(encode_array(["PSYNC", "?", "-1"]))
        await writer.drain()
        reply, _ = await read_value(reader)
        if not isinstance(reply, str) or not reply.startswith("FULLRESYNC"):
            print(f"Unexpected reply to PSYNC from master: {reply}")
            return

        # FULLRESYNC <replid> <offset>: the stream continues from the master's offset
        offset = int(reply.split()[2])

        load_rdb(await read_rdb_payload(reader))

        # Keep reading subsequent replication commands
        while True:
            command, raw = await read_value(reader)

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

            offset += len(raw)

    except asyncio.IncompleteReadError:
        print("Master disconnected")
    except Exception as e:
        print(f"Error in replication with master: {e}")
    finally:
        if writer:
            writer.close()
