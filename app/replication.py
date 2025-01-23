import asyncio
import time

from globals import slaves, slaves_write_count
from parsers import parse_input

async def slave_read_loop(reader, writer):
    """
    Continuously read data from a slave and update ack counters for replication.
    """
    try:
        while True:
            data = await reader.read(1024)
            if not data:
                print("[replication] Slave disconnected.")
                break
            leftover = data
            while leftover:
                parsed, leftover = parse_input(leftover)
                if isinstance(parsed, list) and len(parsed) >= 2:
                    cmd = parsed[0].upper()
                    if cmd == "REPLCONF" and parsed[1].upper() == "ACK":
                        if (reader, writer) in slaves:
                            # increment the ack count
                            slaves[(reader, writer)] += 1
    except Exception as e:
        print(f"[replication] Error in slave read loop: {e}")
    finally:
        # remove the slave from the dictionary
        if (reader, writer) in slaves:
            del slaves[(reader, writer)]
        writer.close()
        await writer.wait_closed()


async def write_to_slave(data):
    """
    Forwards the given 'data' to all slaves. Resets their ack counters to 0 before writing.
    """
    for key in slaves:
        slaves[key] = 0
    for reader, writer in list(slaves.keys()):
        try:
            writer.write(data)
            await writer.drain()
        except Exception as e:
            print(f"[replication] Error forwarding to slave: {e}")
            if (reader, writer) in slaves:
                del slaves[(reader, writer)]


async def wait_for_slaves(count, timeout):
    """
    Wait until at least `count` slaves have acked or `timeout` (in ms) elapses.
    """
    global slaves_write_count
    start_time = time.time()

    # Ask slaves to acknowledge
    for reader, writer in slaves:
        # REPLCONF GETACK command
        writer.write(b"*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n")
        await writer.drain()

    while True:
        acks = sum(1 for v in slaves.values() if v >= 1)
        slaves_write_count = acks
        if acks >= count:
            print("[replication] Enough slaves ACKed.")
            return
        if time.time() - start_time >= (timeout / 1000):
            print("[replication] WAIT timed out.")
            return
        await asyncio.sleep(0.01)
