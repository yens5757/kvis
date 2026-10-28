"""
Write a small RDB file for testing kvis's startup loading. Run from the repo root:

  python tools/make_rdb.py data/dump.rdb
  python app/main.py --dir data --dbfilename dump.rdb

Keys: name="kvis", counter=42 (stored as an integer), temp (expires in 1 hour),
old (expired in 2001, so it should NOT appear), and a 70-character key.
"""
import os
import sys
import time


def string(value):
    data = value.encode()
    if len(data) < 64:  # 6-bit length
        return bytes([len(data)]) + data
    return bytes([0x40 | (len(data) >> 8), len(data) & 0xFF]) + data  # 14-bit length


path = sys.argv[1]
in_one_hour = int((time.time() + 3600) * 1000).to_bytes(8, "little")
in_2001 = (1_000_000_000_000).to_bytes(8, "little")

rdb = b"REDIS0011"
rdb += b"\xfa" + string("redis-ver") + string("7.2.0")                      # aux field
rdb += b"\xfe\x00"                                                          # database 0
rdb += b"\xfb\x05\x02"                                                      # size hints
rdb += b"\x00" + string("name") + string("kvis")                            # plain key
rdb += b"\xfc" + in_one_hour + b"\x00" + string("temp") + string("soon gone")
rdb += b"\x00" + string("counter") + b"\xc0\x2a"                            # int8 42
rdb += b"\xfc" + in_2001 + b"\x00" + string("old") + string("stale")
rdb += b"\x00" + string("long:" + "x" * 65) + string("long key")            # 14-bit length
rdb += b"\xff" + b"\x00" * 8                                                # end + checksum

os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
with open(path, "wb") as f:
    f.write(rdb)
print(f"wrote {len(rdb)} bytes to {path}")
