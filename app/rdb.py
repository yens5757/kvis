import os

from globals import meta_data, global_hashmap, expiry_hashmap
from config import server_config

def read_bytes(data, offset, size):
    """
    Read exactly 'size' bytes starting at 'offset'.
    Returns (bytes, new_offset).
    """
    if offset + size > len(data):
        raise IndexError("Unexpected end of RDB data")
    return data[offset:offset + size], offset + size


def read_length(data, offset):
    """
    Read an RDB length-encoded value starting at 'offset'.
    Returns (value, is_special, new_offset). When is_special is True, 'value' is a
    special string format (0-3) rather than a length.
    """
    first = data[offset]
    kind = first >> 6
    if kind == 0:  # 6-bit length
        return first & 0x3F, False, offset + 1
    if kind == 1:  # 14-bit length
        return ((first & 0x3F) << 8) | data[offset + 1], False, offset + 2
    if kind == 2:
        if first == 0x80:  # 32-bit length
            raw, offset = read_bytes(data, offset + 1, 4)
            return int.from_bytes(raw, "big"), False, offset
        if first == 0x81:  # 64-bit length
            raw, offset = read_bytes(data, offset + 1, 8)
            return int.from_bytes(raw, "big"), False, offset
        raise ValueError(f"Unsupported length encoding: {first:#04x}")
    return first & 0x3F, True, offset + 1


def read_string(data, offset):
    """
    Read an RDB string (plain or integer-encoded) starting at 'offset'.
    Returns (string, new_offset).
    """
    length, is_special, offset = read_length(data, offset)
    if is_special:
        int_sizes = {0: 1, 1: 2, 2: 4}  # 8, 16 and 32-bit ints; 3 is LZF
        if length not in int_sizes:
            raise ValueError("LZF-compressed strings are not supported")
        raw, offset = read_bytes(data, offset, int_sizes[length])
        return str(int.from_bytes(raw, "little", signed=True)), offset

    raw, offset = read_bytes(data, offset, length)
    return raw.decode("utf-8"), offset


def parse_metadata(data):
    """
    Parse everything after the 9-byte header of the RDB-like content.
    Updates global 'meta_data', 'global_hashmap', and 'expiry_hashmap' as needed.
    Returns leftover bytes if any.
    """
    offset = 0
    try:
        while offset < len(data):
            marker = data[offset]
            offset += 1

            if marker == 0xFA:
                # Metadata attribute (e.g. version info)
                name, offset = read_string(data, offset)
                value, offset = read_string(data, offset)
                meta_data[name] = value
                print(f"Key: {name}, Value: {value}")

            elif marker == 0xFE:
                # DB index marker
                db_index, _, offset = read_length(data, offset)
                print(f"Database Index: {db_index}")

            elif marker == 0xFB:
                # Hash table size hints; the entries follow as individual records, in any order
                main_hash_size, _, offset = read_length(data, offset)
                expiry_hash_size, _, offset = read_length(data, offset)
                print(f"Main Hash Table Size: {main_hash_size}")
                print(f"Expiry Hash Table Size: {expiry_hash_size}")

            elif marker in (0xFC, 0xFD):
                # Key with expiration
                if marker == 0xFC:  # Milliseconds
                    raw, offset = read_bytes(data, offset, 8)
                    expiry_timestamp = int.from_bytes(raw, "little") / 1000
                else:  # marker == 0xFD, Seconds
                    raw, offset = read_bytes(data, offset, 4)
                    expiry_timestamp = int.from_bytes(raw, "little")

                value_type = data[offset]
                offset += 1
                if value_type != 0x00:
                    print(f"Unsupported value type: {value_type}")
                    break

                key, offset = read_string(data, offset)
                value, offset = read_string(data, offset)
                global_hashmap[key] = value
                expiry_hashmap[key] = expiry_timestamp
                print(f"Key: {key}, Value: {value}, Expiration: {expiry_timestamp}")

            elif marker == 0x00:
                # String key without expiration
                key, offset = read_string(data, offset)
                value, offset = read_string(data, offset)
                global_hashmap[key] = value
                print(f"Key: {key}, Value: {value}")

            elif marker == 0xFF:
                # End of file (plus checksum)
                raw, offset = read_bytes(data, offset, 8)
                print(f"End of file. Checksum (stored): {int.from_bytes(raw, 'little'):#018x}")
                break

            else:
                print("Unsupported marker or value type:", hex(marker))
                break

    except (IndexError, ValueError) as e:
        print(f"Error parsing RDB data: {e}")

    return data[offset:]


def read_file(directory, filename):
    """
    Read and parse an RDB-like file from directory/filename.
    """
    file_path = os.path.join(directory, filename)
    with open(file_path, 'rb') as file:
        rdb_content = file.read()
        print(rdb_content)

        header = rdb_content[:9].decode('ascii')
        magic, version = header[:5], header[5:]
        print(f"Magic: {magic}, Version: {version}")

        if magic != "REDIS":
            print("Error: Magic header is not 'REDIS'")
            return
        if not version.isdigit():
            print("Error: Version is not numeric")
            return

        # Parse the remainder of the file
        parse_metadata(rdb_content[9:])
