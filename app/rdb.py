import os
import time

from globals import meta_data, global_hashmap, expiry_hashmap

def parse_metadata(data: bytes):
    """
    Parses the 'body' of the RDB-like file.
    Modifies global_hashmap, expiry_hashmap, etc. in place.
    """
    offset = 0
    while offset < len(data):
        marker = data[offset]
        offset += 1

        # 0xFA: metadata attribute
        if marker == 0xFA:
            if offset >= len(data):
                print("Error: incomplete metadata attribute length.")
                break
            name_length = data[offset]
            offset += 1

            name = data[offset:offset + name_length].decode('utf-8')
            offset += name_length

            if offset >= len(data):
                print("Error: incomplete metadata value length.")
                break
            value_length = data[offset]
            offset += 1

            # Check if special integer encoding
            if value_length & 0xC0 == 0xC0:
                encoding_type = value_length & 0x3F
                if encoding_type == 0:
                    # 8-bit
                    value = str(data[offset])
                    offset += 1
                elif encoding_type == 1:
                    # 16-bit
                    value = str(int.from_bytes(data[offset:offset+2], 'little'))
                    offset += 2
                elif encoding_type == 2:
                    # 32-bit
                    value = str(int.from_bytes(data[offset:offset+4], 'little'))
                    offset += 4
                else:
                    print(f"Error: unsupported encoding type {encoding_type}.")
                    break
            else:
                # normal string
                value = data[offset:offset + value_length].decode('utf-8')
                offset += value_length

            meta_data[name] = value
            print(f"[parse_metadata] Key: {name}, Value: {value}")

        # 0xFE: DB index
        elif marker == 0xFE:
            if offset >= len(data):
                print("Error: incomplete DB index.")
                break
            db_index = data[offset]
            offset += 1
            print(f"[parse_metadata] DB Index: {db_index}")

        # 0xFB: main hash table
        elif marker == 0xFB:
            if offset+2 > len(data):
                print("Error: incomplete FB size info.")
                break
            main_hash_size = data[offset]
            offset += 1
            expiry_hash_size = data[offset]
            offset += 1
            print(f"[parse_metadata] Main Hash Table Size: {main_hash_size}")
            print(f"[parse_metadata] Expiry Hash Table Size: {expiry_hash_size}")

            for _ in range(expiry_hash_size, main_hash_size):
                if offset >= len(data):
                    print("Error: incomplete key-value data.")
                    break
                if data[offset] != 0x00:
                    print("[parse_metadata] Non-string data type.")
                offset += 1

                # read key
                if offset >= len(data):
                    print("Error: incomplete key length.")
                    break
                key_length = data[offset]
                offset += 1
                key = data[offset:offset + key_length].decode("utf-8")
                offset += key_length

                # read value
                if offset >= len(data):
                    print("Error: incomplete value length.")
                    break
                value_length = data[offset]
                offset += 1
                value = data[offset:offset + value_length].decode("utf-8")
                offset += value_length

                global_hashmap[key] = value
                print(f"[parse_metadata] Key: {key}, Value: {value}")

        # 0xFC or 0xFD: expiration info
        elif marker in (0xFC, 0xFD):
            if marker == 0xFC:
                # Milliseconds
                if offset+8 > len(data):
                    print("Error: incomplete FC timestamp.")
                    break
                expiry_timestamp = int.from_bytes(data[offset:offset+8], "little") / 1000
                offset += 8
            else:  # 0xFD
                # Seconds
                if offset+4 > len(data):
                    print("Error: incomplete FD timestamp.")
                    break
                expiry_timestamp = int.from_bytes(data[offset:offset+4], "little")
                offset += 4

            if offset >= len(data):
                print("Error: incomplete value type after expiry.")
                break
            value_type = data[offset]
            offset += 1
            if value_type != 0x00:
                print(f"Error: unsupported value type {value_type}.")
                break

            # read key
            if offset >= len(data):
                print("Error: incomplete key length.")
                break
            key_length = data[offset]
            offset += 1
            key = data[offset:offset + key_length].decode("utf-8")
            offset += key_length

            # read value
            if offset >= len(data):
                print("Error: incomplete value length.")
                break
            value_length = data[offset]
            offset += 1
            value = data[offset:offset + value_length].decode("utf-8")
            offset += value_length

            global_hashmap[key] = value
            expiry_hashmap[key] = expiry_timestamp
            print(f"[parse_metadata] Key: {key}, Value: {value}, Expiration: {expiry_timestamp}")

        # 0xFF: end of file / checksum
        elif marker == 0xFF:
            if offset+8 > len(data):
                print("[parse_metadata] Error: insufficient data for checksum.")
                break
            checksum_stored = int.from_bytes(data[offset:offset+8], "little")
            offset += 8
            print(f"[parse_metadata] End of file. Checksum (stored): 0x{checksum_stored:016x}")
            break

        else:
            print(f"[parse_metadata] Unknown marker: {hex(marker)}")
            break

    # Return leftover bytes
    return data[offset:]


def read_file(directory, filename):
    """
    Reads an RDB file from disk, checks "REDISxxxx" header, then calls parse_metadata.
    """
    file_path = os.path.join(directory, filename)
    with open(file_path, 'rb') as f:
        rdb_content = f.read()

    print("[read_file] RDB content size:", len(rdb_content))
    if len(rdb_content) < 9:
        print("[read_file] File too small for REDIS header.")
        return

    header = rdb_content[:9].decode('ascii')
    magic, version = header[:5], header[5:]
    print(f"[read_file] Magic: {magic}, Version: {version}")
    if magic != "REDIS":
        print("[read_file] Invalid magic string, expected 'REDIS'.")
        return
    if not version.isdigit():
        print("[read_file] Version not numeric.")
        return

    body = rdb_content[9:]
    leftover = parse_metadata(body)
    if leftover:
        print(f"[read_file] Leftover bytes after parse_metadata: {len(leftover)}")
