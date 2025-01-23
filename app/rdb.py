import os

from globals import meta_data, global_hashmap, expiry_hashmap
from config import server_config

def parse_metadata(data):
    """
    Parse the metadata portion of the RDB-like content.
    Updates global 'meta_data', 'global_hashmap', and 'expiry_hashmap' as needed.
    Returns leftover bytes if any.
    """
    offset = 0
    while offset < len(data):
        marker = data[offset]
        offset += 1

        if marker == 0xFA:
            # Parse metadata attribute (e.g. version info)
            try:
                name_length = data[offset]
                offset += 1
                name = data[offset:offset + name_length].decode('utf-8')
                offset += name_length

                value_length = data[offset]
                offset += 1

                # Special encoding or plain string?
                if value_length & 0xC0 == 0xC0:
                    encoding_type = value_length & 0x3F
                    if encoding_type == 0:  # 8-bit int
                        value = str(data[offset])
                        offset += 1
                    elif encoding_type == 1:  # 16-bit int
                        value = str(int.from_bytes(data[offset:offset + 2], "little"))
                        offset += 2
                    elif encoding_type == 2:  # 32-bit int
                        value = str(int.from_bytes(data[offset:offset + 4], "little"))
                        offset += 4
                    else:
                        raise ValueError(f"Unsupported encoding: {encoding_type}")
                else:
                    value = data[offset:offset + value_length].decode('utf-8')
                    offset += value_length

                meta_data[name] = value
                print(f"Key: {name}, Value: {value}")

            except IndexError:
                print("Error: Ran out of data while parsing metadata.")
                break

        elif marker == 0xFE:
            # DB index marker
            try:
                db_index = data[offset]
                offset += 1
                print(f"Database Index: {db_index}")
            except IndexError:
                print("Error: Ran out of data while parsing DB index.")
                break

        elif marker == 0xFB:
            # Parse the main hash table
            try:
                main_hash_size = data[offset]
                offset += 1
                print(f"Main Hash Table Size: {main_hash_size}")

                expiry_hash_size = data[offset]
                offset += 1
                print(f"Expiry Hash Table Size: {expiry_hash_size}")

                # We'll read (main_hash_size - expiry_hash_size) keys
                for _ in range(expiry_hash_size, main_hash_size):
                    if data[offset] != 0x00:
                        print("Non-string data type")
                    offset += 1

                    key_length = data[offset]
                    offset += 1
                    key = data[offset:offset + key_length].decode("utf-8")
                    offset += key_length

                    value_length = data[offset]
                    offset += 1
                    value = data[offset:offset + value_length].decode("utf-8")
                    offset += value_length

                    global_hashmap[key] = value
                    print(f"Key: {key}, Value: {value}")

            except IndexError:
                print("Error: Ran out of data while parsing main hash table.")

        elif marker in (0xFC, 0xFD):
            # Parse expiration data
            try:
                if marker == 0xFC:  # Milliseconds
                    expiry_timestamp = int.from_bytes(data[offset:offset + 8], "little") / 1000
                    offset += 8
                else:  # marker == 0xFD, Seconds
                    expiry_timestamp = int.from_bytes(data[offset:offset + 4], "little")
                    offset += 4

                value_type = data[offset]
                offset += 1
                if value_type != 0x00:
                    print(f"Unsupported value type: {value_type}")
                    break

                key_length = data[offset]
                offset += 1
                key = data[offset:offset + key_length].decode("utf-8")
                offset += key_length

                value_length = data[offset]
                offset += 1
                value = data[offset:offset + value_length].decode("utf-8")
                offset += value_length

                global_hashmap[key] = value
                expiry_hashmap[key] = expiry_timestamp
                print(f"Key: {key}, Value: {value}, Expiration: {expiry_timestamp}")

            except IndexError:
                print("Error: Ran out of data while parsing expiration data.")

        elif marker == 0xFF:
            # End of file (plus checksum)
            try:
                checksum_stored = int.from_bytes(data[offset:offset + 8], "little")
                offset += 8
                print(f"End of file. Checksum (stored): {checksum_stored:#018x}")
                break
            except IndexError:
                print("Error: Insufficient data for checksum.")
        else:
            print("Unknown marker:", hex(marker))
            break

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
