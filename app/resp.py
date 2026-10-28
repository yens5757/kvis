# Largest bulk string accepted, as in Redis (proto-max-bulk-len).
MAX_BULK_LENGTH = 512 * 1024 * 1024

# Deepest array nesting accepted. Commands are flat arrays, so this only stops abuse.
MAX_NESTING = 8


async def read_value(reader):
    """
    Read one RESP value from an asyncio StreamReader.
    The reader buffers incoming data itself, so a value split across several network
    reads (or several values in one read) is handled without copying the data again.
    Returns (parsed_result, raw_bytes), where raw_bytes is the value exactly as received.
    Raises asyncio.IncompleteReadError if the connection closes, and ValueError or
    asyncio.LimitOverrunError on malformed input.
    """
    chunks = []
    value = await _read_value(reader, chunks, 0)
    return value, b"".join(chunks)


async def _read_value(reader, chunks, depth):
    line = await reader.readuntil(b"\r\n")
    chunks.append(line)
    first_byte, body = line[0:1], line[1:-2]

    if first_byte == b"+":  # Simple String
        return body.decode()

    elif first_byte == b"-":  # Error
        return {"error": body.decode()}

    elif first_byte == b":":  # Integer
        return int(body)

    elif first_byte == b"$":  # Bulk String
        length = int(body)
        if length == -1:
            return None  # Null bulk string
        if not 0 <= length <= MAX_BULK_LENGTH:
            raise ValueError(f"Invalid bulk length: {length}")
        data = await reader.readexactly(length + 2)
        if data[-2:] != b"\r\n":
            raise ValueError("Bulk string not terminated by CRLF")
        chunks.append(data)
        return data[:-2].decode()

    elif first_byte == b"*":  # Array
        num_elements = int(body)
        if num_elements == -1:
            return None  # Null array
        if num_elements < 0 or depth >= MAX_NESTING:
            raise ValueError("Invalid array")
        result = []
        for _ in range(num_elements):
            result.append(await _read_value(reader, chunks, depth + 1))
        return result

    else:
        raise ValueError("Unknown RESP type")


async def read_rdb_payload(reader):
    """
    Read the RDB file a master sends after FULLRESYNC: "$<length>\\r\\n" followed by
    exactly <length> raw bytes, with no trailing CRLF.
    """
    line = await reader.readuntil(b"\r\n")
    if line[0:1] != b"$":
        raise ValueError("Expected RDB payload from master")
    return await reader.readexactly(int(line[1:-2]))


def encode_bulk_string(value):
    """
    Encode a str (or None for a null bulk string) as a RESP bulk string.
    """
    if value is None:
        return b"$-1\r\n"
    data = value.encode()
    return b"$" + str(len(data)).encode() + b"\r\n" + data + b"\r\n"


def encode_array(items):
    """
    Encode a list of str as a RESP array of bulk strings.
    """
    return f"*{len(items)}\r\n".encode() + b"".join(encode_bulk_string(item) for item in items)
