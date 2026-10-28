class IncompleteError(Exception):
    """
    Raised when the buffer does not yet hold a complete RESP value.
    """


def parse_input(data):
    """
    Redis protocol parser (RESP).
    Returns (parsed_result, leftover_bytes).
    Raises IncompleteError if 'data' ends before the first value is complete.
    """
    if not data:
        raise IncompleteError()

    line_end = data.find(b"\r\n")
    if line_end == -1:
        raise IncompleteError()

    first_byte = data[0:1]
    line = data[1:line_end]
    rest = data[line_end + 2:]

    if first_byte == b"+":  # Simple String
        return line.decode(), rest

    elif first_byte == b"-":  # Error
        return {"error": line.decode()}, rest

    elif first_byte == b":":  # Integer
        return int(line), rest

    elif first_byte == b"$":  # Bulk String
        length = int(line)
        if length == -1:
            return None, rest  # Null bulk string
        if len(rest) < length + 2:
            raise IncompleteError()
        return rest[:length].decode(), rest[length + 2:]

    elif first_byte == b"*":  # Array
        num_elements = int(line)
        if num_elements == -1:
            return None, rest  # Null array
        result = []
        for _ in range(num_elements):
            element, rest = parse_input(rest)
            result.append(element)
        return result, rest

    else:
        raise ValueError("Unknown RESP type")


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
