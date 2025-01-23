def parse_input(data: bytes):
    """
    A simplified RESP parser that expects the entire command
    to be present in 'data'. Returns (parsed_object, leftover).
    """
    if not data:
        raise ValueError("No data to parse")

    first_byte = data[0]

    # + Simple String
    if first_byte == b"+"[0]:
        lines = data.split(b"\r\n", 1)
        if len(lines) < 2:
            return None, b""
        return lines[0][1:].decode().strip(), lines[1]

    # - Error
    elif first_byte == b"-"[0]:
        lines = data.split(b"\r\n", 1)
        if len(lines) < 2:
            return None, b""
        return {"error": lines[0][1:].decode().strip()}, lines[1]

    # : Integer
    elif first_byte == b":"[0]:
        lines = data.split(b"\r\n", 1)
        if len(lines) < 2:
            return None, b""
        return int(lines[0][1:].strip()), lines[1]

    # $ Bulk String
    elif first_byte == b"$"[0]:
        lines = data.split(b"\r\n", 2)
        if len(lines) < 3:
            return None, b""  # incomplete
        length = int(lines[0][1:])
        if length == -1:
            # Null bulk string
            return None, lines[2]
        bulk_str = lines[1][:length].decode()
        leftover = lines[1][length+2:]  # skip CRLF after the bulk
        return bulk_str, leftover

    # * Array
    elif first_byte == b"*"[0]:
        lines = data.split(b"\r\n", 1)
        if len(lines) < 2:
            return None, b""
        num_elements = int(lines[0][1:])
        leftover = lines[1]
        result = []
        for _ in range(num_elements):
            parsed, leftover = parse_input(leftover)
            result.append(parsed)
        return result, leftover

    else:
        raise ValueError("Unknown RESP type")
