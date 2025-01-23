def parse_input(data):
    """
    Redis protocol parser (RESP).
    Returns (parsed_result, leftover_bytes).
    """
    if not data:
        raise ValueError("No data to parse")

    first_byte = data[0]

    if first_byte == b"+"[0]:  # Simple String
        return data[1:].decode().strip(), b""

    elif first_byte == b"-"[0]:  # Error
        return {"error": data[1:].decode().strip()}, b""

    elif first_byte == b":"[0]:  # Integer
        return int(data[1:].strip()), b""

    elif first_byte == b"$"[0]:  # Bulk String
        lines = data.split(b"\r\n")
        length = int(lines[0][1:])
        if length == -1:
            return None, b""  # Null bulk string
        return lines[1].decode(), b""

    elif first_byte == b"*"[0]:  # Array
        lines = data.split(b"\r\n")
        num_elements = int(lines[0][1:])
        if num_elements == 0:
            return [], b""  # Empty array
        result = []
        i = 1
        processed_length = len(lines[0]) + 2

        while i < num_elements * 2:
            line = lines[i]
            processed_length += len(line) + 2
            if line.startswith(b"$"):
                result.append(lines[i + 1].decode())
                processed_length += len(lines[i + 1]) + 2
                i += 1
            i += 1

        leftover_data = data[processed_length:]
        return result, leftover_data

    else:
        raise ValueError("Unknown RESP type")
