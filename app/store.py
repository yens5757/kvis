import time

# The main in-memory key-value store.
global_hashmap = {}

# The in-memory expiry map. key -> float(timestamp in seconds).
expiry_hashmap = {}


def set_key(key, value, expires_at=None):
    """
    Store 'value' at 'key', expiring at the unix timestamp 'expires_at' (seconds) if given.
    """
    global_hashmap[key] = value
    if expires_at is None:
        # A plain SET clears any expiry left over from an earlier SET ... px
        expiry_hashmap.pop(key, None)
    else:
        expiry_hashmap[key] = expires_at


def apply_set(args):
    """
    Apply a SET command: SET <key> <value> [px <milliseconds>].
    Raises IndexError/ValueError on missing or invalid arguments, before changing anything.
    """
    key, value = args[1], args[2]
    expires_at = None
    if len(args) > 3:
        if len(args) != 5 or args[3].lower() != "px":
            raise ValueError("syntax error")
        expires_at = time.time() + int(args[4]) / 1000

    set_key(key, value, expires_at)


def get_value(key):
    """
    Return the value stored at 'key', or None if it is missing or expired.
    Expired keys are removed when found.
    """
    if key in expiry_hashmap and expiry_hashmap[key] <= time.time():
        del expiry_hashmap[key]
        global_hashmap.pop(key, None)
    return global_hashmap.get(key)


def live_keys():
    """
    Return all keys that have not expired.
    """
    return [key for key in list(global_hashmap) if get_value(key) is not None]
