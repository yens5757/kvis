# Arbitrary metadata
meta_data = {}

# The main in-memory key-value store.
global_hashmap = {}

# The in-memory expiry map. key -> float(timestamp in seconds).
expiry_hashmap = {}

# A dictionary to track slaves. Keys are (reader, writer) pairs, values are integer ACK counters.
slaves = {}

# Keep track of the last count of write acknowledgments from slaves.
slaves_write_count = 0