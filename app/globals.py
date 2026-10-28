# Arbitrary metadata
meta_data = {}

# The main in-memory key-value store.
global_hashmap = {}

# The in-memory expiry map. key -> float(timestamp in seconds).
expiry_hashmap = {}

# A dictionary to track slaves. Keys are (reader, writer) pairs, values are the last
# replication offset each slave acknowledged.
slaves = {}