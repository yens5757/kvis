# KVIS
KVIS is a simple key-value storage system inspired by Redis, offering core commands, key expiry, RDB-like persistence, and master-replica synchronization.

# How to use
1. Clone/Download this repository (or place the files in a folder).
2. Navigate to the folder containing your Python files (e.g., main.py, server.py, etc.).
3. Run the server:
```
python main.py [--port PORT] [--dir DIRECTORY] [--dbfilename DBFILE] [--replicaof "HOST PORT"]
```
--port: Optional, defaults to 6379 if not provided.  
--dir: Optional, directory to store or read the RDB-like data file.  
--dbfilename: Optional, name of the RDB-like data file to load at startup.  
--replicaof: Optional, set to "HOST PORT" if you want this server to be a replica of another server.  
```
# Basic usage on port 7000, reading from ./data/mydb.rdb
python main.py --port 7000 --dir ./data --dbfilename mydb.rdb
```
```
# Run as a replica of a master at 127.0.0.1:6379
python main.py --replicaof "127.0.0.1 6379"
```
# What This Project Does
Starts an Asyncio-based TCP server that behaves like a simplified Redis instance.  
Accepts Redis-RESP protocol commands such as PING, ECHO, SET, GET, etc.  
Stores data in memory (a global Python dictionary), with optional expiration times.  
Implements replication where multiple servers can synchronize data (PSYNC, REPLCONF).  
Optionally loads a custom RDB-like file format at startup if --dir and --dbfilename are provided.  

# What Functions It Offers
This server handles a subset of Redis-like commands:  
```
ECHO <message>
```
Replies with the same message.  
```
PING
```
Returns PONG.  
```
SET <key> <value> [px <milliseconds>]
```
Stores a key/value pair in memory, with optional expiration (in ms).  
```
GET <key>
```
Retrieves the value for a given key (or nil if not found/expired).  
```
KEYS *
```
Returns all currently stored keys.  
```
CONFIG GET <param>
```
Retrieves server configuration (e.g., dir, dbfilename).  
```
INFO
```
Provides some basic server info (e.g., role:master or role:slave).  
```
REPLCONF
```
Used internally for replication; slaves inform master of their listening port or request GETACK.  
```
PSYNC
```
Master/Slave synchronization command.  
```
WAIT <numslaves> <timeout>
```
Blocks until a certain number of slaves acknowledge a write or until timeout.  
Additional internal commands and logic (like slave_read_loop, wait_for_slaves) are used to handle replication behind the scenes.
