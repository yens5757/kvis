import argparse
import asyncio
import os

from config import server_config
from rdb import read_file
from server import main as server_main

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Redis-like server")
    parser.add_argument("--dir", required=False, help="Directory to store data files")
    parser.add_argument("--dbfilename", required=False, help="Name of the database file")
    parser.add_argument("--port", required=False, help="Server port")
    parser.add_argument("--replicaof", required=False, help="Replica of master (host port)")

    args = parser.parse_args()

    # Populate the global config
    server_config["dir"] = args.dir
    server_config["dbfilename"] = args.dbfilename
    server_config["port"] = args.port
    server_config["replicaof"] = args.replicaof

    # Try reading the file
    if server_config["dir"] and server_config["dbfilename"]:
        path = os.path.join(server_config["dir"], server_config["dbfilename"])
        if os.path.isfile(path):
            print(f"[main] Reading RDB file: {path}")
            read_file(server_config["dir"], server_config["dbfilename"])

    # Run the server
    asyncio.run(server_main())