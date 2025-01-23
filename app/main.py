import argparse
import asyncio

from config import server_config, is_file_in_dir
from rdb import read_file
from server import start_server

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Redis-like server")
    parser.add_argument("--dir", required=False, help="Directory to store data files")
    parser.add_argument("--dbfilename", required=False, help="Name of the database file")
    parser.add_argument("--port", required=False, help="Server port")
    parser.add_argument("--replicaof", required=False, help="Replica of master (host port)")
    args = parser.parse_args()

    # Set server configuration from args
    server_config["dir"] = args.dir
    server_config["dbfilename"] = args.dbfilename
    server_config["port"] = args.port
    server_config["replicaof"] = args.replicaof

    # If we have a db file in the specified directory, read it
    if server_config["dir"] and server_config["dbfilename"] and \
       is_file_in_dir(server_config["dir"], server_config["dbfilename"]):
        print("Reading RDB file...")
        read_file(server_config["dir"], server_config["dbfilename"])

    # Start the asyncio server
    asyncio.run(start_server())
