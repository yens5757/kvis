import os

server_config = {
    "dir": None,
    "dbfilename": None,
    "port": None,
    "replicaof": None
}

def is_file_in_dir(directory, filename):
    """
    Check if file exists in the given directory.
    """
    try:
        file_path = os.path.join(directory, filename)
        return os.path.isfile(file_path)
    except:
        return False