import json
import os
from pathlib import Path
from datetime import datetime, date


# functions
def readFile(fileName):
    with open(fileName, "r") as file:
        data = file.read()
        file.close()
    return data


def writeFile(fileName, Data):
    with open(fileName, "w", newline='', encoding='utf-8') as file:
        file.write(Data)
    file.close()


def readJsonFile(fileName):
    try:
        with open(fileName, "rb") as file:
            return json.load(file)
    except IOError as err:
        if err.errno == 2:  # File didn't exist, no biggie
            return {}
        else:  # Something weird, just re-raise the error
            raise


def ensureDirectory(path):
    if not os.path.exists(path):
        os.makedirs(path)


def header(stirng):
    print("\n")
    x = len(stirng)*"="
    print(f"{stirng}\n{x}\n")


def get_paths(root, home, file_name ):
    """
    Returns common paths for the project.
    - root: repo name (e.g., 'gaming-bi-system')
    - home: base path (on Windows usually Path('C:/'))
    - file_name: __file__ of the caller (to infer repo root)
    """
    folder_name = Path(os.path.dirname(file_name)).resolve()   # repo root for etl_runner.py at root
    base = home if home else Path.home()

    # Logs/Data go under <home>/temp/.../<root>[/<repo_tail>]
    suffix = Path()
    data_path = (base / 'temp' / 'data' / root / suffix).resolve()
    logs_path = (base / 'temp' / 'logs' / root / suffix).resolve()

    bi_path  = (base / root).resolve()
    bi_auth  = (base / 'auth').resolve()

    return bi_path, bi_auth, data_path, logs_path, folder_name


def write_json_file(data, filename):
    """
    Write data to a JSON file, converting datetime objects to strings.

    Parameters:
    data (dict or list): The data to be written to the JSON file
    filename (str): The name of the file to write to (including path if needed)

    Returns:
    bool: True if successful, False otherwise
    """
    def default_converter(obj):
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()  # e.g. '2025-10-07T12:34:56'
        raise TypeError(f"Type {type(obj)} not serializable")

    try:
        with open(filename, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=4, default=default_converter)
        return True
    except Exception as e:
        print(f"Error writing to JSON file: {e}")
        return False