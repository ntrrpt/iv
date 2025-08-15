from time import sleep
import pathlib
from datetime import datetime

def stamp_fmt(timestamp: int) -> str:
    dt = datetime.fromtimestamp(timestamp)
    return dt.strftime("%H:%M:%S %d/%m/%Y")

def text_append(path, data, end='\n'):
    with open(path, 'a', encoding='utf-8') as f:
        f.write(data + end)

def text_write(path, data, end='\n'):
    with open(path, 'w', encoding='utf-8') as f:
        f.write(data + end)

def delete_file(filename):
    rem_file = pathlib.Path(filename)
    rem_file.unlink(missing_ok=True)
