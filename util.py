import mimetypes
from loguru import logger as log

all_types = mimetypes.types_map

video_exts = []
image_exts = []
audio_exts = []

for ext, mime in all_types.items():
    if mime.startswith("video/"):
        video_exts.append(ext)
    elif mime.startswith("image/"):
        image_exts.append(ext)
    elif mime.startswith("audio/"):
        audio_exts.append(ext)

def stamp_fmt(timestamp: int) -> str:
    from datetime import datetime
    dt = datetime.fromtimestamp(timestamp)
    return dt.strftime("%H:%M:%S %d/%m/%Y")

def text_append(path, data, end='\n'):
    with open(path, 'a', encoding='utf-8') as f:
        f.write(data + end)
    log.trace(f"{path} appended")

def text_write(path, data, end='\n'):
    with open(path, 'w', encoding='utf-8') as f:
        f.write(data + end)
    log.trace(f"{path} writed")

def delete_file(path):
    import pathlib
    rem_file = pathlib.Path(path)
    rem_file.unlink(missing_ok=True)
    log.trace(f"{path} deleted")

def die(s=''):
    import sys
    if s:
        log.critical(str(s))
    sys.exit()