from loguru import logger as log
from datetime import datetime
import mimetypes
import pathlib
import sys
import pprint

ext_2_mime = mimetypes.types_map
mime_2_ext = {}
video_exts = []
image_exts = []
audio_exts = []

for ext, mime in ext_2_mime.items():
    mime_2_ext[mime] = ext

    if mime.startswith("video/"):
        video_exts.append(ext)
    elif mime.startswith("image/"):
        image_exts.append(ext)
    elif mime.startswith("audio/"):
        audio_exts.append(ext)

via_exts = tuple(video_exts + image_exts + audio_exts)

def float_fmt(number, digits):
    return f'{number:.{digits}f}'

def stamp_fmt(timestamp: int) -> str:
    dt = datetime.fromtimestamp(timestamp)
    return dt.strftime("%H:%M:%S %d/%m/%Y")

def append(path, data, end='\n'):
    with open(path, 'a', encoding='utf-8') as f:
        f.write(data + end)
    log.trace(f"{path} appended")

def write(path, data, end='\n'):
    with open(path, 'w', encoding='utf-8') as f:
        f.write(data + end)
    log.trace(f"{path} writed")

def delete(path):
    rem_file = pathlib.Path(path)
    rem_file.unlink(missing_ok=True)
    log.trace(f"{path} deleted")

def pw(path, data, end='\n'):
    s = str(pprint.pformat(str(data)))
    write(path, s, end)
    log.trace(f"{path} pwd")

def pp(data):
    s = str(pprint.pformat(str(data)))
    print(s)

def pf(data):
    s = str(pprint.pformat(str(data)))
    return s

def die(s=''):
    if s:
        log.critical(str(s))
    sys.exit()

if __name__ in "__main__":
    print("%s: %s" % ('ext_2_mime', ext_2_mime))
    print("%s: %s" % ('mime_2_ext', mime_2_ext))
    print("%s: %s" % ('video_exts', video_exts))
    print("%s: %s" % ('image_exts', image_exts))
    print("%s: %s" % ('audio_exts', audio_exts))