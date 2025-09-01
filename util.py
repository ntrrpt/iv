from loguru import logger as log
from datetime import datetime
import subprocess
import mimetypes
import pathlib
import sys
import pprint
import shutil
import base64

aria2c_args = [
    'aria2c',
    '--max-connection-per-server=5',
    '--max-concurrent-downloads=5',
    '--auto-file-renaming=false',
    '--remote-time=true',
    '--log-level=error',
    '--console-log-level=error',
    '--download-result=hide',
    '--summary-interval=0',
    '--file-allocation=none',
    '--continue=true',
    '--check-certificate=false',
    '--allow-overwrite=false',
    '--quiet=true',
    '-Z',
]

ext_2_mime = mimetypes.types_map
mime_2_ext = {}
video_exts = []
image_exts = []
audio_exts = []
exts = []

for ext, mime in ext_2_mime.items():
    mime_2_ext[mime] = ext
    exts.append(ext)

    if mime.startswith('video/'):
        video_exts.append(ext)
    elif mime.startswith('image/'):
        image_exts.append(ext)
    elif mime.startswith('audio/'):
        audio_exts.append(ext)

via_exts = [video_exts + image_exts + audio_exts]


def posts_by_id(posts: list):
    return {int(p['id']): p for p in posts}


def image_from_bytes(data: bytes, mime_type: str):
    encoded = base64.b64encode(data).decode('utf-8')
    return f'data:{mime_type};base64,{encoded}'


def float_fmt(number, digits):
    return f'{number:.{digits}f}'


def stamp_fmt(timestamp: int) -> str:
    dt = datetime.fromtimestamp(timestamp)
    return dt.strftime('%H:%M:%S %d/%m/%Y')


def append(path, data, end='\n'):
    with open(path, 'a', encoding='utf-8') as f:
        f.write(data + end)
    log.trace(f'{path} appended')


def write(path, data, end='\n'):
    with open(path, 'w', encoding='utf-8') as f:
        f.write(data + end)
    log.trace(f'{path} writed')


def delete(path):
    rem_file = pathlib.Path(path)
    rem_file.unlink(missing_ok=True)
    log.trace(f'{path} deleted')


def pw(path, data, end='\n'):
    s = str(pprint.pformat(str(data)))
    write(path, s, end)
    log.trace(f'{path} pwd')


def pp(data):
    s = pprint.pformat(str(data))
    print(s)


def pf(data):
    return str(pprint.pformat(str(data)))


def die(s=''):
    if s:
        log.critical(str(s))
    sys.exit()


def is_aria2c_available():
    if shutil.which('aria2c') is None:
        return False

    try:
        r = subprocess.run(
            ['aria2c', '--version'], capture_output=True, text=True, check=True
        )
        return 'aria2' in r.stdout.lower()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False


if __name__ in '__main__':
    print('%s: %s' % ('ext_2_mime', ext_2_mime))
    print('%s: %s' % ('mime_2_ext', mime_2_ext))
    print('%s: %s' % ('video_exts', video_exts))
    print('%s: %s' % ('image_exts', image_exts))
    print('%s: %s' % ('audio_exts', audio_exts))
    print('%s: %s' % ('exts', exts))
