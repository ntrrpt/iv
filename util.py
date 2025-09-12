from loguru import logger as log
from datetime import datetime
from pathlib import Path

import subprocess
import mimetypes
import sys
import pprint
import shutil
import base64
import requests
import time
import os

from email.utils import parsedate_to_datetime
import asyncio
import aiofiles
from aiohttp_socks import ProxyConnector
import aiohttp

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


def get_with_retries(url, max_retries=5, retry_delay=10, proxy='', headers={}):
    proxies = {'http': proxy, 'https': proxy} if proxy else None

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, proxies=proxies, headers=headers, timeout=15)
            r.raise_for_status()
            return r
        except requests.exceptions.HTTPError as e:
            raise Exception('http failed:', e, '| status:', r.status_code)
        except requests.exceptions.RequestException as e:
            log.warning(f'attempt {attempt} failed: {e}. retrying...')
            time.sleep(retry_delay)
    raise Exception(f'failed {url} after {max_retries} tries')


async def dw_files(
    urls, dest_folder, proxy=None, concurrency=5, max_retries=5, retry_delay=10
):
    Path(dest_folder).mkdir(parents=True, exist_ok=True)

    connector = ProxyConnector.from_url(proxy) if proxy else None
    sem = asyncio.Semaphore(concurrency)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            _dw_file(session, url, dest_folder, sem, max_retries, retry_delay)
            for url in urls
        ]
        await asyncio.gather(*tasks)


async def _dw_file(session, url, dest_folder, sem, max_retries, retry_delay):
    filename = Path(url).name
    dest_path = Path(dest_folder) / filename

    if dest_path.is_file():
        log.info(dest_path)
        return

    async with sem:
        for attempt in range(1, max_retries + 1):
            try:
                async with session.get(url) as r:
                    if r.status == 404:
                        log.error(f'{url} (404)')
                        return

                    r.raise_for_status()
                    async with aiofiles.open(dest_path, mode='wb') as f:
                        await f.write(await r.read())

                    if 'Last-Modified' in r.headers:
                        lm = r.headers['Last-Modified']
                        dt = parsedate_to_datetime(lm)
                        ts = dt.timestamp()
                        os.utime(dest_path, (ts, ts))

                    log.success(dest_path)
                    return

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                log.warning(f'{dest_path} (attempt {attempt}/{max_retries}): {e}')

                if attempt >= max_retries:
                    log.error(f'failed {url} after {max_retries} попыток')
                    return

                await asyncio.sleep(retry_delay)

            except Exception as e:
                log.opt(exception=True).error(f'{url}: {e}')
                return


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
    rem_file = Path(path)
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
