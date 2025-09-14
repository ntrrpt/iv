from loguru import logger as log
from datetime import datetime
from pathlib import Path

import mimetypes
import sys
import pprint
import requests
import time
import os

from email.utils import parsedate_to_datetime
from aiohttp_socks import ProxyConnector
import asyncio
import aiofiles
import aiohttp

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

css_spoiler = """
    <style>
    .spoiler {
        background: #000000;
        color: #000000;
        border-radius: 3px;
        padding: 0 2px;
        cursor: pointer;
    }
    .spoiler:hover {
        background: #000000;
        color: #FFFFFF;
    }
    </style>
    """


def get_with_retries(url, max_retries=10, retry_delay=10, proxy='', headers={}):
    proxies = {'http': proxy, 'https': proxy} if proxy else None

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, proxies=proxies, headers=headers, timeout=15)
            r.raise_for_status()
            return r
        except requests.exceptions.HTTPError as e:
            raise Exception(f'http failed: {e!r}, status: {r.status_code}')
        except requests.exceptions.RequestException as e:
            log.warning(f'attempt {attempt} failed: {e!r}. retrying...')
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
        log.trace(f'[skip] {dest_path}')
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

                    log.trace(dest_path)
                    return

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                log.warning(f'{dest_path} (attempt {attempt}/{max_retries}): {e}')

                if attempt >= max_retries:
                    log.error(f'failed {url!r} after {max_retries} tries')
                    return

                await asyncio.sleep(retry_delay)

            except Exception as e:
                log.opt(exception=True).error(f'{url}: {e}')
                return


def posts_by_id(posts: list):
    return {int(p['id']): p for p in posts}


def float_fmt(number: int, digits: int):
    return f'{number:.{digits}f}'


def stamp_fmt(timestamp: int) -> str:
    dt = datetime.fromtimestamp(timestamp)
    return dt.strftime('%H:%M:%S %d/%m/%Y')


def append(path: Path | str, data: str, end: str = '\n'):
    path = Path(path)
    with open(path, 'a', encoding='utf-8') as f:
        f.write(data + end)


def write(path: Path | str, data: str, end: str = '\n'):
    path = Path(path)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(data + end)


def delete(path: Path | str):
    path = Path(path)
    rem_file = Path(path)
    rem_file.unlink(missing_ok=True)
    log.trace(f'{path} deleted')


def pw(path: Path | str, data: str, end: str = '\n'):
    path = Path(path)
    s = str(pprint.pformat(str(data)))
    write(path, s, end)
    log.trace(f'{path} pwd')


def pp(data: str):
    s = pprint.pformat(str(data))
    print(s)


def pf(data: str):
    return str(pprint.pformat(str(data)))


def die(s: str = ''):
    if s:
        log.critical(str(s))
    sys.exit(1)


if __name__ in '__main__':
    print('%s: %s' % ('ext_2_mime', ext_2_mime))
    print('%s: %s' % ('mime_2_ext', mime_2_ext))
    print('%s: %s' % ('video_exts', video_exts))
    print('%s: %s' % ('image_exts', image_exts))
    print('%s: %s' % ('audio_exts', audio_exts))
    print('%s: %s' % ('exts', exts))
