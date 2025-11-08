import argparse
import asyncio
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from time import sleep as delay
from typing import Optional

from bs4 import BeautifulSoup
from loguru import logger as log
from yarl import URL

import util

SITE = URL('https://img.heyuri.net')
BOARDS = ['a', 'b', 'c', 'h', 'j', 'jp', 'l', 'q', 's']


@dataclass
class Post:
    id: str
    num: Optional[str]
    author: str
    datetime: Optional[str]
    comment: str
    file_url: Optional[str]
    file_preview: Optional[str]
    file_properties: Optional[str]

    def to_dict(self):
        return asdict(self)


def parse_posts(soup, divs='div.post.op, div.post.reply'):
    all_posts: list[Post] = []

    for post in soup.select(divs):
        post_id = post.get('id')

        num = post.select_one('span.postnum a.qu')
        num = num.text if num else None

        author = post.select_one('span.postername')
        author = author.text if author else 'Anonymous'

        date = post.select_one('span.postDate')
        time = post.select_one('span.postTime')
        datetime = f'{date.text} {time.text}' if date and time else None

        comment = post.select_one('div.comment')
        comment = comment.get_text(' ', strip=True) if comment else ''

        file_link = post.select_one('div.filesize a[href]')
        preview = post.select_one('img.postimg')
        props = post.select_one('span.fileProperties')

        file_url = file_link['href'] if file_link else None
        file_preview = preview['src'] if preview else None
        file_properties = props.text if props else None

        all_posts.append(
            Post(
                id=post_id,
                num=num,
                author=author,
                datetime=datetime,
                comment=comment,
                file_url=file_url,
                file_preview=file_preview,
                file_properties=file_properties,
            )
        )

    return all_posts


def dump(sfx, from_to):
    if sfx not in BOARDS:
        log.error('invalid board')
        return

    url = SITE / sfx

    log.info(url)

    r = util.get_with_retries(url, proxy=args.proxy)
    soup = BeautifulSoup(r.text, 'html.parser')

    last_page = 0
    for a in soup.select('div#pagerPagesContainer a'):
        try:
            num = int(a.text.strip())
            if num > last_page:
                last_page = num
        except ValueError:
            continue

    f, t = from_to.split('-')
    ft_range = [x for x in range(int(f), int(t) + 1) if x < last_page + 1]

    pages = [url / 'koko.php' % {'page': x} for x in ft_range]

    op_posts: list[Post] = []

    for pg in pages:
        log.info('%3s / %s' % (pages.index(pg) + 1, len(pages)))

        r = util.get_with_retries(pg, proxy=args.proxy)
        soup = BeautifulSoup(r.text, 'html.parser')
        op_posts += parse_posts(soup, divs='div.post.op')
        delay(1)

    threads = [url / 'koko.php' % {'res': p.num} for p in op_posts]

    for th in threads:
        th_id = th.query.get('res')
        th_folder = Path(sfx) / th_id
        th_folder.mkdir(parents=True, exist_ok=True)

        r = util.get_with_retries(th, proxy=args.proxy)
        soup = BeautifulSoup(r.text, 'html.parser')
        posts = parse_posts(soup)

        json_data = json.dumps(
            [p.to_dict() for p in posts], indent=4, ensure_ascii=False
        )
        util.write(th_folder / f'{th_id}.json', str(json_data))

        images = [p.file_url for p in posts if p.file_url]
        asyncio.run(util.dw_files(images, dest_folder=th_folder, proxy=args.proxy))


if __name__ == '__main__':
    log.add('log.txt', encoding='utf-8')

    ap = argparse.ArgumentParser()
    add = ap.add_argument
    evg = os.environ.get

    # fmt: off
    add('-s', '--sfx',   type=str, nargs='+',        help="boards to dump")
    add('-r', '--range', type=str, default='0-9999', help="pages to dump, default 0-9999 (from-to, 0-5, 20-30)")

    add('--proxy', type=str, default=str(evg("IV_PROXY", '')))
    # fmt: on

    args = ap.parse_args()

    for sfx in args.sfx:
        dump(sfx, args.range)
