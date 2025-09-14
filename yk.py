import util
import db
import re
import mimetypes
import argparse
import dateparser
import os
import sys
import asyncio
import json

from stopwatch import Stopwatch
from loguru import logger as log
from pathlib import Path
from bs4 import BeautifulSoup
from yarl import URL

SITE = URL('http://ii.yakuji.moe')
res_pattern = re.compile(r'^/([^/]+)/res/(\d+)\.html(?:#(\d+))?$')

# fmt: off
main_boards = [
    ['d',     'Работа сайта'],

    ['an',    'Живопись'],
    ['b',     'Бред'],
    ['bro',   'My Little Pony'],
    ['fr' ,   'Фурри'],
    ['gf',    'GIF и FLASH-анимация'],
    ['hr',    'Высокое разрешение'],
    ['l',     'Литература'],
    ['m',     'Картинки-макросы и копипаста'],
    ['maid',  'Служанки'],
    ['med',   'Медицина'],
    ['mi',    'Оружие'],
    ['mu',    'Музыка'],
    ['ne',    'Животные'],
    ['o',     'Оэкаки'],
    ['old_o', 'Архив оэкаки'],
    ['ph',    'Фото'],
    ['r',     'Просьбы'],
    ['s',     'Электроника и ПО'],
    ['sci',   'Наука'],
    ['sp',    'Спорт'],
    ['t',     'Торренты'],
    ['tran',  'Иностранные языки'],
    ['tv',    'Кино и ТВ'],
    ['w',     'Обои'],
    ['x',     'Паранормальные явления'],

    ['au',    'Автомобили'],
    ['mo',    'Мотоциклы'],
    ['tr',    'Транспорт'],

    ['bg',    'Настольные игры'],
    ['vg',    'Видеоигры'],

    ['a',     'Аниме и манга'],
    ['aa',    'Аниме-арт'],
    ['abe',   'Old Home'],
    ['azu',   'Azumanga Daioh'],
    ['c',     'Косплей'],
    ['dn',    'Death Note'],
    ['fi',    'Фигурки'],
    ['hau',   'Higurashi no Naku Koro ni'],
    ['jp',    'Япония'],
    ['ls',    'Lucky☆Star'],
    ['ma',    'Манга'],
    ['me',    'Меха'],
    ['rm',    'Rozen Maiden'],
    ['sos',   'Suzumiya Haruhi no Yūutsu'],
    ['tan',   'Сетевые персонажи'],
    ['to',    'Touhou'],
    ['vn',    'Визуальные новеллы'],

    ['misc',  'Баннеры'],
    ['tenma', 'Юбилейные Баннеры'],
    #['vndev', 'Разработка визуальных новелл'], # only one thread (index)

    #['dev',   'Работа архива']   only one page (w/o index)
]

arch_boards = [
    ['azu_arch',   'Azumanga Daioh (архивы)'],
    ['d_arch',     'Работа сайта (архивы)'],
    ['an_arch',    'Живопись (архивы)'],
    ['b_arch',     'Бред (архивы)'],
    ['bro_arch',   'My Little Pony (архивы)'],
    ['fr_arch',    'Фурри (архивы)'],
    ['gf_arch',    'GIF и FLASH-анимация (архивы)'],
    ['hr_arch',    'Высокое разрешение (архивы)'],
    ['m_arch',     'Картинки-макросы и копипаста (архивы)'],
    ['maid_arch',  'Служанки (архивы)'],
    ['med_arch',   'Медицина (архивы)'],
    ['mi_arch',    'Оружие (архивы)'],
    ['mu_arch',    'Музыка (архивы)'],
    ['sci_arch',   'Наука (архивы)'],
    ['sp_arch',    'Спорт (архивы)'],
    ['tran_arch',  'Иностранные языки (архивы)'],
    ['w_arch',     'Обои (архивы)'],
    ['x_arch',     'Паранормальные явления (архивы)'],
    ['bg_arch',    'Настольные игры (архивы)'],
    ['vg_arch',    'Видеоигры (архивы)'],
    ['au_arch',    'Автомобили (архивы)'],
    ['mo_arch',    'Мотоциклы (архивы)'],
    ['tr_arch',    'Транспорт (архивы)'],
    ['a_arch',     'Аниме и манга (архивы)'],
    ['aa_arch',    'Аниме-арт (архивы)'],
    ['c_arch',     'Косплей (архивы)'],
    ['fi_arch',    'Фигурки (архивы)'],
    ['hau_arch',   'Higurashi no Naku Koro ni (архивы)'],
    ['jp_arch',    'Япония (архивы)'],
    ['ls_arch',    'Lucky☆Star (архивы)'],
    ['ma_arch',    'Манга (архивы)'],
    ['rm_arch',    'Rozen Maiden (архивы)'],
    ['sos_arch',   'Suzumiya Haruhi no Yūutsu (архивы)'],
    ['tan_arch',   'Сетевые персонажи (архивы)'],
    ['to_arch',    'Touhou (архивы)'],
    ['vn_arch',    'Визуальные новеллы (архивы)'],
    ['ne_arch',    'Животные (архивы)'],
    ['ph_arch',    'Фото (архивы)'],
    ['r_arch',     'Просьбы (архивы)']
]
# fmt: on

all_boards = main_boards + arch_boards

main_sfxs = [x[0] for x in main_boards]
arch_sfxs = [x[0] for x in arch_boards]
all_sfxs = main_sfxs + arch_sfxs


def dump(sfx, from_to):
    if sfx not in all_sfxs:
        log.error(f'invalid board: {sfx}')
        return

    url = SITE / sfx.replace('arch_', 'arch/')

    r = util.get_with_retries(url, proxy=args.proxy)
    soup = BeautifulSoup(r.text, 'html.parser')

    fr, to = from_to.split('-')
    fr_range = [x for x in range(int(fr), int(to))]

    ###########################################################################
    # extract pages

    pages = []

    if fr == '0':
        pages += [url / ('wakaba.html' if 'arch' in sfx else 'index.html')]

    for td in soup.find_all('td'):
        for a in td.find_all('a'):
            text = a.get_text(strip=True)
            if text.isdigit() and int(text) in fr_range:
                pages += [(SITE if 'arch' in sfx else url) / a.get('href').lstrip("/")]

    ###########################################################################
    # extract threads from pages

    threads = []

    for i, page in enumerate(pages):
        log.info('%4s / %s, %s found' % (i + 1, len(pages), len(threads)))

        try:
            r = util.get_with_retries(page, proxy=args.proxy)
        except Exception as e:
            # 500: http://ii.yakuji.moe/b/950.html
            log.opt(exception=True).error(f'{page}: {e}')
            continue

        catalog = parse_catalog(r.text)

        for th in catalog:
            threads.append(url / 'res' / f'{th["id"]}.html')

    ###########################################################################
    # extract posts from threads

    for i, thread in enumerate(threads):
        log.info('%4s / %s, %s' % (i + 1, len(threads), thread))

        th_id = thread.name.removesuffix('.html')
        th_path = Path(sfx) / th_id
        th_path.mkdir(parents=True, exist_ok=True)

        img_urls = []

        r = util.get_with_retries(thread, proxy=args.proxy)
        util.write(th_path / f'{th_id}.html', r.text)

        th = parse_thread(r.text)
        json_data = json.dumps(th, indent=4, ensure_ascii=False)
        util.write(th_path / f'{th_id}.json', str(json_data))

        for post in th.get('posts', {}):
            for file in post.get('files', {}):
                img_urls.append(file['url'])

        asyncio.run(util.dw_files(img_urls, dest_folder=th_path, proxy=args.proxy))


def parse_time(date_str: str) -> int:
    """
    date_str = "Пн 11 августа 2025 09:08:29"
    """
    dt = dateparser.parse(date_str, languages=['ru'])
    ts = int(dt.timestamp())
    return ts


def parse_file(file_str):
    try:
        t = file_str.find('span', class_='filesize').find('a')['href']
        file_url = str(SITE) + t

        t = file_str.find('img', class_='thumb')['src']
        thumb_url = str(SITE) + t

        mime_type, _ = mimetypes.guess_type(file_url)

        r = {
            'url': file_url,
            'thumb': thumb_url,
            'file_type': mime_type,
            'has_blob': False,
        }
    except (TypeError, AttributeError):
        r = {}

    return r


def parse_catalog(html_str: str) -> dict:
    soup = BeautifulSoup(html_str, 'html.parser')
    res = soup.find_all('div', id=lambda v: v and v.startswith('thread-'))
    r = [parse_thread(str(x)) for x in res]
    return r


def parse_skipped(skip_str: str) -> tuple:
    """
    Пропущено 1 сообщений. Для просмотра нажмите "Ответ".
    Пропущено 51 сообщений и 35 изображений. Для просмотра нажмите "Ответ".
    """
    msg = img = 0
    pattern = re.compile(r'Пропущено (\d+) сообщений(?: и (\d+) изображений)?')

    match = pattern.search(skip_str)
    if match:
        msg = int(match.group(1))
        img = int(match.group(2)) if match.group(2) else 0

    return (msg, img)


def replace_res_links_with_text(html: str) -> str:
    """
    replaces /board/res/thread.html[#post] with >>post or >>thread
    also adds \n, if in <p> after numero <br> is missing
    """
    soup = BeautifulSoup(html, 'html.parser')

    for a in soup.find_all('a', href=True):
        match = res_pattern.match(a['href'])
        if match:
            board, thread_id, post_number = match.groups()
            number = post_number if post_number else thread_id

            has_br = False
            sibling = a.next_sibling
            while sibling is not None and str(sibling).strip() == '':
                sibling = sibling.next_sibling
            if getattr(sibling, 'name', None) == 'br':
                has_br = True

            replacement = f'>>{number}' if has_br else f'\n>>{number}\n'
            a.replace_with(replacement)

    return str(soup)


def parse_thread(html: str) -> dict:
    thread = {}
    posts = []

    soup = BeautifulSoup(html, 'html.parser')

    # OP
    op = soup.find('div', id=lambda v: v and v.startswith('thread-'))
    if op:
        files = []
        op_id = op.get('id').replace('thread-', '')
        op_title = op.find('span', class_='filetitle').get_text(strip=True)
        op_poster = op.find('span', class_='postername').get_text(strip=True)
        label_text = op.find('label').get_text(' ', strip=True)

        try:
            date_str = label_text.replace(op_title, '').replace(op_poster, '').strip()
            date_time = parse_time(date_str)
        except:
            date_time = 0

        t = parse_file(op)
        if t:
            files.append(t)

        bq = op.find('blockquote')
        if bq:        
            text = bq.decode_contents()
            text = replace_res_links_with_text(text)

            op_json = {
                'id': op_id,
                'files': files,
                'text': text,
                'author': op_poster,
                'time': date_time,
                'index': len(posts) + 1,
            }

            thread['id'] = op_id
            thread['title'] = op_title

            posts.append(op_json)

    # SKIPPED (catalog)
    skip_str = str(soup.find_all('span', class_='omittedposts'))
    skip_tup = parse_skipped(skip_str)
    if skip_tup[0] or skip_tup[1]:
        st = '%s/%s' % (skip_tup[0], skip_tup[1])
        thread['skipped'] = st

    # REPLIES
    replies = soup.find_all('table')
    for reply in replies:
        files = []
        post = reply.find('td', class_='reply')

        if not post:  # arch fix
            continue

        post_id = post.get('id').replace('reply', '')
        poster_name = post.find('span', class_='commentpostername').get_text(strip=True)
        label_text = post.find('label').get_text(' ', strip=True)

        try:
            date_str = (
                label_text.replace(poster_name, '').replace(op_poster, '').strip()
            )
            date_time = parse_time(date_str)
        except:
            date_time = 0

        t = parse_file(post)
        if t:
            files.append(t)

        bq = post.find('blockquote')
        if not bq:  # a_arch fix
            log.warning('no text in reply')
            continue

        text = bq.decode_contents()
        text = replace_res_links_with_text(text)

        reply_json = {
            'id': post_id,
            'files': files,
            'text': text,
            'author': poster_name,
            'time': date_time,
            'index': len(posts) + 1,
        }

        posts.append(reply_json)

    thread['posts'] = posts
    thread['source'] = 'yk'
    return thread


async def make_db(dump_path, db_url):
    dump_folder = Path(dump_path)

    if not dump_folder.exists():
        util.die('no dir')

    await db.init(db_url)

    if not await db.create():
        log.error('create fail')
        await db.close()
        return

    for sfx, desc in all_boards:
        if await db.find_board_by_name(sfx):
            continue

        await db.add_board(sfx, desc)

    board_name = dump_folder.name

    board_id = await db.find_board_by_name(board_name)

    if not board_id:
        log.error(f'{board_name}: invalid board name')
        return

    threads = sorted(
        (p for p in dump_folder.iterdir() if p.is_dir()), key=lambda p: int(p.name)
    )

    sw_b = Stopwatch(2)
    sw_b.restart()

    for i, item in enumerate(threads, start=1):
        th_id = item.name

        files = [f for f in item.iterdir() if f.is_file()]
        html_files = [f for f in files if f.name == f'{th_id}.html']
        json_files = [f for f in files if f.name == f'{th_id}.json']

        if json_files:
            with open(json_files[0], 'r', encoding='utf-8') as f:
                thread = json.load(f)

        elif html_files:
            with open(html_files[0], 'r', encoding='utf-8') as f:
                thread = parse_thread(f.read())

        else:
            log.warning(f'{board_name}/{th_id}: no html/json lole')
            continue

        if not thread['posts']:
            log.warning(f'{board_name}/{th_id}: no posts lole')
            continue

        first_post_id = thread['posts'][0]['id']
        title = thread.get('title') or ''
        thread_id = await db.add_thread(board_id, first_post_id, title)

        await db.add_posts(
            board_id, thread_id, thread['posts'], item if args.files else ''
        )

        log.info('%5s / %s, %s/%s' % (i, len(threads), board_name, th_id))

    sw_b.stop()
    log.success(f'{dump_folder}: {str(sw_b)}')

    await db.close()


if __name__ == '__main__':
    ap = argparse.ArgumentParser(description='ii.yakuji.moe tools')
    add = ap.add_argument
    evg = os.environ.get

    # fmt: off
    add('-v', '--verbose', action='store_true', default=False, help='verbose output (traces)')

    g = ap.add_argument_group('html2db options')
    add = g.add_argument
    add('-p', '--path', nargs='+', type=str, help="""
        [toggle] input dirs with thread folders 
        (<board_prefix>/<thread_id>/<thread_id>.html, 
        b/1182145/1182145.html)
        """
    )
    add('--db', type=str, help="database output url ('postgres://', 'sqlite://')")
    add('--files', action='store_true', default=False, help='add file blobs to db')

    g = ap.add_argument_group('dumper options')
    add = g.add_argument

    add('-s', '--sfx', type=str, nargs='+', help="[toggle] boards to dump (azu, arch/ls)")
    add('-r', '--range', type=str, default='0-9999', help="pages to dump, default 0-9999 (from-to, 0-5, 20-30)")
    add('--proxy', type=str, default=str(evg("IV_PROXY", '')))
    # fmt: on

    args = ap.parse_args()

    if args.verbose:
        log.remove()
        log.add(sys.stderr, level='TRACE')

    log.add('yk.txt')

    if args.sfx:
        match args.sfx[0]:
            case 'main':
                log.info(f'main boards set: {main_sfxs!r}')
                args.sfx = main_sfxs
            case 'arch':
                log.info(f'arch boards set: {arch_sfxs!r}')
                args.sfx = arch_sfxs
            case 'all':
                log.info(f'all boards set: {all_sfxs!r}')
                args.sfx = all_sfxs

        for s in args.sfx:
            dump(sfx=s, from_to=args.range)

    for path in args.path or []:
        if not args.db:
            db_file = '%s.db' % Path(path).name
            db_url = 'sqlite://' + db_file
        else:
            db_url = args.db

        asyncio.run(make_db(path, db_url))

"""
todo: catalog
def board_menu():
    with ui.button(icon='menu'):
        with ui.menu() as menu:
            for item in boards:
                desc = "/%s/ — %s" % (item[0], item[1])
                (
                    ui.menu_item(desc, lambda e, it=item[0]: ui.navigate.to(f'/catalog/{it}'))
                    .props(f'id=post-{item[0]}')
                )
"""
