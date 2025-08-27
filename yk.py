import util, db
import requests, re, mimetypes, argparse
import dateparser
import subprocess
import os, sys
from stopwatch import Stopwatch

import asyncio

import warnings

from loguru import logger as log
from pathlib import Path
from bs4 import BeautifulSoup

SITE = 'http://ii.yakuji.moe'

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
    ['vndev', 'Разработка визуальных новелл'],

    ['dev',   'Работа архива'] # FAIL ########################
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

all_boards = main_boards + arch_boards

main_sfxs = [x[0] for x in main_boards]
arch_sfxs = [x[0] for x in arch_boards]
all_sfxs = main_sfxs + arch_sfxs

def dump(sfx, from_to):
    def dump_thread(t_url, board_sfx):
        #######################################
        img_urls = []

        while True:
            try:
                r = requests.get(t_url)
                break
            except Exception as e:
                log.error(str(e))

        soup = BeautifulSoup(r.text, "html.parser")
        htm_urls = [x.get('href') for x in soup.find_all("a") if x.get('href') is not None]
        
        for htm in htm_urls:
            g = [ # must be all true
                htm.startswith(f'/{board_sfx}/src/'),
                htm.endswith(tuple(util.exts)),
                'iichan' not in htm,
                'desuchan' not in htm
            ]

            if all(g):
                img_urls.append('http://ii.yakuji.moe' + htm)
    
        img_urls = list(set(img_urls))
        img_urls.append(t_url)
        subprocess.run(util.aria2c_args + img_urls)
        #####################################

    if sfx not in all_sfxs:
        log.error('invalid board')
        return

    if not util.is_aria2c_available():
        log.error('no aria2c detected ;C')
        sys.exit()

    url = '/'.join([SITE, sfx])

    fr, to = from_to.split('-')
    fr_to = [x for x in range(int(fr), int(to))]

    soup = BeautifulSoup(requests.get(url).text, "html.parser")
    
    pages = ['wakaba.html' if 'arch' in sfx else 'index.html'] # num of pages
    _0_9999 = tuple([str(x) for x in range(9999)])

    for sp in soup.find_all("a"):
        href = sp.get('href')
        if not href:
            continue

        href = href.removeprefix('/%s/' % sfx)
        if href.startswith(_0_9999) and href.endswith('.html'):
            pages.append(href)

    threads = []
    
    for i, page in enumerate(pages):
        if i not in fr_to:
            continue
        
        while True:
            try:
                u = '/'.join([url, page])
                r = requests.get(u)
                break
            except Exception as e:
                log.error(str(e))

        soup = BeautifulSoup(r.text, "html.parser")
        htm_urls = [x.get('href') for x in soup.find_all("a") if x.get('href') is not None] 

        for htm in htm_urls:
            htm = htm.replace('/%s' % sfx, '.') # /ph/arch/res/10852.html => ./res/10852.html (for arch)

            g = [ # must be all true
                htm.endswith('.html'), 
                htm.startswith('./res/'),
                'iichan' not in htm,
                'desuchan' not in htm
            ]
            
            if all(g):
                threads.append(url + htm[1:])

        log.info(f'{i + 1} / {len(pages)}, {len(threads)} found')

    d = sfx.replace('/', '_')
    os.makedirs(d, exist_ok=True)
    os.chdir(d)

    for ii, thread in enumerate(threads):
        log.info(f'{ii + 1} / {len(threads) }, {thread}')

        num = thread[thread.rfind('/')+1:-5]
        os.makedirs(num, exist_ok=True)

        os.chdir(num)
        dump_thread(thread, sfx)
        os.chdir('..')

    os.chdir('..')

def yakuify(url):
    return ''.join([SITE, url])

''' todo: catalog
def board_menu():
    with ui.button(icon='menu'):
        with ui.menu() as menu:
            for item in boards:
                desc = "/%s/ — %s" % (item[0], item[1])
                (
                    ui.menu_item(desc, lambda e, it=item[0]: ui.navigate.to(f'/catalog/{it}'))
                    .props(f'id=post-{item[0]}')
                )
'''

def parse_time(date_str: str) -> int:
    '''
        date_str = "Пн 11 августа 2025 09:08:29"
    '''
    dt = dateparser.parse(date_str, languages=['ru'])
    ts = int(dt.timestamp())
    return ts

def parse_file(file_str):
    try:
        t = file_str.find("span", class_="filesize").find("a")["href"]
        file_url = yakuify(t)

        t = file_str.find("img", class_="thumb")["src"]
        thumb_url = yakuify(t)

        mime_type, _ = mimetypes.guess_type(file_url)

        r = {
            "url": file_url,
            "thumb": thumb_url,
            "file_type": mime_type,
            "has_blob": False
        }
    except:
        r = {}

    return r

def parse_catalog(html_str: str) -> dict:
    util.text_write('cat.txt', html_str)
    soup = BeautifulSoup(html_str, "html.parser")
    res = soup.find_all("div", id=lambda v: v and v.startswith("thread-"))
    r = [yk_parse_thread(str(x)) for x in res]
    return r

def parse_skipped(skip_str: str) -> tuple:
    '''
        Пропущено 1 сообщений. Для просмотра нажмите "Ответ".
        Пропущено 51 сообщений и 35 изображений. Для просмотра нажмите "Ответ".
    '''
    msg = img = 0
    pattern = re.compile(r'Пропущено (\d+) сообщений(?: и (\d+) изображений)?')

    match = pattern.search(skip_str)
    if match:
        msg = int(match.group(1))
        img = int(match.group(2)) if match.group(2) else 0

    return (msg, img)

def parse_thread(html: str) -> dict:
    thread = {}
    posts = []

    soup = BeautifulSoup(html, "html.parser")

    # OP
    op = soup.find("div", id=lambda v: v and v.startswith("thread-"))
    if op:
        files = []
        op_id = op.get("id").replace("thread-", "")
        op_title = op.find("span", class_="filetitle").get_text(strip=True)
        op_poster = op.find("span", class_="postername").get_text(strip=True)
        label_text = op.find("label").get_text(" ", strip=True)

        try:
            date_str = label_text. \
                replace(op_title, ""). \
                replace(op_poster, ""). \
                strip()
            date_time = parse_time(date_str)
        except:
            date_time = 0

        t = parse_file(op)
        if t:
            files.append(t)

        text = op.find_all("blockquote")[0].get_text("\n", strip=True)

        op_json = {
            "id": op_id,
            "files": files,
            "text": text,
            "author": op_poster,
            "time": date_time,
            "index": len(posts) + 1
        }

        thread['id'] = op_id
        thread['title'] = op_title

        posts.append(op_json)

    # SKIPPED (catalog)
    skip_str = str(soup.find_all("span", class_="omittedposts"))
    skip_tup = parse_skipped(skip_str)
    if skip_tup[0] or skip_tup[1]:
        st = "%s/%s" % (skip_tup[0], skip_tup[1])
        thread["skipped"] = st

    # REPLIES
    replies = soup.find_all("table")
    for reply in replies:
        files = []
        post = reply.find("td", class_="reply")
                
        if not post: # arch fix
            continue

        post_id = post.get("id").replace("reply", "")
        poster_name = post.find("span", class_="commentpostername").get_text(strip=True)
        label_text = post.find("label").get_text(" ", strip=True)

        try:
            date_str = label_text. \
                replace(poster_name, ""). \
                replace(op_poster, ""). \
                strip()
            date_time = parse_time(date_str)
        except:
            date_time = 0

        t = parse_file(post)
        if t:
            files.append(t)

        _ = post.find_all("blockquote")
        if not _:
            # a_arch fix
            log.warning('no text in reply')
            continue

        text = _[-1].get_text("\n", strip=True)

        reply_json = {
            "id": post_id,
            "files": files,
            "text": text,
            "author": poster_name,
            "time": date_time,
            "index": len(posts) + 1
        }

        posts.append(reply_json)

    thread["posts"] = posts
    thread["source"] = 'yk'
    return thread

async def html2db(dump_path='b', db_url='ii.db'):
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
        (p for p in dump_folder.iterdir() if p.is_dir()),
        key=lambda p: int(p.name)
    )

    sw_b = Stopwatch(2)
    sw_b.restart()

    for i, item in enumerate(threads, start=1):
        html_files = [
            f.name for f in item.iterdir() if f.is_file() and f.suffix == '.html'
        ]

        if not html_files:
            log.error(f'{item.name}: no html')
            continue

        if len(html_files) > 1:
            log.error(f'{item.name}: too many htmls')
            continue

        html_file = item / html_files[0] # posixpath

        with open(html_file, 'r', encoding='utf-8') as f:
            html_data = f.read()

        thread = parse_thread(html_data)
        if not thread['posts']:
            log.warning('no posts lole')
            continue

        first_post_id = thread['posts'][0]['id']
        title = thread.get('title') or ''
        thread_id = await db.add_thread(board_id, first_post_id, title)

        await db.add_posts(board_id, thread_id, thread['posts'], item if args.files else '')

        log.info(f"{i} / {len(threads)}: {board_name}/{item.name}")

    sw_b.stop()
    log.success(f"{dump_folder}: {str(sw_b)}")

    await db.close()

if __name__ == "__main__":
    # for async event loops
    warnings.filterwarnings("ignore", category=DeprecationWarning) 

    ap = argparse.ArgumentParser(description='ii.yakuji.moe tools')
    ap.add_argument('-v', '--verbose', action="store_true", default=False, help='verbose output (traces)')

    g = ap.add_argument_group("html2db options")
    g.add_argument('-p', '--path', nargs='+', type=str, help='''
        [toggle] input dirs with thread folders 
        (<board_prefix>/<thread_id>/<thread_id>.html, 
        b/1182145/1182145.html)
        '''
    )
    g.add_argument('--db', type=str, help='database output url (\'postgres://\', \'sqlite://\')')
    g.add_argument('--files', action="store_true", default=False, help='add file blobs to db')

    g = ap.add_argument_group("dumper options")
    g.add_argument('-s', '--sfx', type=str, nargs='+', help='''
        [toggle] boards to dump (azu, arch/ls)
        '''
    )
    g.add_argument('--range', type=str, default='0-9999', help='''
        pages to dump, default 0-9999 (from-to, 0-5, 20-30)
        '''
    )

    args = ap.parse_args()

    if args.verbose:
        log.remove()
        log.add(sys.stderr, level="TRACE")

    log.add('yk.txt')

    if args.sfx:
        match args.sfx[0]:
            case 'main':
                log.info('main boards set: %s' % main_sfxs)
                args.sfx = main_sfxs
            case 'arch':
                log.info('arch boards set: %s' % arch_sfxs)
                args.sfx = arch_sfxs
            case 'all':
                log.info('all boards set: %s' % all_sfxs)
                args.sfx = all_sfxs

        for s in args.sfx:
            dump(sfx=s, from_to=args.range)

    for path in args.path or []:
        if not args.db:
            db_file = '%s.db' % Path(path).name
            db_url = "sqlite://" + db_file
        else:
            db_url = args.db

        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        loop.run_until_complete(html2db(path, db_url))

