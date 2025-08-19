import util, db
import requests, re, mimetypes, argparse
import dateparser

import pprint
pp = pprint.pprint

from loguru import logger as log
from pathlib import Path
from bs4 import BeautifulSoup

def _test_thread(url: str):
    try:
        r = requests.get(url, timeout=10)
    except Exception as e:
        log.error(f"r ex: {e}")

    if r:
        log.info(url)
        try: 
            conv_r = yk_parse_thread(r.text)
        except Exception as e:
            log.error(f"conv ex: {e}")
            return
        pp(conv_r)

def _test_catalog(url: str):
    try:
        r = requests.get(url, timeout=10)
    except Exception as e:
        log.error(f"r ex: {e}")

    if r:
        log.info(url)
        try: 
            conv_r = yk_parse_catalog(r.text)
        except Exception as e:
            log.error(f"conv ex: {e}")
            return
        pp(conv_r)

#_test_thread("http://ii.yakuji.moe/b/res/4886591.html")
#_test_catalog('http://ii.yakuji.moe/b/')

def yakuify(url):
    return ''.join(['http://ii.yakuji.moe',url])

boards = [
    ['d',     'Работа сайта'],

    ['an',    'Живопись'],
    ['b',     'Бред'],
    ['bro',   'My Little Pony'],
    ['fr' ,   'Фурри'],
    ['gf',    'GIF- и FLASH-анимация'],
    ['hr',    'Высокое разрешение'],
    ['l',     'Литература'],
    ['m',     'Картинки-макросы и копипаста'],
    ['maid',  'Служанки'],
    ['med',   'Медицина'],
    ['mi',    'Оружие'],
    ['mu',    'Музыка'],
    ['ne',    'Животные'],
    ['o',     'Оэкаки'],
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

    ['bg',    'IIchan Archives — Настольные игры'],
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

    ['dev',   'Работа сайта']
]

sfxs = [x[0] for x in boards]

def board_menu():
    with ui.button(icon='menu'):
        with ui.menu() as menu:
            for item in boards:
                desc = "/%s/ — %s" % (item[0], item[1])
                (
                    ui.menu_item(desc, lambda e, it=item[0]: ui.navigate.to(f'/catalog/{it}'))
                    .props(f'id=post-{item[0]}')
                )

def parse_time(date_str: str) -> int:
    '''
        date_str = "Пн 11 августа 2025 09:08:29"
    '''
    dt = dateparser.parse(date_str, languages=['ru'])
    ts = int(dt.timestamp())
    return ts

def parse_file(file_str):
    r = {}

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
        pass

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

        text = post.find_all("blockquote")[-1].get_text("\n", strip=True)

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

def html2db(dump_path='b', db_path='ii.db'):
    dump_folder = Path(dump_path)
    db_file = Path(db_path)
    
    if not dump_folder.exists():
        util.die('no dir')

    if not db_file.is_file() or args.recreate:
        util.delete(db_file)
        db.init(db_path)
        log.info("db created!")

    for board in boards:
        if db.find_board_by_name(db_file, board[0]):
            continue

        db.add_board(db_file, board[0], board[1])
    
    board_name = dump_folder.name
    
    board_id = db.find_board_by_name(db_file, board_name)

    if not board_id:
        log.error(f'{board_name} is not standard board name')
        return
        #db.add_board(db_file, board_name)

    threads = sorted(
        (p for p in dump_folder.iterdir() if p.is_dir()),
        key=lambda p: int(p.name)
    )

    for i, item in enumerate(threads, start=1):
        html_files = [
            f.name for f in item.iterdir() if f.is_file() and f.suffix == '.html'
        ]

        if len(html_files) > 1:
            log.error(f'{item.name}: too many htmls')
            continue

        html_file = item / html_files[0] # posixpath

        with open(html_file, 'r', encoding='utf-8') as f:
            html_data = f.read()

        thread = parse_thread(html_data)
        title = thread['title'] or ''

        pos_id = db.add_thread(db_file, board_id, title)

        for post in thread['posts']:
            db.add_post(db_file, pos_id, post, item if args.files else '')

        log.info(f"{item.name}: {i} of {len(threads)} ")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='yk parser')

    parser.add_argument('brd_dir', type=str, help='''
        input dir with thread folders 
        (<board_prefix>/<thread_id>/<thread_id>.html, 
        b/1182145/1182145.html)'''
    )

    parser.add_argument('db_file', type=str, help='database file (*.db)')

    parser.add_argument('--recreate', action="store_true", default=False, help='rewrite db')
    parser.add_argument('--files', action="store_true", default=False, help='add file blobs to db')

    args = parser.parse_args()

    html2db(dump_path=args.brd_dir, db_path=args.db_file)


