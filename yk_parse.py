import util
from nicegui import ui
from loguru import logger as log

import requests, re, mimetypes
from bs4 import BeautifulSoup

from pprint import pprint as pp
import pprint

import dateparser

def yakuify(url):
    return ''.join(['http://ii.yakuji.moe',url])

boards = [
    ['an',   'Живопись'],
    ['b',    'Бред'],
    ['bro',  'My Little Pony'],
    ['fr' ,  'Фурри'],
    ['gf',   'GIF- и FLASH-анимация'],
    ['hr',   'Высокое разрешение'],
    ['l',    'Литература'],
    ['m',    'Картинки-макросы и копипаста'],
    ['maid', 'Служанки'],
    ['med',  'Медицина'],
    ['mi',   'Оружие'],
    ['mu',   'Музыка'],
    ['ne',   'Животные'],
    ['o',    'Оэкаки'],
    ['ph',   'Фото'],
    ['r',    'Просьбы'],
    ['s',    'Электроника и ПО'],
    ['sci',  'Наука'],
    ['sp',   'Спорт'],
    ['t',    'Торренты'],
    ['tran', 'Иностранные языки'],
    ['tv',   'Кино и ТВ'],
    ['w',    'Обои'],
    ['x',    'Паранормальные явления']
]

def board_menu():
    with ui.button(icon='menu'):
        with ui.menu() as menu:
            for item in boards:
                desc = "/%s/ — %s" % (item[0], item[1])
                (
                    ui.menu_item(desc, lambda e, it=item[0]: ui.navigate.to(f'/catalog/{it}'))
                    .props(f'id=post-{item[0]}')
                )

def yk_parse_time(date_str: str) -> int:
    '''
        date_str = "Пн 11 августа 2025 09:08:29"
    '''
    dt = dateparser.parse(date_str, languages=['ru'])
    ts = int(dt.timestamp())
    return ts

def yk_parse_file(file_str):
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

def yk_parse_catalog(html_str: str) -> dict:
    util.text_write('cat.txt', html_str)
    soup = BeautifulSoup(html_str, "html.parser")
    res = soup.find_all("div", id=lambda v: v and v.startswith("thread-"))
    r = [yk_parse_thread(str(x)) for x in res]
    return r

def yk_parse_skipped(skip_str: str) -> tuple:
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

def yk_parse_thread(html: str) -> dict:
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
            date_str = label_text.replace(op_title, "").replace(op_poster, "").strip()
            date_time = yk_parse_time(date_str)
        except:
            date_time = 0

        t = yk_parse_file(op)
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
    skip_tup = yk_parse_skipped(skip_str)
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
            date_str = label_text.replace(poster_name, "").replace(op_poster, "").strip()
            date_time = yk_parse_time(date_str)
        except:
            date_time = 0

        t = yk_parse_file(post)
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

def _test_thread(url: str):
    try:
        r = requests.get(url, timeout=10)

    except Exception as e:
        pp(f"r ex: {e}")

    if r:
        pp(url)

        try: 
            conv_r = yk_parse_thread(r.text)
        except Exception as e:
            pp(f"conv ex: {e}")
            return

        pp(conv_r)

def _test_catalog(url: str):
    try:
        r = requests.get(url, timeout=10)

    except Exception as e:
        pp(f"r ex: {e}")

    if r:
        pp(url)

        try: 
            conv_r = yk_parse_catalog(r.text)
        except Exception as e:
            pp(f"conv ex: {e}")
            return

        pp(conv_r)

if __name__ == "__main__":
    _test_thread("http://ii.yakuji.moe/b/res/4886591.html")
    #_test_catalog('http://ii.yakuji.moe/b/')