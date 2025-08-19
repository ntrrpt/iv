from nicegui import app, ui
from fastapi import HTTPException
from fastapi.responses import FileResponse

from loguru import logger as log
from datetime import timedelta
from pathlib import Path
import argparse, os
import time, pprint, requests, sys, os, mimetypes
import util, db, yk

from stopwatch import Stopwatch

pp = pprint.pformat
pf = pprint.pprint

cache_thread = {}
cache_files = {}

color_blank = """
    document.getElementById('post-%s')
    .style.backgroundColor = '%s';
"""

@app.get('/res/{board}/{filename}')
async def serve_local_file(board, filename: str):
    if board not in cache_files:
        s = f"board \'{board}\' not in cache_files"
        log.warning(s)
        raise HTTPException(status_code=404, detail=s)

    if filename in cache_files[board]:
        log.trace(filename)
        return FileResponse(cache_files[board][filename])

    s = f"file \'{board}/{filename}\' not found"
    log.warning(s)
    raise HTTPException(status_code=404, detail=s)

def check_for_post(post_id: int, posts: list):
    for post in posts:
        if int(post['id']) == post_id:
            return post

def scroll_to_post(post_id: int):
    ui.run_javascript(f"""
        const el = document.getElementById('post-{post_id}');
        if (el != null) el.scrollIntoView({{alignToTop: 'true', behavior: 'smooth', block: 'start'}});
    """)

def render_post_text(text):
    html_lines = []
    for line in text.split("\n"):
        if line.startswith(">") and not line.startswith(">>"):
            html_lines.append(f'<div style="color:green; margin:0; padding:0;">{line}</div>')

        elif line.startswith(">>"):
            try:
                target_id = int(line[2:])
                link = ui.link(line, None).style('color: blue; cursor: pointer; text-decoration: underline;')

                #todo: try/raise, just blue label on non-ext posts 
                if ui.context.client.id in cache_thread:
                    chk = check_for_post(target_id, cache_thread[ui.context.client.id]['posts'])
                    if chk:
                        menu = ui.menu().props('anchor="bottom start" self="top start"')
                        menu.on('mouseleave', lambda e, m=menu: m.close())

                        with menu:
                            render_post(chk, disable_menu=True)

                        link.on('click', lambda e, m=menu, tid=target_id: (
                            m.close(), 
                            scroll_to_post(tid))
                        ).classes('text-blue-600')

                        link.on('mouseenter', lambda e, m=menu, tid=target_id: (
                            m.open(), ui.run_javascript(
                                color_blank % (tid, '#78E800') # green
                            )
                        ))
                        
                        link.on('mouseleave', lambda e, m=menu, tid=target_id: (
                            m.close(), ui.run_javascript( 
                                color_blank % (tid, '#f3f4f6') # grey
                            )
                        ))

            except ValueError:
                ui.label(line)

        else:
            html_lines.append(f'<div style="margin:0; padding:0;">{line}</div>')
    return "<br>".join(html_lines)

def render_skipped(sk: str):
    skipped_posts, skipped_images = sk.split('/')
    st_str = f"Пропущено {skipped_posts} сообщений."
    if int(skipped_images):
        st_str = st_str.replace('.', f" и {skipped_images} изображений.")

    # skipped posts
    ui.label(st_str). \
        classes('font-bold text-sm text-gray-600'). \
        style("line-height: 1;")

def render_post(
        post, # dict with post
        disable_menu=False # disable menus
    ):
    rows = []
    with (
        ui.row().
        classes('w-full items-start bg-gray-100 rounded p-2 gap-2')
        .props(f'id=post-{post["id"]}')
    ):
        for file in post["files"] or []:
            try:
                if not cache_files:
                    raise
                file_url = thumb_url = f'/res/{post["board"]}/{file['file_name']}'
            except:
                file_url = file['url']
                thumb_url = file["thumb"]

            with ui.dialog().props('backdrop-filter="blur(8px) brightness(40%)"') as dialog, ui.card():
                with ui.link(target=file_url):
                    if file_url.endswith(tuple(util.video_exts)):
                        ui.video(file_url, autoplay=True)
                    elif file_url.endswith(tuple(util.image_exts)):
                        ui.html(f'<img src="{file_url}" style="max-height: 100%; max-width: 100%;">')
                        # because ui.image is smol
            
            # thumbnail
            ui.image(thumb_url) \
                .classes("rounded") \
                .style("width: 200px; height: auto; flex-shrink: 0;") \
                .on('click', dialog.open)
            
        with ui.column().classes('flex-1').style("line-height: 1;"):
            with ui.row().classes('gap-1 items-center'):
                # number of post
                ui.label(f'#{post['index']} |'). \
                    classes('text-sm text-gray-600'). \
                    style("line-height: 1;")

                # poster name
                ui.label(post['author']). \
                    classes('font-bold text-sm text-gray-600'). \
                    style("line-height: 1;")

                # post date/time
                if post['time']:
                    date_time = util.stamp_fmt(post['time'])
                    ui.label(date_time). \
                        classes('text-sm text-gray-600'). \
                        style("line-height: 1;")

                # thread id
                goto_thread = f"/{post['source']}/{post['board']}/{post['id']}"
                ui.link(f'No.{post['id']}', goto_thread). \
                    style("line-height: 1;"). \
                    classes('text-sm text-gray-600 no-underline')

                if ui.context.client.id in cache_thread:
                    replies = cache_thread[ui.context.client.id]['posts']
                    for reply in replies:
                        if not reply['id']: # skipped
                            continue

                        if '>>%s' % post["id"] not in reply['text']:
                            continue

                        target_post = check_for_post(reply['id'], replies)

                        if not target_post:
                            continue

                        link = ui.link('>>%s' % reply["id"]). \
                            style('color: blue; cursor: pointer; text-decoration: underline;')
                        if not disable_menu:
                            menu = ui.menu().props('anchor="bottom start" self="top start"')
                            menu.on('mouseleave', lambda e, m=menu: m.close())

                            with menu:
                                render_post(target_post, disable_menu=True)

                            link.on('click', lambda e, m=menu, tid=target_post['id']: (
                                m.close(), 
                                scroll_to_post(tid))
                            ).classes('text-blue-600')

                            link.on('mouseenter', lambda e, m=menu, tid=target_post['id']: (
                                m.open(), ui.run_javascript(
                                    color_blank % (tid, '#78E800') # green
                                ) 
                            ))
                            
                            link.on('mouseleave', lambda e, m=menu, tid=target_post['id']: (
                                m.close(), ui.run_javascript( 
                                    color_blank % (tid, '#f3f4f6') # grey
                                ) 
                            ))

            ui.separator()
            ui.html(render_post_text(post["text"])).style("line-height: 1;")

@ui.page('/search')
async def db_search():
    if not args.db:
        raise HTTPException(status_code=404, detail='--db not enabled')

    def draw(page=0):
        limit = int(posts_on_page.value)
        offset = limit*page
        q = str(query.value)
        sw = Stopwatch(2)
        
        try:
            sw.restart()
            count, posts = db.find_posts_by_text(
                args.db, 
                q, 
                FTS=fts_checkbox.value,
                LIMIT=limit, 
                OFFSET=offset
            )
            sw.stop()
        except Exception as ex:
            ui.notify(f"query err: {str(ex)}", type='negative', position='top')
            return

        ui.notify(
            f"{q}: {count} posts in {str(sw)}, {len(posts)} displayed", 
            type='positive', 
            position='top'
        )

        results.clear()
        with results:
            for post in posts:
                render_post(post)

        page_buttons.clear()
        if limit >= count:
            return

        with page_buttons:
            pages = int(count / limit) + 1

            for i in range(min(10, pages)):
                ui.button(i, on_click=lambda e, ii=i: (draw(ii)), color='green' if i == page else 'primary')

            if pages > 10:
                with ui.dropdown_button():
                    with ui.row().classes('w-full items-start bg-gray-100 rounded p-1 gap-1'):
                        for i in range(10, pages):
                            ui.button(i, on_click=lambda e, ii=i: (draw(ii)), color='green' if i == page else 'primary') 

    with ui.header().classes('bg-blue-900 text-white').classes('p-1.5 gap-1.5 self-center transition-all'):
        search = ui.button(color='orange-8', icon='search')

        query = ui.input(label='search query'). \
            props('autofocus outlined dense'). \
            classes('bg-gray-100')

        query.on('keydown.enter', lambda e: (draw()))
        
        search.on('click', lambda e: (draw()))

        posts_on_page = ui.number(label='posts on page', value=50, format='%d').props('autofocus outlined dense').classes('bg-gray-100 w-32')
        posts_on_page.on('keydown.enter', lambda e: (draw()))

        fts_checkbox = ui.checkbox('FTS5', value=True)

        ui.space()

        page_buttons = ui.button_group().props('outline')

    results = ui.column()

@ui.page('/db/{board}')
async def db_catalog(board: str):
    if not args.db:
        raise HTTPException(status_code=404, detail='--db not enabled')

    log.info(board)
    ui.label(f"^____^ catalog")

@ui.page('/db/{board}/{thread_id}')
async def db_thread(board: str, thread_id: int):
    if not args.db:
        raise HTTPException(status_code=404, detail='--db not enabled')
        
    def draw(page=0):
        limit = int(posts_on_page.value)
        offset = page*limit
        
        results.clear()

        with results:
            chk = [x for x in range(offset, offset + limit)]
            for i, post in enumerate(thread['posts']):
                if i in chk:
                    render_post(post)

        page_buttons.clear()
        if limit >= len(thread['posts']):
            return

        with page_buttons:
            pages = int(len(thread['posts']) / limit) + 1

            for i in range(min(10, pages)):
                ui.button(i, on_click=lambda e, ii=i: (draw(ii)), color='green' if i == page else 'primary')

            if pages > 10:
                with ui.dropdown_button():
                    with ui.row().classes('w-full items-start bg-gray-100 rounded p-1 gap-1'):
                        for i in range(10, pages):
                            ui.button(i, on_click=lambda e, ii=i: (draw(ii)), color='green' if i == page else 'primary')

    if not db.find_board_by_name(args.db, board):
        s = f"board \'{board}\' not found"
        log.warning(s); raise HTTPException(status_code=404, detail=s)

    thread = db.find_thread_by_post(args.db, thread_id)

    if not thread:
        s = f"thread \'{thread_id}\' not found"
        log.warning(s); raise HTTPException(status_code=404, detail=s)

    with ui.header().classes('bg-blue-900 text-white').classes('p-1.5 gap-1.5 self-center transition-all'):
        with ui.column().style('margin-top: -0.5em;'):
            title = thread['title'] or f"{board}/{thread_id}"
            ui.label(title).classes('text-xl font-bold')
            ui.label(f"{len(thread['posts'])} posts").style('margin-top: -1em;')

        ui.space()

        refresh = ui.button(color='orange-8', icon='refresh')
        refresh.on('click', lambda e: (draw()))

        posts_on_page = ui.number(label='posts on page', value=50, format='%d').props('autofocus outlined dense').classes('bg-gray-100 w-32')
        posts_on_page.on('keydown.enter', lambda e: (draw()))

        ui.space()

        page_buttons = ui.button_group().props('outline')

    cache_thread[ui.context.client.id] = thread.copy()

    results = ui.column()

    draw()

    if thread_id != thread['posts'][0]['id']:
        scroll_to_post(thread_id)

@ui.page('/')
async def index():
    ui.label("hai").classes('text-xl font-bold mb-4')

if __name__ in ["__main__", "__mp_main__"]:
    parser = argparse.ArgumentParser(description='db viewer')
    parser.add_argument('-v', '--verbose', action="store_true", default=False, help='verbose output (traces)')
    parser.add_argument('-d', '--db', type=str, help='database file (*.db)')
    parser.add_argument('-p', '--path', nargs='+',
        help='''
            [repeatable] dir with thread files 
            (<board_prefix>/<thread_id>/<files>, 
            b/1182145/1461775075639.jpg)
        '''
    )
    args = parser.parse_args()

    if args.db and not Path(args.db).exists():
        log.error('db file doesn\'t exist')
        sys.exit()

    if args.verbose:
        log.remove(); log.add(sys.stderr, level="TRACE")

    ui.run(port=1337, title='iv', show=False)

    for p in args.path or []:
        log.info(f'indexing {p}')
        path = Path(p)
        board_name = path.name

        if not path.exists():
            log.error('%s doesn\'t exists' % path)
            continue

        for thread in path.iterdir():
            if thread.is_file():
                continue

            try:
                int(thread.name)
            except:
                continue

            if board_name not in cache_files:
                cache_files[board_name] = {}

            for file in thread.iterdir():
                if not file.is_file() or file.suffix == '.html':
                    continue
                
                file_name = file.name
                file_path = str(file.resolve())

                if file_name in cache_files[board_name]:
                    log.critical(f'FILE DUB: {file_path}')

                cache_files[board_name][file_name] = file_path
'''
todo:
    - spoilers
    - cross-threads ???
    - idk

# dbg
#ui.label(str(pprint.pformat(jj))).style('white-space: pre-line;')
#ui.button('Say hi!', on_click=lambda: ui.notify(ui.context.client.id, close_button='OK'))
#ui.label(str(pprint.pformat(cache_posts)))
#util.delete_file("test.txt")
#util.text_append("test.txt", str(pprint.pformat(str(json))))
'''
