from nicegui import ui
from loguru import logger as log
import util
import db

import pprint, requests, sys, os, mimetypes

if True:
    #from yk_parse import yk_parse_thread as get_thread
    #from yk_parse import yk_parse_catalog as parse_catalog
    #from yk_parse import yk_board_menu as board_menu
    #from yk_parse import menu_items
    #SITE = 'http://ii.yakuji.moe'
    pass
elif False:
    # iichan.hk soon
    pass
else:
    # local db soon
    pass

'''
from yk_parse import yk_parse_catalog as parse_catalog
from yk_parse import yk_board_menu as board_menu
from yk_parse import menu_items
SITE = 'http://ii.yakuji.moe'
'''

DBB = 'yk.db'
cache_thread = {}

color_blank = """
    document.getElementById('post-%s')
    .style.backgroundColor = '#%s';
"""

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
                            m.open(),
                            ui.run_javascript(
                                color_blank % (tid, '#78E800')
                            ) # green
                        ))
                        
                        link.on('mouseleave', lambda e, m=menu, tid=target_id: (
                            m.close(),
                            ui.run_javascript( 
                                color_blank % (tid, '#f3f4f6')
                            ) # grey
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
        if post["files"]:
            for file in post["files"]:
                with ui.dialog().props('backdrop-filter="blur(8px) brightness(40%)"') as dialog, ui.card():
                    with ui.link(target=file['url']):
                        if file['url'].endswith(tuple(util.video_exts)):
                            ui.video(file['url'], autoplay=True)
                        elif file['url'].endswith(tuple(util.image_exts)):
                            ui.html(f'<img src="{file['url']}" style="max-height: 100%; max-width: 100%;">')
                            # because ui.image is smol
                
                # thumbnail
                ui.image(file["thumb"]) \
                    .classes("rounded") \
                    .style("width: 200px; height: auto; flex-shrink: 0;") \
                    .on('click', dialog.open) \
            
        with ui.column().classes('flex-1').style("line-height: 1;"):
            with ui.row().classes('gap-1 items-center'):
                if post['index']:
                    # number of post
                    ui.label(f'#{post['index']} |'). \
                        classes('text-sm text-gray-600'). \
                        style("line-height: 1;")

                # poster name
                ui.label(post['author']). \
                    classes('font-bold text-sm text-gray-600'). \
                    style("line-height: 1;")

                # post date/time
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
                                m.open(),
                                ui.run_javascript(
                                    color_blank % (tid, '#78E800') # green
                                ) 
                            ))
                            
                            link.on('mouseleave', lambda e, m=menu, tid=target_post['id']: (
                                m.close(),
                                ui.run_javascript( 
                                    color_blank % (tid, '#f3f4f6') # grey
                                ) 
                            ))

            ui.separator()
            ui.html(render_post_text(post["text"])).style("line-height: 1;")

@ui.page('/db/search')
def db_search():
    def db_request_and_show(db_file, text):
        results.clear()
        
        with results:
            with ui.column(). \
                classes('w-full items-start bg-gray-100 rounded p-2 gap-2').\
                style('margin-top: -0.5em;'):
                
                try:
                    r = db.find_posts_by_text(db_file, text)
                except Exception as ex:
                    ui.label(f"query err: {str(ex)}")
                    return
                    
                ui.label(f"query: {text}")
                ui.label(f"found: {len(r)}")
                if checkbox.value:
                    ui.label(f"checked")

            for post in r:
                render_post(post)

    with ui.row().classes('bg-gray-100 rounded p-2 gap-2 self-center transition-all'):
        search = ui.input(placeholder='enter for request'). \
            props('autofocus outlined')
        
        checkbox = ui.checkbox('FTS5')

        search.on('keydown.enter', lambda e, db=DBB, sf=search.value: (
            db_request_and_show(DBB, str(search.value))
        ))

    results = ui.column()

@ui.page('/db/{board}')
def db_catalog(board: str):
    log.info(board)
    ui.label(f"^____^ catalog")

@ui.page('/db/{board}/{thread_id}')
def db_thread(board: str, thread_id: int):
    log.info(board)

    if not db.find_board_by_name(DBB, board):
        ui.label(f"board not found")
        return

    thread = db.find_thread_by_post(DBB, thread_id)

    if not thread:
        ui.label(f"thread not found")
        return

    with ui.header().classes('bg-blue-900 text-white'):
        with ui.column().style('margin-top: -0.5em;'):
            b_tid = f"{board}/{thread_id}"
            title = thread['title'] or b_tid
            
            ui.label(title).classes('text-xl font-bold')
            
            ui.label(f"{len(thread['posts'])} posts"). \
                style('margin-top: -1em;')

            #ui.space()

    cache_thread[ui.context.client.id] = thread.copy()

    for post in thread['posts']:
        render_post(post)

    if thread_id != thread['posts'][0]['id']:
        scroll_to_post(thread_id)

@ui.page('/')
def main_page():
    ui.label("haiii ^____^").classes('text-xl font-bold mb-4')

if __name__ in {"__main__", "__mp_main__"}:
    log.info('loaded')
    ui.run(
        port=1337, 
        title='viewer',
        show=False
    )

'''
todo:
    - spoilers
    - cross-threads ???
    -- ( ) to / / 

http://127.0.0.1:8080/thread/b/4104479

# dbg
#ui.label(str(pprint.pformat(jj))).style('white-space: pre-line;')
#ui.button('Say hi!', on_click=lambda: ui.notify(ui.context.client.id, close_button='OK'))
#ui.label(str(pprint.pformat(cache_posts)))
#util.delete_file("test.txt")
#util.text_append("test.txt", str(pprint.pformat(str(json))))
'''
