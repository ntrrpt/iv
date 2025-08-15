import pprint, util, requests, sys, os
from nicegui import ui
from loguru import logger as log
tr, st, er, ok = (log.trace, log.info, log.error, log.success)

log.remove(0)
log.add(sys.stderr, format = "<level>[{time:DD-MMM-YYYY HH:mm:ss}]</level> {message}",
        backtrace = True, diagnose = True, colorize = True, level = 5)

log.add('log.txt', format = "[{time:DD-MMM-YYYY HH:mm:ss}] {message}",
        backtrace = True, diagnose = True, colorize = True, level = 5)

if True:
    from yk import yk_conv_thread as conv_thread
    from yk import yk_conv_catalog as conv_catalog
    from yk import yk_board_menu as board_menu
    from yk import menu_items
    SITE = 'http://ii.yakuji.moe'
elif False:
    # iichan.hk soon
    pass
else:
    # local db soon
    pass

cache_posts = {} # {id: post} -> dict
cache_json = {} # [posts] -> list

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

                if ui.context.client.id in cache_posts and target_id in cache_posts[ui.context.client.id]:
                    menu = ui.menu().props('anchor="bottom start" self="top start"')
                    menu.on('mouseleave', lambda e, m=menu: m.close())

                    with menu:
                        render_post(cache_posts[ui.context.client.id][target_id], disable_menu=True)

                    link.on('click', lambda e, m=menu, tid=target_id: (
                        m.close(), 
                        scroll_to_post(tid))
                    ).classes('text-blue-600')

                    link.on('mouseenter', lambda e, m=menu, tid=target_id: (
                        m.open(),
                        ui.run_javascript(
                            f"""document.getElementById('post-{tid}')
                            .style.backgroundColor = '#78E800';""" # green
                        )
                    ))
                    
                    link.on('mouseleave', lambda e, m=menu, tid=target_id: (
                        m.close(),
                        ui.run_javascript(
                            f"""document.getElementById('post-{tid}')
                            .style.backgroundColor = '#f3f4f6';""" # grey
                        )
                    ))

            except ValueError:
                ui.label(line)

        else:
            html_lines.append(f'<div style="margin:0; padding:0;">{line}</div>')
    return "<br>".join(html_lines)

def render_post(
        post, # dict with post
        index=0, # enumetate post for thread 
        disable_menu=False # disable menus (recursion fix) 
    ):
    rows = []
    with (
        ui.row().
        classes('w-full items-start bg-gray-100 rounded p-2 gap-2')
        .props(f'id=post-{post["id"]}')
    ):
        if 'skipped' in post:
            skipped_posts, skipped_images = post['skipped'].split('/')
            st_str = f"Пропущено {skipped_posts} сообщений."
            if int(skipped_images):
                st_str = st_str.replace('.', f" и {skipped_images} изображений.")

            ( # skipped posts
                ui.label(st_str).
                classes('font-bold text-sm text-gray-600').
                style("line-height: 1;")
            )
            return

        if post["image"]:
            with ui.dialog().props('backdrop-filter="blur(8px) brightness(40%)"') as dialog, ui.card():
                with ui.link(target=post["image"]):
                    if post["image"].endswith(('mp4', 'webm', 'mkv')):
                        ui.video(post["image"], autoplay=True)
                    else:
                        ui.html(f'<img src="{post["image"]}" style="max-height: 100%; max-width: 100%;">') 
                        # because ui.image is smol
            
            ( # thumbnail
                ui.image(post["thumb"])
                .classes("rounded")
                .style("width: 200px; height: auto; flex-shrink: 0;")
                .on('click', dialog.open)
            )
            
        with ui.column().classes('flex-1').style("line-height: 1;"):
            with ui.row().classes('gap-1 items-center'):
                if index > 0:
                    ( # number of post
                        ui.label(f'#{index} |').
                        classes('text-sm text-gray-600').
                        style("line-height: 1;")
                    )

                ( # poster name
                    ui.label(post['author']).
                    classes('font-bold text-sm text-gray-600').
                    style("line-height: 1;")
                )

                date_time = util.stamp_fmt(post['time'])
                ( # post date/time
                    ui.label(date_time).
                    classes('text-sm text-gray-600').
                    style("line-height: 1;")
                )

                ( # thread id
                    ui.link(f'No.{post['id']}', 'https://example.com').
                    style("line-height: 1;").
                    classes('text-sm text-gray-600 no-underline')
                )
            
                gate = [
                    not disable_menu, 
                    ui.context.client.id in cache_posts, 
                    ui.context.client.id in cache_json
                ]

                if all(gate):
                    replies = cache_json[ui.context.client.id]
                    for reply in replies:
                        if not reply['id']: # skipped
                            continue

                        if '>>%s' % post["id"] not in reply['text']:
                            continue

                        target_id = int(reply['id'])
                        if target_id not in cache_posts[ui.context.client.id]:
                            continue

                        link = ui.link('>>%s' % reply["id"]).style('color: blue; cursor: pointer; text-decoration: underline;')

                        menu = ui.menu().props('anchor="bottom start" self="top start"')
                        menu.on('mouseleave', lambda e, m=menu: m.close())

                        with menu:
                            render_post(cache_posts[ui.context.client.id][target_id], disable_menu=True)

                        link.on('click', lambda e, m=menu, tid=target_id: (
                            m.close(), 
                            scroll_to_post(tid))
                        ).classes('text-blue-600')

                        link.on('mouseenter', lambda e, m=menu, tid=target_id: (
                            m.open(),
                            ui.run_javascript(
                                f"""document.getElementById('post-{tid}')
                                .style.backgroundColor = '#78E800';""" # green
                            )
                        ))
                        
                        link.on('mouseleave', lambda e, m=menu, tid=target_id: (
                            m.close(),
                            ui.run_javascript(
                                f"""document.getElementById('post-{tid}')
                                .style.backgroundColor = '#f3f4f6';""" # grey
                            )
                        ))

            ui.separator()
            ui.html(render_post_text(post["text"])).style("line-height: 1;")

@ui.page('/thread/{board}/{thread_id}')
def thread_page(payload: str):
    url = f"{SITE}/{board}/res/{thread_id}.html"
    
    try:
        r = requests.get(url, timeout=5)
    except Exception as e:
        ui.label(f"r ex: {e}")
        return

    if r:
        try: 
            json = conv_thread(r.text)
        except Exception as e:
            ui.label(f"conv ex: {e}")
            return

        with ui.header().classes('bg-blue-900 text-white'):
            title = url
            if 'title' in json[0] and json[0]['title']:
                title = json[0]['title']
            ui.label(title).classes('text-xl font-bold mb-4')
            
            ui.space()
            board_menu()

        # for '>>' popups
        tmp = {}
        for p in json:
            tmp[int(p['id'])] = p
        cache_posts[ui.context.client.id] = tmp.copy()
        cache_json[ui.context.client.id] = json.copy()

        for index, post in enumerate(json, start=1):
            render_post(post, index)

        # dbg
        #ui.button('Say hi!', on_click=lambda: ui.notify(ui.context.client.id, close_button='OK'))
        #ui.label(str(pprint.pformat(cache_posts)))
        #util.delete_file("test.txt")
        #util.text_append("test.txt", str(pprint.pformat(conv_r)))
        
    st(f"{url}, {len(json)} posts")

@ui.page('/catalog/{board}')
def catalog_page(board: str):
    url = f"{SITE}/{board}"

    desc = ''
    for item in menu_items:
        if item[0] == board:
            desc = item[1]
            break

    if not desc:
        ui.label('invalid board')
        return
        
    try:
        r = requests.get(url, timeout=5)
    except Exception as e:
        ui.label(f"r ex: {e}")
        return
    
    if r:
        try: 
            json = conv_catalog(r.text)
        except Exception as e:
            ui.label(f"conv ex: {e}")
            return

        with ui.header().classes('bg-blue-900 text-white'):
            ui.label(desc).classes('text-xl font-bold mb-4')
            ui.space()
            board_menu()

        # FIXME: idk how fix this
        # TODO: idk what to do
        tmp1 = {}
        tmp2 = []
        for thread in json:
            for post in thread:
                tmp1[int(post['id'])] = post
                tmp2.append(post)
        cache_posts[ui.context.client.id] = tmp1.copy()
        cache_json[ui.context.client.id] = tmp2.copy()

        for thread in json:
            with ui.column().classes('w-full items-start bg-gray-300 rounded p-2 gap-2'):
                
                thread_title = thread[0]['title'] if 'title' in thread[0] else ''
                thread_url = '/'.join([SITE,'thread',board,thread[0]['id']])
                if not thread_title:
                    ui.label(thread_url).classes('text-xl font-bold')
                else:
                    ui.label(thread_title).classes('text-xl font-bold')
                    ui.label(thread_url).classes('text-s')

                ui.separator()
                for post in thread:
                    render_post(post, 0)

    st(f"{url}, {len(json)} posts")

@ui.page('/')
def main_page():
    ui.label("haiii ^____^").classes('text-xl font-bold mb-4')

if __name__ in {"__main__", "__mp_main__"}:
    ui.run()

'''
todo:
    - spoilers
    - cross-threads ???
    - mentions

http://127.0.0.1:8080/thread/b/4104479

# dbg
#ui.label(str(pprint.pformat(jj))).style('white-space: pre-line;')
#ui.button('Say hi!', on_click=lambda: ui.notify(ui.context.client.id, close_button='OK'))
#ui.label(str(pprint.pformat(cache_posts)))
#util.delete_file("test.txt")
#util.text_append("test.txt", str(pprint.pformat(str(json))))
'''
