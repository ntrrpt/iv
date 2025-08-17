'''
@ui.page('/yk/{board}/{thread_id}')
def yk_thread_page(board: str, thread_id: str):
    url = f"{SITE}/{board}/res/{thread_id}.html"
    
    try:
        r = requests.get(url, timeout=5)
    except Exception as e:
        ui.label(f"r ex: {e}")
        return

    if r:
        try: 
            thread = get_thread(r.text)
            #thread = get_thread(2)
        except Exception as e:
            ui.label(f"conv ex: {e}")
            return

        tr(thread['posts'][0])

        with ui.header().classes('bg-blue-900 text-white'):
            title = thread['title'] or url
            ui.label(title).classes('text-xl font-bold mb-4')
            
            ui.space()
            board_menu()

        # for '>>' popups
        tmp = {}
        for p in thread['posts']:
            tmp[int(p['id'])] = p
        cache_posts[ui.context.client.id] = tmp.copy()
        cache_json[ui.context.client.id] = thread['posts'].copy()

        for post in thread['posts']:
            render_post(post)

    st(f"{url}, {len(thread['posts'])} posts")

@ui.page('/catalog/{board}')
def yk_catalog_page(board: str):
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
            json = parse_catalog(r.text)
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
                    # todo: render_skipped enumerate
                    render_post(post, 0)

    st(f"{url}, {len(json)} posts")
'''
