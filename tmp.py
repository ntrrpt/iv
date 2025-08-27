'''

async def _table_exists(table_name: str) -> bool:
    conn = connections.get("default")
    dialect = conn.capabilities.dialect

    match dialect:
        case "postgres":
            query = """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name = $1
                ) AS exists
            """
            _, rows = await conn.execute_query(query, [table_name])
            return rows[0]["exists"]

        case "mysql":
            query = """
                SELECT COUNT(*) AS cnt FROM information_schema.tables 
                WHERE table_schema = DATABASE() AND table_name = %s
            """
            _, rows = await conn.execute_query(query, [table_name])
            return rows[0]["cnt"] > 0

        case "sqlite":
            query = """
                SELECT name FROM sqlite_master WHERE type='table' AND name=?
            """
            _, rows = await conn.execute_query(query, [table_name])
            return len(rows) > 0

        case _:
            raise NotImplementedError(f"{dialect} not supported")

    1.html  
    2.html  
    3.html  
    4.html  
    5.html  
    6.html  
    7.html  
    8.html  
    9.html  
    10.html 
    11.html 
    12.html 
    13.html 
    14.html 
    15.html 
    16.html 
    17.html 
    18.html 
    19.html 
    20.html 
    21.html 
    22.html 
    23.html 
    24.html 
    25.html 
    26.html 
    27.html 




    /ph/arch/1.html
    /ph/arch/2.html
    
    /ph/arch/res/10852.html#i14836
/ph/arch/res/10852.html#i14837
/ph/arch/res/10852.html#i14838
/ph/arch/res/10852.html#i14840
/ph/arch/res/10852.html#i14853
/ph/arch/res/10852.html#i14860
/ph/arch/res/10852.html#i14861
/ph/arch/res/10852.html#i14862
    
    ./res/10783.html http://ii.yakuji.moe/azu/res/10783.html
./res/57.html http://ii.yakuji.moe/azu/res/57.html
./res/3971.html http://ii.yakuji.moe/azu/res/3971.html
./res/3073.html http://ii.yakuji.moe/azu/res/3073.html
./res/7850.html http://ii.yakuji.moe/azu/res/7850.html
./res/10800.html http://ii.yakuji.moe/azu/res/10800.html
./res/10993.html http://ii.yakuji.moe/azu/res/10993.html
    
    

def _find_file_by_seq(db, post_seq): # sync
    with sqlite3.connect(db) as conn:
        cur = conn.cursor()

        q = "SELECT file_type, file_data FROM attachments WHERE seq = ?"
        cur.execute(q, (post_seq,))
        r = cur.fetchone()

    return r if r else None

async def _async_find_file_by_seq(post_seq: int):
    qs = Attachment.filter(post_seq=post_seq).annotate(
        has_file_data=Case(
            When(file_data__not_isnull=True, then=Value(True)),
            default=Value(False)
        )
    ).limit(1).values("file_type", "file_name", "file_url", "thumb_url", "has_file_data")

    result = await qs  # это вернёт список словарей
    return result[0] if result else None

def _add_board(db, name, description=''):
    with sqlite3.connect(db) as conn:
        cur = conn.cursor()

        q = "INSERT INTO boards (name, description) VALUES (?, ?)"
        cur.execute(q, (name, description))
        conn.commit()

        log.trace(f'board {name} created')

        return cur.lastrowid

def _add_thread(db, board_id, first_id, title):
    with sqlite3.connect(db) as conn:
        cur = conn.cursor()

        q = """
            SELECT seq FROM threads WHERE board_id = ? AND first_id = ?
        """

        cur.execute(q, (board_id, first_id))

        if cur.fetchone(): # dub in one board
            log.warning(f'found dub: fid:{first_id} on bid:{board_id}')
            return cur.lastrowid

        q = "INSERT INTO threads (board_id, first_id, title) VALUES (?, ?, ?)"
        cur.execute(q, (board_id, first_id, title))
        conn.commit()

        return cur.lastrowid

def _add_posts(db, board_id, thread_id, posts=[], path=''):
    with sqlite3.connect(db) as conn:
        cur = conn.cursor()

        for post in posts:
            q = """
                SELECT 
                    p.seq 
                FROM 
                    posts AS p
                    JOIN threads AS t ON p.thread_id = t.seq
                    JOIN boards AS b ON t.board_id = b.seq
                WHERE 
                    post_id = ? AND b.seq = ?
            """

            cur.execute(q, (post['id'], board_id))

            if cur.fetchone(): # dub in one board
                log.warning(f'found dub: tid:{thread_id} on bid:{board_id}')
                continue

            q = """
                INSERT INTO posts (board_id, thread_id, post_id, author, text, time)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            cur.execute(q, (board_id, thread_id, post['id'], post['author'], post['text'], post['time']))
            post_seq = cur.lastrowid

            for file in post['files']:
                file_data = None
                file_type = file['file_type'] or None
                file_name = file['url'].split('/')[-1]
                #file_id = int(file_name.split('.')[0])

                if path:
                    f_path = [f for f in path.iterdir() if f.is_file() and f.name == file_name]

                    if f_path:
                        f_path = f_path[0]
                        f_abspath = f_path.resolve()

                        kind = filetype.guess(f_abspath)
                        if kind:
                            file_type = kind.mime

                            with open(f_abspath, 'rb') as f:
                                file_data = f.read()

                q = """
                    INSERT INTO attachments (post_seq, file_type, file_name, file_url, thumb_url, file_data)
                    VALUES (?, ?, ?, ?, ?, ?)
                """
                cur.execute(q,              (post_seq, file_type, file_name, file['url'], file['thumb'], file_data))

def _find_board_by_name(db, name):
    board_id = None
    with sqlite3.connect(db) as conn:
        cur = conn.cursor()

        q = "SELECT seq FROM boards WHERE name = ?"
        cur.execute(q, (name,))
        r = cur.fetchone()
        
        if r:
            board_id = r[0]
    return board_id

def _test_thread(url: str):
    try:
        r = requests.get(url, timeout=10)
    except Exception as e:
        log.error(f"r ex: {e}")

    if r:
        log.info(url)
        try: 
            conv_r = parse_thread(r.text)
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
            conv_r = parse_catalog(r.text)
        except Exception as e:
            log.error(f"conv ex: {e}")
            return
        pp(conv_r)

#_test_thread("http://ii.yakuji.moe/b/res/4886591.html")
#_test_catalog('http://ii.yakuji.moe/b/')

def _find_thread_by_seq(db, board_id, thread_seq):
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT seq, title FROM threads WHERE seq = ?", (thread_seq,))
        thread = dict(cur.fetchone())

        q = """
            SELECT
                p.seq AS seq,
                p.post_id AS post_id,
                p.author,
                p.text,
                p.time,
                a.seq AS file_seq,
                a.file_url,
                a.file_name,
                a.thumb_url,
                a.file_type,
                a.file_data,
                b.name AS board
            FROM 
                posts p
                LEFT JOIN attachments a ON a.post_seq = p.seq
                JOIN threads t ON p.thread_id = t.seq
                JOIN boards b ON t.board_id = b.seq
            WHERE 
                p.thread_id = ? AND b.seq = ?
            ORDER BY 
                p.seq ASC;
        """

        cur.execute(q, (thread_seq, board_id))
        
        posts = {}
        for row in cur.fetchall():
            r = dict(row)
            pid = r.get('seq')
            if pid:
                posts[pid] = {
                    "author": r["author"],
                    "files": [],
                    "id": r["post_id"],
                    "index": len(posts) + 1,
                    "text": r["text"],
                    "time": r["time"],
                    "board": r["board"],
                    "source": "db"
                }

                if r["file_url"]:
                    f = {
                        "seq": r["file_seq"],
                        "url": r["file_url"],
                        "file_name": r["file_name"],
                        "thumb": r["thumb_url"],
                        "file_type": r["file_type"],
                        "file_data": r["file_data"] is not None
                    }

                    posts[pid]["files"].append(f)
        
        thread["posts"] = list(posts.values())
        
        return thread



async def _find_thread_by_post(db, board_id, post_id):
    thread_id = 0

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        q = """
            SELECT 
                p.thread_id 
            FROM 
                posts AS p
                JOIN threads AS t ON p.thread_id = t.seq
                JOIN boards AS b ON t.board_id = b.seq
            WHERE 
                post_id = ? AND b.seq = ?
        """

        cur.execute(q, (post_id, board_id))
        r = cur.fetchone()
        
        if r:
            thread_id = r[0]

    if thread_id:
        qs = await find_thread_by_seq(board_id, thread_id)
        return qs

async def serve_base64_file(file_seq: int):
    blob = await db.find_file_by_seq(file_seq)

    if not blob:
        log.warning(f"blob \'{file_seq}\' not found in db")
        return ''

    mime_type, image_data = blob
    r = util.image_from_bytes(image_data, mime_type)

    log.trace(f'{mime_type}: {len(image_data)}')

    return r

def _stats(db: str):
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        q = """
            SELECT 
                b.seq AS board_id,
                b.name AS board_name,
                (SELECT COUNT(*) FROM threads t WHERE t.board_id = b.seq) AS threads_count,
                (SELECT COUNT(*) FROM posts p WHERE p.board_id = b.seq) AS posts_count,
                (SELECT COUNT(*) FROM attachments a 
                    JOIN posts p ON a.post_seq = p.seq 
                    WHERE p.board_id = b.seq) AS attachments_count,
                (SELECT t.title FROM threads t 
                    WHERE t.board_id = b.seq 
                    ORDER BY t.seq DESC 
                    LIMIT 1) AS last_thread_title,
                (SELECT MAX(p.time) FROM posts p WHERE p.board_id = b.seq) AS last_post_time,
                (SELECT MIN(p.time) FROM posts p WHERE p.board_id = b.seq) AS first_post_time
            FROM boards b
            WHERE EXISTS (SELECT 1 FROM threads t WHERE t.board_id = b.seq)
            ORDER BY b.seq;
        """

        cur = conn.cursor()
        cur.execute(q)

        r = [dict(row) for row in cur.fetchall()]

        return r

def _find_posts_by_text(DB, TEXT, LIMIT=50, OFFSET=0, FTS=True, BOARDS=[]):
    with sqlite3.connect(DB) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        total_count = 0

        placeholders = ",".join(["?"] * len(BOARDS))

        sw = Stopwatch(3)
        sw.restart()

        if not FTS:
            q = f"""
                SELECT 
                    COUNT(*) AS total_count
                FROM 
                    posts AS p
                    JOIN threads AS t ON p.thread_id = t.seq
                    JOIN boards AS b ON t.board_id = b.seq
                WHERE 
                    p.text LIKE ? COLLATE NOCASE
                    AND b.seq IN ({placeholders})
            """

            cur.execute(q, [f"%{TEXT}%"] + BOARDS)
            r = cur.fetchall()
            total_count = dict(r[0])['total_count']

            q = f"""
                SELECT
                    p.seq,
                    p.post_id,
                    p.author,
                    p.text,
                    p.time,
                    a.seq AS file_seq,
                    a.file_url,
                    a.file_name,
                    a.thumb_url,
                    a.file_type,
                    a.file_data,
                    b.name AS board
                FROM 
                    posts AS p
                    LEFT JOIN attachments AS a ON a.post_seq = p.seq
                    JOIN threads AS t ON p.thread_id = t.seq
                    JOIN boards AS b ON t.board_id = b.seq
                WHERE 
                    p.text LIKE ? COLLATE NOCASE
                    AND b.seq IN ({placeholders})
                ORDER BY 
                    p.seq ASC
                LIMIT ? OFFSET ?;
            """

            cur.execute(q, [f"%{TEXT}%"] + BOARDS + [LIMIT, OFFSET])
        else:
            q = f"""
                SELECT 
                    COUNT(*) AS total_count
                FROM 
                    posts_fts
                    JOIN posts p ON posts_fts.rowid = p.seq
                    JOIN threads AS t ON p.thread_id = t.seq
                    JOIN boards AS b ON t.board_id = b.seq
                WHERE 
                    posts_fts
                MATCH 
                    ?
                AND
                    b.seq IN ({placeholders})
            """

            cur.execute(q, [TEXT] + BOARDS)
            r = cur.fetchall()
            total_count = dict(r[0])['total_count']

            q = f"""
                SELECT
                    p.seq,
                    p.post_id,
                    p.author,
                    p.text,
                    p.time,
                    a.seq AS file_seq,
                    a.file_url,
                    a.file_name,
                    a.thumb_url,
                    a.file_type,
                    a.file_data,
                    b.name AS board
                FROM 
                    posts_fts
                    JOIN posts p ON posts_fts.rowid = p.seq
                    LEFT JOIN attachments a ON a.post_seq = p.seq
                    JOIN threads t ON p.thread_id = t.seq
                    JOIN boards b ON t.board_id = b.seq
                WHERE 
                    posts_fts MATCH ?
                    AND b.seq IN ({placeholders})
                ORDER BY 
                    p.seq ASC
                LIMIT ? OFFSET ?;
            """
            cur.execute(q, [TEXT] + BOARDS + [LIMIT, OFFSET])

        sw.stop()

        posts = {}
        for row in cur.fetchall():
            r = dict(row)
            pid = r.get('seq')
            if pid:
                posts[pid] = {
                    "author": r["author"],
                    "files": [],
                    "id": r["post_id"],
                    "index": int(OFFSET) + len(posts) + 1,
                    "text": r["text"],
                    "time": r["time"],
                    "board": r["board"],
                    "source": "db"
                }
                if r["file_url"]:
                    posts[pid]["files"].append({
                        "seq": r["file_seq"],
                        "url": r["file_url"],
                        "thumb": r["thumb_url"],
                        "file_name": r["file_name"],
                        "file_type": r["file_type"],
                        "file_data": r["file_data"] is not None
                    })

        r = list(posts.values())
        log.trace(f"{TEXT}: {len(r)} in {str(sw)}")

        return total_count, r

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
