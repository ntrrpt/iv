'''

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
