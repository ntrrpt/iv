import util, yk
from loguru import logger as log
from stopwatch import Stopwatch
import sqlite3, os, requests, filetype
from pprint import pprint as pp, pformat as pf
from pathlib import Path

def init(db: str):
    schema = """
        CREATE TABLE IF NOT EXISTS boards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT
        );

        CREATE TABLE IF NOT EXISTS threads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            board_id INTEGER NOT NULL,
            title TEXT,
            FOREIGN KEY(board_id) REFERENCES boards(id)
        );

        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            thread_id INTEGER NOT NULL,
            post_id INTEGER NOT NULL UNIQUE,
            author TEXT,
            text TEXT NOT NULL,
            time INTEGER,
            FOREIGN KEY(thread_id) REFERENCES threads(id)
        );

        CREATE TABLE IF NOT EXISTS attachments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            file_type TEXT,
            file_name TEXT,
            file_url TEXT,
            thumb_url TEXT,
            file_data BLOB,
            FOREIGN KEY(post_id) REFERENCES posts(post_id)
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS posts_fts USING fts5 (
            text, content='posts', content_rowid='id'
        );

        CREATE TRIGGER IF NOT EXISTS posts_ai AFTER INSERT ON posts BEGIN
            INSERT INTO posts_fts(rowid, text) VALUES (new.id, new.text);
        END;

        CREATE TRIGGER IF NOT EXISTS posts_au AFTER UPDATE ON posts BEGIN
            UPDATE posts_fts SET text = new.text WHERE rowid = new.id;
        END;

        CREATE TRIGGER IF NOT EXISTS posts_ad AFTER DELETE ON posts BEGIN
            DELETE FROM posts_fts WHERE rowid = old.id;
        END;
    """

    with sqlite3.connect(db) as conn:
        conn.executescript(schema)
        conn.commit()

def add_board(db, name, description=''):
    with sqlite3.connect(db) as conn:
        cur = conn.cursor()

        q = "INSERT INTO boards (name, description) VALUES (?, ?)"
        cur.execute(q, (name, description))
        conn.commit()

        log.success(f'board {name} created')

        return cur.lastrowid

def add_thread(db, board_id, title):
    with sqlite3.connect(db) as conn:
        cur = conn.cursor()

        q = "INSERT INTO threads (board_id, title) VALUES (?, ?)"
        cur.execute(q, (board_id, title))
        conn.commit()

        return cur.lastrowid

def add_post(db, thread_id, post, path=''):
    with sqlite3.connect(db) as conn:
        cur = conn.cursor()

        q = "SELECT id FROM posts WHERE post_id = ?"
        cur.execute(q, (post['id'],))

        if not cur.fetchone():
            for file in post['files']:
                file_data = None
                file_type = file['file_type'] or None
                file_name = file['url'].split('/')[-1]
                #file_id = int(file_name.split('.')[0])

                if path:
                    f_path = [f for f in path.iterdir() if f.is_file() and f.name == f_url]

                    if f_path:
                        f_path = f_path[0]
                        f_abspath = f_path.resolve()

                        kind = filetype.guess(f_abspath)
                        if kind:
                            file_type = kind.mime

                            with open(f_abspath, 'rb') as f:
                                file_data = f.read()

                q = """
                    INSERT INTO attachments (post_id, file_type, file_name, file_url, thumb_url, file_data)
                    VALUES (?, ?, ?, ?, ?, ?)
                """
                cur.execute(q, (post.get('id'), file_type, file_name, file['url'], file['thumb'], file_data))

            q = """
                INSERT INTO posts (thread_id, post_id, author, text, time)
                VALUES (?, ?, ?, ?, ?)
            """
            cur.execute(q, (thread_id, post['id'], post.get('author'), post.get('text'), post.get('time')))

def find_board_by_name(db, name):
    board_id = 0
    with sqlite3.connect(db) as conn:
        cur = conn.cursor()

        q = "SELECT id FROM boards WHERE name = ?"
        cur.execute(q, (name,))
        r = cur.fetchone()
        
        if r:
            board_id = r[0]
    return board_id

def find_thread_by_id(db, thread_id):
    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT id, title FROM threads WHERE id = ?", (thread_id,))
        thread = dict(cur.fetchone())

        q = """
            SELECT
                p.id AS id,
                p.post_id AS post_id,
                p.author,
                p.text,
                p.time,
                a.file_url,
                a.file_name,
                a.thumb_url,
                a.file_type,
                a.file_data,
                b.name AS board
            FROM 
                posts p
                LEFT JOIN attachments a ON a.post_id = p.post_id
                JOIN threads t ON p.thread_id = t.id
                JOIN boards b ON t.board_id = b.id
            WHERE 
                p.thread_id = ?
                ORDER BY p.id ASC;
        """

        cur.execute(q, (thread_id,))
        
        posts = {}
        for row in cur.fetchall():
            r = dict(row)
            pid = r.get('id')
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
                        "url": r["file_url"],
                        "file_name": r["file_name"],
                        "thumb": r["thumb_url"],
                        "file_type": r["file_type"],
                        "has_blob": r["file_data"] is not None
                    }

                    posts[pid]["files"].append(f)
        
        thread["posts"] = list(posts.values())
        
        return thread

def find_thread_by_post(db, post_id):
    thread_id = 0

    with sqlite3.connect(db) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        q = "SELECT thread_id FROM posts WHERE post_id = ?"
        cur.execute(q, (post_id,))
        r = cur.fetchone()
        
        if r:
            thread_id = r[0]

    if thread_id:
        return find_thread_by_id(db, thread_id)

def find_posts_by_text(DB, TEXT, LIMIT=50, OFFSET=0, FTS=True):
    with sqlite3.connect(DB) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        count = 0

        sw = Stopwatch(3)
        sw.restart()

        if not FTS:
            q = """
                SELECT 
                    COUNT(*) AS total_count
                FROM 
                    posts AS p
                WHERE 
                    p.text LIKE ? COLLATE NOCASE;
            """
            
            cur.execute(q, (f"%{TEXT}%",))
            r = cur.fetchall()
            count = dict(r[0])['total_count']

            q = f"""
                SELECT
                    p.id AS id,
                    p.post_id AS post_id,
                    p.author,
                    p.text,
                    p.time,
                    a.file_url,
                    a.file_name,
                    a.thumb_url,
                    a.file_type,
                    a.file_data,
                    b.name AS board
                FROM 
                    posts AS p
                    LEFT JOIN attachments AS a ON a.post_id = p.post_id
                    JOIN threads AS t ON p.thread_id = t.id
                    JOIN boards AS b ON t.board_id = b.id
                WHERE 
                    p.text LIKE ? COLLATE NOCASE
                    ORDER BY p.id ASC
                    LIMIT ? OFFSET ?;
            """

            cur.execute(q, (f"%{TEXT}%", LIMIT, OFFSET))
        else:
            q = """
                SELECT 
                    COUNT(*) AS total_count
                FROM 
                    posts_fts
                    JOIN posts p ON posts_fts.rowid = p.id
                WHERE 
                    posts_fts MATCH ?
            """

            cur.execute(q, (TEXT,))
            r = cur.fetchall()
            count = dict(r[0])['total_count']

            q = """
                SELECT
                    p.id,
                    p.post_id,
                    p.author,
                    p.text,
                    p.time,
                    a.file_url,
                    a.file_name,
                    a.thumb_url,
                    a.file_type,
                    a.file_data,
                    b.name AS board
                FROM 
                    posts_fts
                    JOIN posts p ON posts_fts.rowid = p.id
                    LEFT JOIN attachments a ON a.post_id = p.post_id
                    JOIN threads t ON p.thread_id = t.id
                    JOIN boards b ON t.board_id = b.id
                WHERE 
                    posts_fts MATCH ?
                    ORDER BY p.id ASC
                    LIMIT ? OFFSET ?;
            """
            cur.execute(q, (TEXT, LIMIT, OFFSET))

        sw.stop()

        posts = {}
        for row in cur.fetchall():
            r = dict(row)
            pid = r.get('id')
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
                        "url": r["file_url"],
                        "thumb": r["thumb_url"],
                        "file_type": r["file_type"],
                        "has_blob": r["file_data"] is not None
                    })

        r = list(posts.values())
        log.trace(f"{TEXT}: {len(r)} in {str(sw)}")

        return count, r