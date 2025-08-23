import util, yk
from loguru import logger as log
from stopwatch import Stopwatch
import sqlite3, os, requests, filetype
from pprint import pprint as pp, pformat as pf
from pathlib import Path

from tortoise import fields, connections
from tortoise.transactions import in_transaction
from tortoise.expressions import Q, Case, When, Value
from tortoise.functions import Count
from tortoise.models import Model

log.add('db.txt')

async def init(DB: str) -> None:
    await Tortoise.init(db_url=f'sqlite://{DB}', modules={'models': ['db']})
    await Tortoise.generate_schemas()

async def close() -> None:
    await Tortoise.close_connections()

class Board(Model):
    seq = fields.IntField(pk=True)
    name = fields.CharField(max_length=255, unique=True)
    description = fields.TextField(null=True)

    class Meta:
        table = "boards"

class Thread(Model):
    seq = fields.IntField(pk=True)
    board = fields.ForeignKeyField("models.Board", related_name="threads", on_delete=fields.CASCADE)
    first_id = fields.IntField()
    title = fields.CharField(max_length=255, null=True)

    class Meta:
        table = "threads"
        unique_together = (("board", "first_id"),)

class Post(Model):
    seq = fields.IntField(pk=True)
    board = fields.ForeignKeyField("models.Board", related_name="posts", on_delete=fields.CASCADE)
    thread = fields.ForeignKeyField("models.Thread", related_name="posts", on_delete=fields.CASCADE)
    post_id = fields.IntField()
    author = fields.CharField(max_length=255, null=True)
    text = fields.TextField()
    time = fields.IntField(null=True)

    class Meta:
        table = "posts"
        unique_together = (("board", "post_id"),)

class Attachment(Model):
    seq = fields.IntField(pk=True)
    post_seq = fields.ForeignKeyField("models.Post", related_name="attachments", on_delete=fields.CASCADE, source_field="post_seq")
    file_type = fields.CharField(max_length=100, null=True)
    file_name = fields.CharField(max_length=255, null=True)
    file_url = fields.CharField(max_length=500, null=True)
    thumb_url = fields.CharField(max_length=500, null=True)
    file_data = fields.BinaryField(null=True)

    class Meta:
        table = "attachments"

async def find_file_by_seq(post_seq: int):
    qs = Attachment.filter(seq=post_seq).values_list("file_type", "file_data", flat=False)
    r = await qs
    return r[0] if r else None

async def find_board_by_name(name: str) -> int | None:
    qs = Board.filter(name=name).only("seq").first()
    r = await qs
    return r.seq if r else None

async def find_thread_by_seq(board_id: int, thread_seq: int):
    thread = await Thread.filter(seq=thread_seq, board_id=board_id).only("seq", "title").first()
    if not thread:
        return None

    thread_dict = {"seq": thread.seq, "title": thread.title}

    posts_dict = {}
    posts_qs = (
        Post.filter(thread_id=thread_seq, board_id=board_id)
        .prefetch_related("board")
        .order_by("seq")
    )

    async for post in posts_qs:
        posts_dict[post.seq] = {
            "author": post.author,
            "files": [],
            "id": post.post_id,
            "index": len(posts_dict) + 1,
            "text": post.text,
            "time": post.time,
            "board": post.board.name,
            "source": "db",
        }

    attachments_qs = (
        Attachment.filter(post_seq__seq__in=list(posts_dict.keys()))
        .annotate(
            has_file_data=Case(
                When(file_data__not_isnull=True, then=Value(True)),
                default=Value(False)
            )
        )
        .values(
            "seq",
            "post_seq__seq",
            "file_url",
            "file_name",
            "thumb_url",
            "file_type",
            "has_file_data"
        )
    )

    async for a in attachments_qs:
        if a["file_url"]:
            posts_dict[a["post_seq__seq"]]["files"].append({
                "seq": a["seq"],
                "url": a["file_url"],
                "file_name": a["file_name"],
                "thumb": a["thumb_url"],
                "file_type": a["file_type"],
                "file_data": bool(a["has_file_data"])
            })

    thread_dict["posts"] = list(posts_dict.values())

    return thread_dict

async def find_thread_by_post(board_id: int, post_id: int):
    post = await Post.filter(
        Q(post_id=post_id) & Q(board__seq=board_id)
    ).select_related("thread").first()

    if not post:
        return None

    thread_id = post.thread.seq

    return await find_thread_by_seq(board_id, thread_id)

async def find_posts_by_text(TEXT: str, LIMIT: int = 50, OFFSET: int = 0, BM25: bool = True, FTS: bool = False, BOARDS: list[int] = []):
    log.warning(BM25)
    total_count = 0
    posts = []

    sw = Stopwatch(3)
    sw.restart()

    if not FTS:
        # === LIKE-search via ORM ===
        total_count = await Post.filter(
            Q(text__icontains=TEXT) & Q(thread__board_id__in=BOARDS)
        ).count()

        query = Post.filter(
            Q(text__icontains=TEXT) & Q(thread__board_id__in=BOARDS)
        ).prefetch_related("attachments", "thread__board").order_by("seq").offset(OFFSET).limit(LIMIT)

        result = await query

        for idx, post in enumerate(result, start=1 + OFFSET):
            posts.append({
                "author": post.author,
                "id": post.post_id,
                "index": idx,
                "text": post.text,
                "time": post.time,
                "board": post.thread.board.name,
                "source": "db",
                "files": [
                    {
                        "seq": att.seq,
                        "url": att.file_url,
                        "thumb": att.thumb_url,
                        "file_name": att.file_name,
                        "file_type": att.file_type,
                        "file_data": att.file_data is not None
                    }
                    for att in post.attachments
                ]
            })

    else:
        # === FTS5 via raw SQL ===
        placeholders = ",".join(["?"] * len(BOARDS))
        async with in_transaction() as conn:
            count_sql = f"""
                SELECT COUNT(*) AS total_count
                FROM posts_fts
                JOIN posts p ON posts_fts.rowid = p.seq
                JOIN threads t ON p.thread_id = t.seq
                JOIN boards b ON t.board_id = b.seq
                WHERE posts_fts MATCH ?
                AND b.seq IN ({placeholders})
            """
            count_params = [TEXT] + BOARDS
            rows = await conn.execute_query(count_sql, count_params)
            total_count = rows[1][0]["total_count"]

            # Основной запрос
            query_sql = f"""
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
                    b.name AS board,
                    bm25(posts_fts) AS rank
                FROM posts_fts
                JOIN posts p ON posts_fts.rowid = p.seq
                LEFT JOIN attachments a ON a.post_seq = p.seq
                JOIN threads t ON p.thread_id = t.seq
                JOIN boards b ON t.board_id = b.seq
                WHERE posts_fts MATCH ?
                AND b.seq IN ({placeholders})
                ORDER BY {'rank' if BM25 else 'p.seq'} ASC
                LIMIT ? OFFSET ?;
            """
            params = [TEXT] + BOARDS + [LIMIT, OFFSET]
            result = await conn.execute_query(query_sql, params)

            posts_dict = {}
            for row in result[1]:
                pid = row["seq"]
                if pid not in posts_dict:
                    posts_dict[pid] = {
                        "author": row["author"],
                        "id": row["post_id"],
                        "index": len(posts_dict) + OFFSET + 1,
                        "text": row["text"],
                        "time": row["time"],
                        "board": row["board"],
                        "source": "db",
                        "files": []
                    }
                if row["file_url"]:
                    posts_dict[pid]["files"].append({
                        "seq": row["file_seq"],
                        "url": row["file_url"],
                        "thumb": row["thumb_url"],
                        "file_name": row["file_name"],
                        "file_type": row["file_type"],
                        "file_data": row["file_data"] is not None
                    })
            posts = list(posts_dict.values())

    sw.stop()
    log.trace(f"{TEXT}: {len(posts)} in {str(sw)}")
    return total_count, posts

def create(db: str):
    schema = """
        CREATE TABLE IF NOT EXISTS boards (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            description TEXT
        );

        CREATE TABLE IF NOT EXISTS threads (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            board_id INTEGER NOT NULL,
            first_id INTEGER NOT NULL,
            title TEXT,
            FOREIGN KEY(board_id) REFERENCES boards(seq),
            UNIQUE(board_id, first_id)
        );

        CREATE TABLE IF NOT EXISTS posts (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            board_id INTEGER NOT NULL,
            thread_id INTEGER NOT NULL,
            post_id INTEGER NOT NULL,
            author TEXT,
            text TEXT NOT NULL,
            time INTEGER,
            FOREIGN KEY(thread_id) REFERENCES threads(seq),
            FOREIGN KEY(board_id) REFERENCES boards(seq),
            UNIQUE(board_id, post_id)
        );

        CREATE TABLE IF NOT EXISTS attachments (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            post_seq INTEGER NOT NULL,
            file_type TEXT,
            file_name TEXT,
            file_url TEXT,
            thumb_url TEXT,
            file_data BLOB,
            FOREIGN KEY(post_seq) REFERENCES posts(seq)
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS posts_fts USING fts5 (
            text, content='posts', content_rowid='seq'
        );

        CREATE TRIGGER IF NOT EXISTS posts_ai AFTER INSERT ON posts BEGIN
            INSERT INTO posts_fts(rowid, text) VALUES (new.seq, new.text);
        END;

        CREATE TRIGGER IF NOT EXISTS posts_au AFTER UPDATE ON posts BEGIN
            UPDATE posts_fts SET text = new.text WHERE rowid = new.seq;
        END;

        CREATE TRIGGER IF NOT EXISTS posts_ad AFTER DELETE ON posts BEGIN
            DELETE FROM posts_fts WHERE rowid = old.seq;
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

        log.trace(f'board {name} created')

        return cur.lastrowid

def add_thread(db, board_id, first_id, title):
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

def add_posts(db, board_id, thread_id, posts=[], path=''):
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

async def stats():
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

    conn = connections.get("default")
    rows = await conn.execute_query_dict(q)
    return rows