import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras

DB_DSN = os.environ.get(
    "PIPELINE_DB_DSN",
    "dbname=news_pipeline user=postgres password=pipeline_dev_pw host=localhost port=5433",
)


@contextmanager
def get_conn():
    conn = psycopg2.connect(DB_DSN)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def get_dict_cursor(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield cur
    finally:
        cur.close()
