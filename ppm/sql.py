import csv
import sqlite3
import logging

logger = logging.getLogger(__name__)


def execute_sql(db_path: str, query: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    conn.close()
