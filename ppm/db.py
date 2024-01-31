import os
import csv
import sqlite3
import logging

logger = logging.getLogger(__name__)


def insert_csv_into_db(csv_file_path: str, db_path: str, table_name: str, vintage: str):
    """Insert a csv file into a sqlite db with a specific vintage column

    Args:
        csv_file_path (str): path to the csv file
        db_path (str): path to the sqlite db
        table_name (str): name of the table
    """
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    with open(csv_file_path, "r", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file)
        header_row = next(reader)
        header = ["vintage"] + header_row
        columns = ", ".join(header)
        placeholders = ", ".join("?" * len(header))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        for row in reader:
            insert_row = [vintage] + row
            cur.execute(insert_query, insert_row)

    conn.commit()
    conn.close()
    logger.debug(f"inserted {csv_file_path} into {db_path} with vintage {vintage}")


def get_header_row(csv_file_path: str):
    with open(csv_file_path, "r", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file)
        header_row = next(reader)

    return header_row


def truncate_table(db_path: str, table_name: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(f"DELETE FROM {table_name}")
    conn.commit()
    conn.close()
    logger.debug(f"truncated table {table_name} in {db_path}")


def init_db_table(db_path: str, table_name: str, columns: list):
    """
        Create a sqlite db of with a single table and text columns
        if it already exists, truncate the table


    Args:
        db_path (str): path to the sqlite db
        table_name (str): name of the table
        columns (list): list of column names
    """

    # Check if database exists, if not, create it
    if not os.path.isfile(db_path):
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        column_str = " TEXT, ".join(columns)
        cur.execute(f"CREATE TABLE {table_name} ({column_str})")
        conn.commit()
        conn.close()
        logger.debug(f"created db at {db_path}")
    else:
        logger.debug(f"db at {db_path} already exists")
        truncate_table(db_path, table_name)
