import csv
import sqlite3
import logging
from .sql import execute_sql

logger = logging.getLogger(__name__)


def insert_csv_into_db(
    csv_file_path: str,
    db_path: str,
    table_name: str,
):
    """Insert a csv file into a sqlite db

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
        header = header_row
        columns = ", ".join(header)
        placeholders = ", ".join("?" * len(header))
        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        for row in reader:
            cur.execute(insert_query, row)

    conn.commit()
    conn.close()
    logger.debug(f"inserted {csv_file_path} into {db_path}")


def insert_csv_into_db_w_vintage(
    csv_file_path: str, db_path: str, table_name: str, vintage: str
):
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
    execute_sql(f"DELETE FROM {table_name}")
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
    try:
        column_str = " TEXT, ".join(columns)
        sql = f"CREATE TABLE {table_name} ({column_str})"
        execute_sql(db_path, sql)
        logger.debug(f"created db at {db_path}")
    except sqlite3.OperationalError:
        logger.error(f"db({db_path}) and table({table_name}) already exists")
        logger.info("try running with the -f/--force flag to recreate the database")
        raise SystemExit(1)


def export_table_to_csv(db_path: str, table_name: str, csv_file_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute(f"SELECT * FROM {table_name}")
    rows = cur.fetchall()

    column_names = [description[0] for description in cur.description]

    # Write the data to a CSV file
    with open(csv_file_path, "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(column_names)  # write the header
        writer.writerows(rows)  # write the data

    conn.close()

    logger.debug(f"Exported data from {table_name} to {csv_file_path}")
