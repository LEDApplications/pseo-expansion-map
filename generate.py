#!/usr/bin/env python
import os
import re
import argparse
import logging
import tempfile
from ppm.log import (
    LOG_FORMAT,
    log_levels,
    log_level_type,
    get_log_level,
)
from ppm.download import get_links_in_table, download_url_to_file
from ppm.zip import ungz
from ppm.db import get_header_row, init_db_table, insert_csv_into_db_w_vintage
from ppm.sql import execute_sql

logger = logging.getLogger(__name__)

PSEO_PUBLICATION_URL = "https://lehd.ces.census.gov/data/pseo/"
ALL_FILE_URL_FORMAT = (
    "https://lehd.ces.census.gov/data/pseo/VINTAGE/all/pseoe_all.csv.gz"
)


def parse_args():
    parser = argparse.ArgumentParser(description="Perform a task")
    parser.add_argument(
        "-r",
        "--run",
        help="run the process",
        action="store_true",
        required=True,
    )
    parser.add_argument(
        "-l",
        "--loglevel",
        default="info",
        help="set logging verbosity",
        required=False,
        type=log_level_type,
        choices=log_levels,
    )
    parser.add_argument(
        "-d",
        "--database",
        help="sqlite intermediate database location",
        required=False,
        type=str,
    )
    parser.add_argument(
        "-f",
        "--force",
        help="delete and recreate the intermediate database",
        action="store_true",
        required=False,
    )

    return parser.parse_args()


def execute_db_sql(db_path: str):
    # trim down only what's needed, sum the grad counts
    sql = """
        --sql
        drop table if exists ipeds_count_long;
        """
    execute_sql(db_path, sql)
    sql = """
        --sql
        create table ipeds_count_long as
            SELECT
                vintage,
                institution,
                degree_level,
                cast(y1_ipeds_count as integer) + cast(y5_ipeds_count as integer) + cast(y10_ipeds_count as integer) as grad_count
            FROM pseoe_public
            WHERE
                inst_level = 'I' AND -- single institution
                grad_cohort = '0000' AND -- all cohorts
                cipcode = '00' -- all instructional programs
            ORDER BY
                vintage,
                institution,
                degree_level;
        """
    execute_sql(db_path, sql)

    # determine the first vintage per inst x degree level
    sql = """
        --sql
        drop table if exists ipeds_count;
        """
    execute_sql(db_path, sql)
    sql = """
        --sql
        CREATE TABLE ipeds_count AS
        with earliest_vintage as (
            SELECT min(vintage) as vintage, institution, degree_level
            FROM ipeds_count_long
            GROUP BY
                institution,
                degree_level
        )
        SELECT
            v.vintage,
            v.institution,
            v.degree_level,
            d.grad_count
        FROM earliest_vintage v
        LEFT JOIN ipeds_count_long d
            USING (vintage, institution, degree_level);
        """
    execute_sql(db_path, sql)

    # update any 0 count values with the earliest available count
    sql = """
        --sql
        with earliest_valid_ipeds_vintage as (
            SELECT min(vintage) as vintage, institution, degree_level
            FROM ipeds_count_long
            WHERE grad_count <> 0
            GROUP BY
                institution,
                degree_level
        ), earliest_valid_ipeds_data as (
            SELECT
                v.vintage,
                v.institution,
                v.degree_level,
                d.grad_count
            FROM earliest_valid_ipeds_vintage v
            LEFT JOIN
                ipeds_count_long d
            USING (vintage, institution, degree_level)
        ) update ipeds_count
          set grad_count = d.grad_count
            from earliest_valid_ipeds_data d
            where
                ipeds_count.institution = d.institution and
                ipeds_count.degree_level = d.degree_level and
                ipeds_count.grad_count = 0;
        """
    execute_sql(db_path, sql)

    # good enough, delete rows with 0 grads
    sql = """
        --sql
        delete
        from ipeds_count
        where grad_count = 0;
        """
    execute_sql(db_path, sql)

    # convert year-quarter into yyyy-mm
    sql = """
        --sql
        ALTER TABLE ipeds_count
            ADD COLUMN year text;
        """
    execute_sql(db_path, sql)

    sql = """
        --sql
        ALTER TABLE ipeds_count
            ADD COLUMN quarter text;
        """
    execute_sql(db_path, sql)

    sql = """
        --sql
        UPDATE ipeds_count
            set year = substr(vintage,2,4),
                quarter =substr(vintage,7,7);
        """
    execute_sql(db_path, sql)

    sql = """
        --sql
        ALTER TABLE ipeds_count
            ADD COLUMN month text;
        """
    execute_sql(db_path, sql)

    sql = """
        --sql
        update ipeds_count
            set month = CASE quarter
                            WHEN '1' THEN '01'
                            WHEN '2' THEN '04'
                            WHEN '3' THEN '07'
                            WHEN '4' THEN '10'
                        END;
        """
    execute_sql(db_path, sql)

    sql = """
        --sql
        alter table ipeds_count
            ADD COLUMN date_release text;
        """
    execute_sql(db_path, sql)

    sql = """
        --sql
        update ipeds_count
            SET date_release = year || '-' || month;
        """
    execute_sql(db_path, sql)


def main():
    inputs = parse_args()
    logging.basicConfig(level=get_log_level(inputs.loglevel), format=LOG_FORMAT)

    # if the database exists and the force flag is set, delete it
    if inputs.force and inputs.database:
        if os.path.isfile(inputs.database):
            os.remove(inputs.database)

    logger.info("Creating the pseo-partners-map data")

    # get the list of published vintages from the release page
    links = get_links_in_table(PSEO_PUBLICATION_URL)
    vintages = [
        item.rstrip("/") for item in links if re.match(r"^R\d{4}Q[1-4]/$", item)
    ]
    url_vintage_list = zip(
        [ALL_FILE_URL_FORMAT.replace("VINTAGE", vintage) for vintage in vintages],
        vintages,
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        # You can use 'temp_dir' here. It will be automatically deleted when the 'with' block ends.
        logger.debug(f"temp directory path is {temp_dir}")

        if inputs.database:
            db_path = inputs.database
        else:
            db_path = f"{temp_dir}/pseo.db"

        logger.info("downloading and extracting")

        # download and extract all the csv files
        # since we'll need to unify the columns since they change over time
        csv_vintage_list = []
        for url, vintage in url_vintage_list:
            destination_file_name = f"pseoe_all_{vintage}.csv.gz"
            csv_gz = download_url_to_file(url, temp_dir, destination_file_name)
            csv = ungz(csv_gz)
            csv_vintage_list.append((csv, vintage))

        # unify the headers
        unified_header = set()
        for csv, _ in csv_vintage_list:
            header_row = get_header_row(csv)
            unified_header.update(header_row)

        # create the db table and schema with an added "vintage" column
        header_row = ["vintage"] + list(unified_header)
        init_db_table(db_path, "pseoe_public", header_row)

        logger.info(f"loading sqlite database")
        for csv, vintage in csv_vintage_list:
            logger.debug(f"inserting {vintage} into {db_path}")
            insert_csv_into_db_w_vintage(csv, db_path, "pseoe_public", vintage)

        logger.info(f"executing sql process")
        execute_db_sql(db_path)

    logger.info("done.")


if __name__ == "__main__":
    main()
