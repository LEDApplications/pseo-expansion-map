#!/usr/bin/env python
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
from ppm.db import get_header_row, init_db_table, insert_csv_into_db

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

    return parser.parse_args()


def main():
    inputs = parse_args()
    logging.basicConfig(level=get_log_level(inputs.loglevel), format=LOG_FORMAT)

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

    db_path = "./tmp/pseo.db"
    table_name = "pseoe_public"
    with tempfile.TemporaryDirectory() as temp_dir:
        # You can use 'temp_dir' here. It will be automatically deleted when the 'with' block ends.

        logger.debug(f"temp directory path is {temp_dir}")

        # download and extract all the csv files
        # since we'll need to unify the columns since they change over time
        csv_vintage_list = []
        for url, vintage in url_vintage_list:
            logger.info(f"loading {vintage} data")

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
        init_db_table(db_path, table_name, header_row)

        for csv, vintage in csv_vintage_list:
            logger.info(f"inserting {vintage} into {db_path}")
            insert_csv_into_db(csv, db_path, table_name, vintage)

    logger.info("done.")


if __name__ == "__main__":
    main()
