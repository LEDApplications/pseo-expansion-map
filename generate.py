#!/usr/bin/env python
import re
import argparse
import logging
from ppm.log import (
    LOG_FORMAT,
    log_levels,
    log_level_type,
    get_log_level,
)
from ppm.download import get_links_in_table, download_url_to_file
from ppm.zip import ungz
from ppm.db import get_header_row, create_db_table_if_not_exists, insert_csv_into_db

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
    all_file_links = [
        ALL_FILE_URL_FORMAT.replace("VINTAGE", vintage) for vintage in vintages
    ]

    # TODO use actual temp dir
    temp_dir = "./tmp"

    # TODO rm hardcode
    # shape_zip = download_url_to_file(all_file_links[0], temp_dir)
    csv_gz = "/home/jody/workspace/language/py/pseo-partners-map/tmp/pseoe_all.csv.gz"

    # TODO rme hardcode
    # csv = ungz(csv_gz)
    csv = "/home/jody/workspace/language/py/pseo-partners-map/tmp/pseoe_all.csv"

    # TODO validate that each csv has the same header row
    header_row = get_header_row(csv)

    # prepend the vintage to the header row
    header_row = ["vintage"] + header_row

    db_path = "./tmp/pseo.db"
    table_name = "pseoe_public"
    create_db_table_if_not_exists(db_path, table_name, header_row)
    # TODO fix vintage hardcode
    insert_csv_into_db(csv, db_path, table_name, "R2020Q4")


if __name__ == "__main__":
    main()
