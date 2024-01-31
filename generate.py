#!/usr/bin/env python
import argparse
import logging
from ppm.log import (
    LOG_FORMAT,
    log_levels,
    log_level_type,
    get_log_level,
)

logger = logging.getLogger(__name__)


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


if __name__ == "__main__":
    main()
