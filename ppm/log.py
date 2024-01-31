import logging

LOG_FORMAT = "%(levelname)s: %(message)s"

_levels = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
}

log_levels = _levels.keys()


def log_level_type(string: str):
    """Determine if a string is a valid log level"""
    if string in log_levels:
        return string
    else:
        raise SystemExit(f"{string} is not a valid log level")


def get_log_level(requested_level: str):
    """Take a string and return the corresponding logging level.

    Args:
        requested_level (str): the user input log level

    Raises:
        ValueError: the requested level is not in the list of valid levels

    Returns:
        logging.LEVEL: the logging level corresponding to the requested level
    """
    level = _levels.get(requested_level.lower())
    if level is None:
        raise ValueError(
            f"log level given: {requested_level}"
            f" -- must be one of: {' | '.join(log_levels)}"
        )
    return level
