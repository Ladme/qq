# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import logging
import os

from qq_lib.env_vars import DEBUG_MODE

LOG_FORMAT = "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str | None = None, log_file: str | None = None) -> logging.Logger:
    """
    Return a logger with unified formatting.

    Logs are written to stderr by default. If log_file is provided, logs will
    also be written to the specified file.
    """
    logger = logging.getLogger(name or __name__)

    if not logger.handlers:
        if os.environ.get(DEBUG_MODE) is not None:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)

        # always write to console
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

        # write to a log file if specified
        if log_file:
            fh = logging.FileHandler(log_file, mode="a")
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(formatter)
            logger.addHandler(fh)

    return logger
