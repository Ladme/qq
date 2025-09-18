# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import logging
from typing import Union

LOG_FORMAT = "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: Union[str, None] = None):
    """Return a logger with unified formatting."""
    logger = logging.getLogger(name or __name__)

    if not logger.handlers:
        logger.setLevel(logging.DEBUG)

        # Console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)
        ch.setFormatter(formatter)

        logger.addHandler(ch)

    return logger
