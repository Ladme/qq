# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import logging
import os

from qq_lib.env_vars import DEBUG_MODE

DEBUG_LOG_FORMAT = "[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s"
LOG_FORMAT = "[%(name)s] [%(levelname)s] %(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


class ColoredFormatter(logging.Formatter):
    """Custom formatter to color the log level based on severity."""

    COLOR_CODES = {
        "DEBUG": "",  # default
        "INFO": "\033[32m",  # green
        "WARNING": "\033[33m",  # yellow
        "ERROR": "\033[31m",  # red
        "CRITICAL": "\033[31m",  # red
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        levelname = record.levelname
        color = self.COLOR_CODES.get(levelname, self.RESET)
        record.levelname = f"{color}{levelname}{self.RESET}"
        return super().format(record)


def get_logger(name: str | None = None, colored: bool = False) -> logging.Logger:
    """
    Return a logger with unified formatting.

    Logs are written to stderr.
    If colored=True, the log level is colored based on severity:
      - DEBUG → default
      - INFO → green
      - WARNING → yellow
      - ERROR/CRITICAL → red
    """
    logger = logging.getLogger(name or __name__)

    if not logger.handlers:
        debug_mode = os.environ.get(DEBUG_MODE) is not None

        logger.setLevel(logging.DEBUG if debug_mode else logging.INFO)

        fmt = DEBUG_LOG_FORMAT if debug_mode else LOG_FORMAT
        formatter = (
            ColoredFormatter(fmt=fmt, datefmt=DATE_FORMAT)
            if colored
            else logging.Formatter(fmt=fmt, datefmt=DATE_FORMAT)
        )

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    return logger
