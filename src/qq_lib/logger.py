# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import logging
import os

from rich.logging import RichHandler
from rich.console import Console

from qq_lib.env_vars import DEBUG_MODE

LOG_FORMAT = "%(message)s"
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def get_logger(name: str) -> logging.Logger:
    """
    Return a logger with unified formatting.
    If colored=True, use rich's RichHandler with colored levels.
    """
    logger = logging.getLogger(name)

    debug_mode = os.environ.get(DEBUG_MODE) is not None
    logger.setLevel(logging.DEBUG if debug_mode else logging.INFO)

    console = Console(stderr=True)
    handler = RichHandler(
        console=console,
        rich_tracebacks=True,
        show_path=False,
        show_level=True,
        show_time=debug_mode,
        log_time_format=DATE_FORMAT,
        locals_max_length=None,
        locals_max_string=None,
        tracebacks_width=None,
        tracebacks_code_width=None,
        tracebacks_extra_lines=None
    )

    handler.setLevel(logging.DEBUG if debug_mode else logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False

    return logger
