# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import logging
import os

from rich.console import Console
from rich.logging import RichHandler

from .config import CFG


def get_logger(name: str, show_time: bool = False) -> logging.Logger:
    """
    Return a logger with unified formatting.
    If colored=True, use rich's RichHandler with colored levels.
    """
    logger = logging.getLogger(name)

    debug_mode = os.environ.get(CFG.env_vars.debug_mode) is not None
    logger.setLevel(logging.DEBUG if debug_mode else logging.INFO)

    console = Console(stderr=True)
    handler = RichHandler(
        console=console,
        rich_tracebacks=True,
        show_path=False,
        show_level=True,
        show_time=show_time or debug_mode,
        log_time_format=CFG.date_formats.standard,
        tracebacks_width=None,
        tracebacks_code_width=None,
    )

    handler.setLevel(logging.DEBUG if debug_mode else logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False

    return logger
