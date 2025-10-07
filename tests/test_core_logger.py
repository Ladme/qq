# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import io
import logging
from datetime import datetime

from rich.console import Console

from qq_lib.core.constants import DATE_FORMAT
from qq_lib.core.logger import DEBUG_MODE, get_logger


def test_logger_debug_mode(monkeypatch):
    # enable debug mode
    monkeypatch.setenv(DEBUG_MODE, "1")
    logger = get_logger("test_debug")

    assert logger.level == logging.DEBUG


def test_logger_non_debug_mode(monkeypatch):
    # disable debug mode
    monkeypatch.delenv(DEBUG_MODE, raising=False)
    logger = get_logger("test_info")

    assert logger.level == logging.INFO


def _make_stringio_logger(monkeypatch, *, show_time=False):
    """Return a logger writing into a StringIO buffer."""
    buf = io.StringIO()

    monkeypatch.setitem(
        get_logger.__globals__,
        "Console",
        lambda **kwargs: Console(file=buf, force_terminal=False, **kwargs),
    )

    name = f"test_logger_{show_time}"
    logging.getLogger(name).handlers.clear()
    logger = get_logger(name, show_time=show_time)
    return logger, buf


def test_logger_outputs_time_in_debug_mode(monkeypatch):
    # enable debug mode
    monkeypatch.setenv(DEBUG_MODE, "1")
    logger, buf = _make_stringio_logger(monkeypatch)
    logger.info("hello")
    output = buf.getvalue()

    timestamp = datetime.now().strftime(DATE_FORMAT)[:-3]  # ignore seconds
    assert timestamp in output


def test_logger_outputs_time_show_time_true(monkeypatch):
    # disable debug mode
    monkeypatch.delenv(DEBUG_MODE, raising=False)
    logger, buf = _make_stringio_logger(monkeypatch, show_time=True)
    logger.info("hello")
    output = buf.getvalue()

    timestamp = datetime.now().strftime(DATE_FORMAT)[:-3]  # ignore seconds
    assert timestamp in output


def test_logger_does_not_outputs_time_default(monkeypatch):
    # disable debug mode
    monkeypatch.delenv(DEBUG_MODE, raising=False)
    logger, buf = _make_stringio_logger(monkeypatch)
    logger.info("hello")
    output = buf.getvalue()

    timestamp = datetime.now().strftime(DATE_FORMAT)[:-3]  # ignore seconds
    assert timestamp not in output
