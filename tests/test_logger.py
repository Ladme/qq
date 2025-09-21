# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import logging

from qq_lib.logger import DEBUG_MODE, get_logger


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
