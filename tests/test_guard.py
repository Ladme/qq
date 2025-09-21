# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import pytest

from qq_lib.env_vars import GUARD
from qq_lib.error import QQError
from qq_lib.guard import guard


def test_guard_env_set(monkeypatch):
    monkeypatch.setenv(GUARD, "1")
    guard()


def test_guard_env_not_set(monkeypatch):
    monkeypatch.delenv(GUARD, raising=False)
    with pytest.raises(QQError, match="must be run as a qq job"):
        guard()
