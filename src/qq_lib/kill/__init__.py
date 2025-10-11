# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

# ruff: noqa: F401

"""
Module for terminating qq jobs submitted from the current directory.

Read the documentation of the `kill` function for more details.
"""

from .cli import kill
from .killer import QQKiller
