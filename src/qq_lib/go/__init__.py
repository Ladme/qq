# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Navigation utilities for entering a qq job's working directory.

This module defines the `Goer` class, which extends `Navigator` to ensure a job
is in a suitable state for directory access and to open an interactive shell on
the job's main execution node. It handles queued jobs, missing destinations,
and state-based safety checks.
"""

from .goer import Goer

__all__ = [
    "Goer",
]
