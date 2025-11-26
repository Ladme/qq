# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
File-synchronization utilities for retrieving data from a running or failed qq job.

This module defines the `Syncer` class, an extension of `Navigator` that handles
copying files from a job's remote working directory back to the job's input
directory. It performs safety checks based on the job's real state, ensuring
that synchronization is attempted only when a working directory actually exists.
"""

from .syncer import Syncer

__all__ = [
    "Syncer",
]
