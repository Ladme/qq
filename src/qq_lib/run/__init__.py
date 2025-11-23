# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Execution utilities for running qq jobs inside the batch environment.

This module defines the `Runner` class, which prepares the execution
environment, launches the user's job script, updates qq's state tracking,
and performs cleanup on success, failure, or interruption. It handles both
shared and scratch working directories, loop-job archiving, resubmiting,
communication with the batch system, and SIGTERM-safe shutdown.
"""

from .runner import Runner

__all__ = [
    "Runner",
]
