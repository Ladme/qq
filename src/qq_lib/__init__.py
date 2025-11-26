# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Core implementation of the qq command-line tool.

This package provides the internal logic behind qq's job-submission and
job-management workflow. It defines the abstractions for batch systems, concrete
backends (PBS, Slurm, and site-specific variants), utilities for preparing and
synchronizing working directories, loop-job handling, and helpers for inspecting
jobs, queues, and nodes. All qq CLI commands ultimately delegate to the
functionality implemented here.
"""

from .qq import __version__, cli

__all__ = [
    "__version__",
    "cli",
    "archive",
    "batch",
    "cd",
    "clear",
    "core",
    "go",
    "info",
    "jobs",
    "kill",
    "nodes",
    "properties",
    "queues",
    "run",
    "submit",
    "sync",
    "wipe",
]
