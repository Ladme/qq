# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

# ruff: noqa: F401

"""
This module provides a tool for safely clearing qq run files from a job directory.

The process goes as follows:
    - the QQClearer scans the directory for files with qq-specific suffixes.
    - it checks whether it is safe to remove files
      (all jobs must be in a failed/killed/inconsistent state or `--force` must be used).
    - files are removed if clearing is allowed.
"""

from .clearer import QQClearer
from .cli import clear
