# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Utilities for locating and retrieving the input directory of a job.

This module provides the `Cder` class, which queries the configured
batch system for a job's input directory. The printed path is intended
to be consumed by a shell wrapper function that performs the actual directory change.
"""

from .cder import Cder

__all__ = [
    "Cder",
]
