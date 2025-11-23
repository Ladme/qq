# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Utilities for removing the working directory of a qq job.

This module defines the `Wiper` class, an extension of `Navigator` responsible for
safely deleting a job's remote working directory once it is no longer needed.

`Wiper` distinguishes between shared-storage jobs and scratch-based jobs, and
guards against accidental deletion of the job's input directory.
"""

from .wiper import Wiper

__all__ = [
    "Wiper",
]
