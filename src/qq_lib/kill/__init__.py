# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Termination utilities for qq jobs.

This module defines the `Killer` class, which extends `Operator` to validate
whether a job can be terminated and to invoke the batch system's kill command.

It also updates and locks the qq info file when appropriate, ensuring that killed jobs
are consistently recorded.
"""

from .killer import Killer

__all__ = [
    "Killer",
]
