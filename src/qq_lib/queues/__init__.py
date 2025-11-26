# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Presentation utilities for queues of the batch system.

This module defines `QueuesPresenter`, a formatter that turns raw queue data from
the batch system into user-friendly Rich panels. It summarizes per-queue load,
availability, routing relationships, limits such as walltime and node caps, and
optional administrative comments.
"""

from .presenter import QueuesPresenter

__all__ = [
    "QueuesPresenter",
]
