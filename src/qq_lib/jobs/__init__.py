# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Presentation utilities for batch-system job listings and statistics.

This module provides `JobsPresenter`, which formats batch-system job data
into compact CLI tables and Rich panels.

Unlike many other qq modules, this module operates purely
on information obtained directly from the batch system
and does not use qq info files.
"""

from .presenter import JobsPresenter

__all__ = ["JobsPresenter"]
