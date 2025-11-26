# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Utilities for detecting and removing qq runtime files.

This module provides the `Clearer` class, which identifies and deletes
qq-generated runtime files from a directory. Files associated with active
or successfully completed jobs are preserved unless forced removal is requested.
"""

from .clearer import Clearer

__all__ = [
    "Clearer",
]
