# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

# ruff: noqa: F401

from qq_lib.batch.interface.meta import QQBatchMeta

from .job import VBSJobInfo
from .qqvbs import QQVBS
from .system import VBSError, VirtualBatchSystem, VirtualJob

# register QQVBS
QQBatchMeta.register(QQVBS)
