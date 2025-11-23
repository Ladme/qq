# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
PBS backend for qq: job submission, monitoring, and cluster-resource access.

This module implements qq's full integration with the PBS Pro batch system
as configured on the Metacentrum-family clusters.

It provides:

- The `PBS` batch-system backend, implementing job submission, killing, file
  synchronization (local and remote), work-directory handling, resource
  translation, dependency formatting, and scratch-directory logic.

- `PBSJob`, `PBSNode`, and `PBSQueue`, concrete implementations of qq's
  job/node/queue interfaces, responsible for parsing PBS command output and
  exposing normalized metadata to the rest of qq.
"""

from .job import PBSJob
from .node import PBSNode
from .pbs import PBS
from .queue import PBSQueue

__all__ = [
    "PBSJob",
    "PBSNode",
    "PBS",
    "PBSQueue",
]
