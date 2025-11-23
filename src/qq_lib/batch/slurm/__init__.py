# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Slurm backend for qq: job submission, monitoring, and cluster-resource access.

This module implements qq's full integration with the Slurm batch system.

It provides:

- The `Slurm` batch-system backend, implementing job submission, killing,
  remote file access and synchronization, resource translation, dependency formatting,
  and all Slurm-specific environment propagation.

- `SlurmJob`, `SlurmNode`, and `SlurmQueue`, concrete implementations of qq's
  job/node/queue interfaces, responsible for parsing Slurm command output and exposing
  normalized metadata to the rest of qq.
"""

from .job import SlurmJob
from .node import SlurmNode
from .queue import SlurmQueue
from .slurm import Slurm

__all__ = [
    "SlurmJob",
    "SlurmNode",
    "SlurmQueue",
    "Slurm",
]
