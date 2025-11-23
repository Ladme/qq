# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Abstractions for integrating qq with HPC batch scheduling systems.

This module defines the core interfaces that allow qq to interact with
multiple batch systems through a unified API. It provides:

- `BatchInterface`: the central abstract interface that every batch-system
  backend implements. It defines operations such as job submission, job
  querying, directory synchronization, remote file access, resubmission, and
  navigation to compute nodes.

- `BatchJobInterface`, `BatchNodeInterface`, and `BatchQueueInterface`:
  lightweight abstractions representing jobs, nodes, and queues as reported
  by the underlying scheduler. These interfaces expose normalized metadata
  and allow qq to present consistent job/queue/node information regardless
  of scheduler differences.

- `BatchMeta`: a metaclass that registers available batch-system backends
  and provides mechanisms for selecting one from environment variables or by
  probing system availability. The `@batch_system` decorator registers
  implementations automatically.
"""

from .interface import BatchInterface
from .job import BatchJobInterface
from .meta import BatchMeta
from .node import BatchNodeInterface
from .queue import BatchQueueInterface

__all__ = [
    "BatchInterface",
    "BatchJobInterface",
    "BatchMeta",
    "BatchNodeInterface",
    "BatchQueueInterface",
]
