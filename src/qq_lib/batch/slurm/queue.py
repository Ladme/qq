# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from qq_lib.batch.interface.queue import BatchQueueInterface


class SlurmQueue(BatchQueueInterface):
    """
    Implementation of BatchQueueInterface for Slurm.
    Stores metadata for a single Slurm queue.
    """
