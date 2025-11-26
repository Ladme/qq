# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
SlurmLumi backend for qq: job submission, monitoring, and LUMI-specific
scratch/flash storage handling.

This module integrates qq with the Slurm environment deployed on the LUMI
supercomputer. It extends the IT4I Slurm backend with all LUMI-specific
behavior, most importantly the dual-tier temporary storage model and
queue-resource conventions.

- `SlurmLumi`, the batch-system backend implementing job submission,
  dependency handling, resource translation, scratch/flash directory creation,
  and file/directory operations on LUMI's fully shared storage.
"""

from .slurm import SlurmLumi

__all__ = [
    "SlurmLumi",
]
