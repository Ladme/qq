# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
SlurmIT4I backend for qq: job submission, monitoring, and IT4I-specific
scratch and resource handling.

This module provides qq's full integration with the Slurm batch system as
configured on IT4Innovations clusters (e.g., Karolina, Barbora). It extends the
generic Slurm backend with all IT4I-specific behavior:

- `SlurmIT4I`, the batch-system backend implementing job submission, killing,
  resource translation, local/remote file access, scratch-directory creation,
  and work-directory selection logic.
"""

from .slurm import SlurmIT4I

__all__ = [
    "SlurmIT4I",
]
