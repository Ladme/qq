# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Batch-system support for qq.

This module groups all components that allow qq to interact with HPC batch
schedulers. It defines the abstract interfaces for jobs, queues, and nodes,
together with the concrete backends for PBS, Slurm, and site-specific Slurm
variants.
"""

# import so that these batch systems are available but do not export them from here
from .pbs import PBS as _PBS
from .slurm import Slurm as _Slurm
from .slurmit4i import SlurmIT4I as _SlurmIT4I
from .slurmlumi import SlurmLumi as _SlurmLumi

_PBS, _Slurm, _SlurmIT4I, _SlurmLumi
