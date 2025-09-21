# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from qq_lib.pbs import QQPBS

# list of supported batch systems
BATCH_SYSTEMS = {QQPBS.envName(): QQPBS}
