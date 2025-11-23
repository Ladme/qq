# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Utilities for submitting qq jobs.

This module integrates three main components - `Parser`, `Submitter`, and
`SubmitterFactory` - that collectively interpret submission settings, construct
job metadata, and hand off execution to the batch system.

`Parser` extracts qq directives declared inside the script (via `# qq ...`
lines) and normalizes them into structured submission parameters such as
resources, dependencies, file include/exclude rules and loop-job fields.

`Submitter` validates the script, prevents accidental duplicate submissions,
constructs the qq info file, sets up environment variables needed by `qq run`,
and finally invokes the batch system's submission mechanism.

`SubmitterFactory` coordinates command-line arguments with script-embedded
directives, merges and resolves resources, determines the batch system and
queue, constructs loop-job settings, and ultimately produces a fully configured
`Submitter`. It ensures a consistent and unified interpretation of submission
parameters from all available sources.
"""

from .factory import SubmitterFactory
from .parser import Parser
from .submitter import Submitter

__all__ = ["SubmitterFactory", "Parser", "Submitter"]
