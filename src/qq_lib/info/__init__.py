# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Facilities for loading, interpreting, and presenting qq job information.

This module defines the `Informer` class, which loads qq job metadata,
combines information from info files and from the batch system, and provides
access to runtime details such as working nodes, submission/start/end times, and resources.

It also provides the `Presenter` class, which formats this information into
Rich-based status panels, full job-information views, and compact summaries used
throughout qq's CLI.
"""

from .informer import Informer
from .presenter import Presenter

__all__ = ["Informer", "Presenter"]
