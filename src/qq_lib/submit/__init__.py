# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

# ruff: noqa: F401

"""
This module manages submission of qq jobs using the QQSubmitter class.
"""

from .cli import submit
from .factory import SubmitterFactory
from .parser import Parser
from .submitter import QQSubmitter
