# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Provides node presentation utilities.

This module organizes and formats information about compute nodes as reported by
the batch system, preparing it for human-readable terminal output.

Internal grouping logic clusters nodes with similar naming patterns, extracts
shared attributes, and aggregates resource and property data. These groups are
then rendered by `NodesPresenter`, which produces a unified panel showing node
availability, CPU/GPU capacities, scratch resources, and other relevant metrics.
"""

from .presenter import NodesPresenter

__all__ = ["NodesPresenter"]
