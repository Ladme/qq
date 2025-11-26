# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Base functionality for qq job operations.

This module defines the `Operator` class, which provides a common interface for
working with qq jobs. It loads job information, tracks job state, refreshes
metadata, and renders formatted status output using Rich presenters.
"""

from pathlib import Path
from typing import Self

from rich.console import Console

from qq_lib.info.informer import Informer
from qq_lib.info.presenter import Presenter


class Operator:
    """
    Base class for performing operations with qq jobs.

    Attributes:
        _informer (Informer): The underlying informer object that provides job details.
        _info_file (Path): The path to the qq info file associated with this job.
        _input_machine (str | None): Hostname of the machine on which the qq info file is stored.
        _batch_system (str): The batch system type as reported by the informer.
        _state (RealState): The current real state of the qq job.
    """

    def __init__(self, info_file: Path, host: str | None = None):
        """
        Initialize an Operator instance from a qq info file.

        Args:
            info_file (Path): Path to the qq info file describing the job.
            host (str | None, optional): Optional hostname of a machine from
                which to load job information. Defaults to None meaning 'current machine'.
        """
        self._informer = Informer.fromFile(info_file, host)
        self._info_file = info_file
        self._input_machine = host
        self._batch_system = self._informer.batch_system
        self._state = self._informer.getRealState()

    @classmethod
    def fromInformer(cls, informer: Informer) -> Self:
        """
        Initialize an Operator instance from an Informer.

        Path to info file is set based on the information in the Informer, even if it does not exist.

        Args:
            informer (Informer): Initialized informer instance containing information about the job.

        Returns:
            Operator: Initialized Operator.
        """
        operator = cls.__new__(cls)
        operator._informer = informer
        operator._info_file = informer.getInfoFile()
        operator._input_machine = informer.info.input_machine
        operator._batch_system = informer.batch_system
        operator._state = informer.getRealState()

        return operator

    def update(self) -> None:
        """
        Refresh the internal informer and job state from the qq info file.
        """
        self._informer = Informer.fromFile(self._info_file, self._input_machine)
        self._state = self._informer.getRealState()

    def getInformer(self) -> Informer:
        """
        Retrieve the underlying Informer instance.

        Returns:
            Informer: The informer currently associated with this operator.
        """
        return self._informer

    def printInfo(self, console: Console) -> None:
        """
        Display the current job information in a formatted Rich panel.

        Args:
            console (Console): Rich Console instance used to render output.
        """
        presenter = Presenter(self._informer)
        panel = presenter.createJobStatusPanel(console)
        console.print(panel)

    def matchesJob(self, job_id: str) -> bool:
        """
        Determine whether this operator corresponds to the specified job ID.

        Args:
            job_id (str): The job ID to compare against (e.g., "12345" or "12345.cluster.domain").

        Returns:
            bool: True if both job IDs refer to the same job (same numeric/job part),
                False otherwise.
        """
        return self._informer.matchesJob(job_id)
