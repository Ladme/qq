# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from pathlib import Path

from rich.console import Console

from qq_lib.info.informer import QQInformer
from qq_lib.info.presenter import QQPresenter


class QQOperator:
    """
    Base class for performing operations with qq jobs.

    Attributes:
        _informer (QQInformer): The underlying informer object that provides job details.
        _info_file (Path): The path to the qq info file associated with this job.
        _input_machine (str | None): Hostname of the machine on which the qq info file is stored.
        _batch_system (str): The batch system type as reported by the informer.
        _state (RealState): The current real state of the qq job.
    """

    def __init__(self, info_file: Path, host: str | None = None):
        """
        Initialize a QQOperator instance from a qq info file.

        Args:
            info_file (Path): Path to the qq info file describing the job.
            host (str | None, optional): Optional hostname of a machine from
                which to load job information. Defaults to None meaning 'current machine'.
        """
        self._informer = QQInformer.fromFile(info_file, host)
        self._info_file = info_file
        self._input_machine = host
        self._batch_system = self._informer.batch_system
        self._state = self._informer.getRealState()

    def update(self) -> None:
        """
        Refresh the internal informer and job state from the qq info file.
        """
        self._informer = QQInformer.fromFile(self._info_file, self._input_machine)
        self._state = self._informer.getRealState()

    def getInformer(self) -> QQInformer:
        """
        Retrieve the underlying QQInformer instance.

        Returns:
            QQInformer: The informer currently associated with this operator.
        """
        return self._informer

    def printInfo(self, console: Console) -> None:
        """
        Display the current job information in a formatted Rich panel.

        Args:
            console (Console): Rich Console instance used to render output.
        """
        presenter = QQPresenter(self._informer)
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
