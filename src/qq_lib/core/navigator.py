# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import socket
from pathlib import Path

from qq_lib.properties.states import RealState

from .operator import QQOperator


class QQNavigator(QQOperator):
    """
    Base class for performing operations with job's working directory.

    Attributes:
        _informer (QQInformer): The underlying informer object that provides job details.
        _info_file (Path): The path to the qq info file associated with this job.
        _input_machine (str | None): Hostname of the machine on which the qq info file is stored.
        _batch_system (str): The batch system type as reported by the informer.
        _state (RealState): The current real state of the qq job.
        _work_dir (Path | None): Path to the job's working directory. None if it does not exist.
        _main_node (str | None): Main node on which the job is running. None if main node is not known.
    """

    def __init__(self, info_file: Path, host: str | None = None):
        super().__init__(info_file, host)
        self._setDestination()

    def update(self):
        super().__init__()
        self._setDestination()

    def hasDestination(self) -> bool:
        """
        Check that the job has an assigned host and working directory.

        Returns:
            bool: True if the job has both a host and a working directory,
            False otherwise.
        """
        return self._work_dir is not None and self._main_node is not None

    def _setDestination(self) -> None:
        """
        Get the job's host and working directory from the wrapped informer.

        Updates:
            - _main_node: hostname of the main node where the job runs
            - _work_dir: absolute path to the working directory

        Raises:
            QQError: If main_node or work_dir are not defined in the informer.
        """
        destination = self._informer.getDestination()

        if destination:
            (self._main_node, self._work_dir) = destination
        else:
            self._main_node = None
            self._work_dir = None

    def _isInWorkDir(self) -> bool:
        """
        Check if the current process is already in the job's working directory.

        Returns:
            bool: True if the current directory matches the job's work_dir and:
              a) either an input_dir was used to run the job, or
              b) local hostname matches the job's main node
        """
        # note that we cannot just compare directory paths, since
        # the same directory path may point to different directories
        # on the current machine and on the execution node
        # we also need to check that
        #   a) job was running in shared storage or
        #   b) we are on the same machine
        return (
            self._work_dir is not None
            and self._work_dir.resolve() == Path.cwd().resolve()
            and (
                not self._informer.usesScratch()
                or self._main_node == socket.gethostname()
            )
        )

    def _isQueued(self) -> bool:
        """Check if the job is queued, booting, held, or waiting."""
        return self._state in {
            RealState.QUEUED,
            RealState.BOOTING,
            RealState.HELD,
            RealState.WAITING,
        }

    def _isKilled(self) -> bool:
        """Check if the job has been or is being killed."""
        return self._state == RealState.KILLED or (
            self._state == RealState.EXITING
            and self._informer.info.job_exit_code is None
        )

    def _isFinished(self) -> bool:
        """Check if the job has finished succesfully."""
        return self._state == RealState.FINISHED

    def _isFailed(self) -> bool:
        """Check if the job has failed."""
        return self._state == RealState.FAILED

    def _isUnknownInconsistent(self) -> bool:
        """Check if the job is in an unknown or inconsistent state."""
        return self._state in {RealState.UNKNOWN, RealState.IN_AN_INCONSISTENT_STATE}

    def _isExitingSuccessfully(self) -> bool:
        """
        Check whether the job is currently successfully exiting.
        """
        return (
            self._state == RealState.EXITING and self._informer.info.job_exit_code == 0
        )
