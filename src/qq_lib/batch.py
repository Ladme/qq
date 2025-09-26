# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import socket
import subprocess
from abc import ABC, ABCMeta, abstractmethod
from pathlib import Path

from qq_lib.common import convert_absolute_to_relative
from qq_lib.constants import BATCH_SYSTEM, SSH_TIMEOUT
from qq_lib.error import QQError
from qq_lib.logger import get_logger
from qq_lib.resources import QQResources
from qq_lib.states import BatchState

logger = get_logger(__name__)


class BatchJobInfoInterface(ABC):
    """
    Abstract base class for retrieving and maintaining job information
    from a batch scheduling system.

    Must support situations where the job information no longer exists.

    The implementation of the constructor is arbitrary and should only
    be used inside the corresponding implementation of `QQBatchInterface.getJobInfo`.
    """

    @abstractmethod
    def update(self):
        """
        Refresh the stored job information from the batch system.

        Raises:
            QQError: If the job cannot be queried or its info updated.
        """
        pass

    @abstractmethod
    def getJobState(self) -> BatchState:
        """
        Return the current state of the job as reported by the batch system.

        If the job information is no longer available, return `BatchState.UNKNOWN`.

        Returns:
            BatchState: The job state according to the batch system.
        """
        pass


class QQBatchInterface[TBatchInfo: BatchJobInfoInterface](ABC):
    """
    Abstract base class for batch system integrations.

    Concrete batch system classes must implement these methods to allow
    qq to interact with different batch systems uniformly.

    All functions should raise QQError when encountering an error.
    """

    # magic number indicating unreachable directory when navigating to it
    CD_FAIL = 94
    # exit code of ssh if connection fails
    SSH_FAIL = 255

    @staticmethod
    @abstractmethod
    def envName() -> str:
        """
        Return the name of the batch system environment.

        Returns:
            str: The batch system name.
        """
        pass

    @staticmethod
    @abstractmethod
    def isAvailable() -> bool:
        """
        Determine whether the batch system is available on the current host.

        Implementations typically verify this by checking for the presence
        of required commands or other environment-specific indicators.

        Returns:
            bool: True if the batch system is available, False otherwise.
        """
        pass

    @staticmethod
    @abstractmethod
    def getScratchDir(job_id: str) -> Path:
        """
        Retrieve the scratch directory for a given job.

        Args:
            job_id (int): Unique identifier of the job.

        Returns:
            Path: Path to the scratch directory.

        Raises:
            QQError: If there is no scratch directory available for this job.
        """
        pass

    @staticmethod
    @abstractmethod
    def jobSubmit(res: QQResources, queue: str, script: Path) -> str:
        """
        Submit a job to the batch system.

        Args:
            res (QQResources): Resources required for the job.
            queue (str): Target queue for the job submission.
            script (Path): Path to the script to execute.

        Returns:
            str: Unique ID of the submitted job.

        Raises:
            QQError: If the job submission fails.
        """
        pass

    @staticmethod
    @abstractmethod
    def jobKill(job_id: str):
        """
        Terminate a job gracefully. This assumes that job has time for cleanup.

        Args:
            job_id (str): Identifier of the job to terminate.

        Raises:
            QQError: If the job could not be killed.
        """
        pass

    @staticmethod
    @abstractmethod
    def jobKillForce(job_id: str):
        """
        Forcefully terminate a job. This assumes that the job has no time for cleanup.

        Args:
            job_id (str): Identifier of the job to forcefully terminate.

        Raises:
            QQError: If the job could not be killed.
        """
        pass

    @staticmethod
    @abstractmethod
    def getJobInfo(job_id: str) -> TBatchInfo:
        """
        Retrieve comprehensive information about a job.

        The returned object should be fully initialized, even if the job
        no longer exists or its information is unavailable.

        Args:
            job_id (str): Identifier of the job.

        Returns:
            TBatchInfo: Object containing the job's metadata and state.
        """
        pass

    @staticmethod
    @abstractmethod
    def buildResources(**kwargs) -> QQResources:
        """
        Build a QQResources object for the target batch system using input parameters.

        By default, this method constructs basic resources directly from `kwargs`
        without performing any additional validation. The `kwargs` dictionary contains
        parameters provided to `qq submit`. Implementations must override this method
        to add validation or transform the input as needed.

        Raises:
            QQError: If any required parameters are missing or invalid.
        """
        # batch_system is not part of resources
        del kwargs["batch_system"]
        return QQResources(**kwargs)

    @staticmethod
    @abstractmethod
    def navigateToDestination(host: str, directory: Path):
        """
        Open a new terminal on the specified host and change the working directory
        to the given path, handing control over to the user.

        Default behavior:
            - If the target host is different from the current host, SSH is used
            to connect and `cd` is executed to switch to the directory.
            Note that the timeout for the SSH connection is set to `SSH_TIMEOUT` seconds.
            - If the target host matches the current host, only `cd` is used.

        A new terminal should always be opened, regardless of the host.

        Args:
            host (str): Hostname where the directory is located.
            directory (Path): Directory path to navigate to.

        Raises:
            QQError: If the navigation fails.
        """
        # if the directory is on the current host, we do not need to use ssh
        if host == socket.gethostname():
            QQBatchInterface._navigateSameHost(directory)
            return

        # the directory is on an another node
        ssh_command = QQBatchInterface._translateSSHCommand(host, directory)
        logger.debug(f"Using ssh: '{' '.join(ssh_command)}'")
        result = subprocess.run(ssh_command)

        # the subprocess exit code can come from:
        # - SSH itself failing - returns SSH_FAIL
        # - the explicit exit code we set if 'cd' to the directory fails - returns CD_FAIL
        # - the exit code of the last command the user runs in the interactive shell
        #
        # we ignore user exit codes entirely and only treat SSH_FAIL and CD_FAIL as errors
        if result.returncode == QQBatchInterface.SSH_FAIL:
            raise QQError(
                f"Could not reach '{host}:{str(directory)}': Could not connect to host."
            )
        if result.returncode == QQBatchInterface.CD_FAIL:
            raise QQError(
                f"Could not reach '{host}:{str(directory)}': Could not change directory."
            )

    @staticmethod
    @abstractmethod
    def readRemoteFile(host: str, file: Path) -> str:
        """
        Read the contents of a file on a remote host and return it as a string.

        The default implementation uses SSH to retrieve the file contents.
        This approach may be inefficient on shared storage or high-latency networks.
        Note that the timeout for the SSH connection is set to `SSH_TIMEOUT` seconds.

        Subclasses should override this method to provide a more efficient implementation
        if possible.

        Args:
            host (str): The hostname of the remote machine where the file resides.
            file (Path): The path to the file on the remote host.

        Returns:
            str: The contents of the remote file.

        Raises:
            QQError: If the file cannot be read or SSH fails.
        """
        result = subprocess.run(
            [
                "ssh",
                "-o PasswordAuthentication=no",
                f"-o ConnectTimeout={SSH_TIMEOUT}",
                "-q",  # suppress some SSH messages
                host,
                f"cat {file}",
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise QQError(
                f"Could not read remote file '{file}' on '{host}': {result.stderr.strip()}."
            )
        return result.stdout

    @staticmethod
    @abstractmethod
    def writeRemoteFile(host: str, file: Path, content: str):
        """
        Write the given content to a file on a remote host, overwriting it if it exists.

        The default implementation uses SSH to send the content to the remote file.
        This approach may be inefficient on shared storage or high-latency networks.
        Note that the timeout for the SSH connection is set to `SSH_TIMEOUT` seconds.

        Subclasses should override this method to provide a more efficient implementation
        if possible.

        Args:
            host (str): The hostname of the remote machine where the file resides.
            file (Path): The path to the file on the remote host.
            content (str): The content to write to the remote file.

        Raises:
            QQError: If the file cannot be written or SSH fails.
        """

        result = subprocess.run(
            [
                "ssh",
                "-o PasswordAuthentication=no",
                f"-o ConnectTimeout={SSH_TIMEOUT}",
                host,
                f"cat > {file}",
            ],
            input=content,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise QQError(
                f"Could not write to remote file '{file}' on '{host}': {result.stderr.strip()}."
            )

    @staticmethod
    @abstractmethod
    def submitGuard():
        """
        Perform an optional pre-submission check for the batch system.

        This method can be used to enforce custom rules or constraints before
        submitting a job. If the submission should be rejected, it must raise a QQError.

        Raises:
            QQError: If the submission violates any rules or should otherwise be rejected.

        Notes:
            The default implementation does nothing.
        """
        pass

    @staticmethod
    @abstractmethod
    def syncDirectories(
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        exclude_files: list[Path] | None = None,
    ):
        """
        Synchronize the contents of two directories using rsync, optionally across remote hosts.
        Files are never removed from the destination directory.

        Args:
            src_dir (Path): Source directory to sync from.
            dest_dir (Path): Destination directory to sync to.
            src_host (str | None): Optional hostname of the source machine if remote;
                None if the source is local.
            dest_host (str | None): Optional hostname of the destination machine if remote;
                None if the destination is local.
            exclude_files (list[Path] | None): Optional list of absolute file paths to exclude from syncing.
                These will be converted to paths relative to `src_dir`.

        Raises:
            QQError: If the rsync command fails for any reason.
        """
        # convert absolute paths of files to exclude into relative to src_dir
        relative_excluded = (
            convert_absolute_to_relative(exclude_files, src_dir)
            if exclude_files
            else []
        )

        command = QQBatchInterface._buildRsyncCommand(
            src_dir, dest_dir, src_host, dest_host, relative_excluded
        )
        logger.debug(f"Rsync command: {command}.")

        # run the rsync command
        result = subprocess.run(command, capture_output=True, text=True)

        if result.returncode != 0:
            src = f"{src_host}:{str(src_dir)}" if src_host else str(src_dir)
            dest = f"{dest_host}:{str(dest_dir)}" if dest_host else str(dest_dir)
            raise QQError(
                f"Could not rsync files between '{src}' and '{dest}': {result.stderr.strip()}."
            )

    @staticmethod
    def _translateSSHCommand(host: str, directory: Path) -> list[str]:
        """
        Construct the SSH command to navigate to a remote directory.
        Internal method of QQBatchInterface, you should probably not override it.

        Args:
            host (str): The hostname of the remote machine.
            directory (Path): The target directory to navigate to.

        Returns:
            list[str]: SSH command as a list suitable for subprocess execution.
        """
        return [
            "ssh",
            "-o PasswordAuthentication=no",  # never ask for password
            f"-o ConnectTimeout={SSH_TIMEOUT}",
            host,
            "-t",
            f"cd {directory} || exit {QQBatchInterface.CD_FAIL} && exec bash -l",
        ]

    @staticmethod
    def _navigateSameHost(directory: Path):
        """
        Navigate to a directory on the current host using a subprocess.
        Internal method of QQBatchInterface, you should probably not override it.

        Args:
            directory (Path): Directory to navigate to.
        """
        logger.debug("Current host is the same as target host. Using 'cd'.")
        if not directory.is_dir():
            raise QQError(
                f"Could not reach '{socket.gethostname()}:{str(directory)}': Could not change directory."
            )

        subprocess.run(["bash"], cwd=directory)

        # if the directory exists, always report success,
        # no matter what the user does inside the terminal

    @staticmethod
    def _buildRsyncCommand(
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        relative_excluded: list[Path],
    ) -> list[str]:
        """
        Build an rsync command for syncing files between local and/or remote directories.

        Both `src_host` and `dest_host` should not be set simultaneously,
        otherwise the resulting rsync command will be invalid.

        This is an internal method of `QQBatchInterface`; you typically should not override it.

        Args:
            src_dir (Path): Source directory path.
            dest_dir (Path): Destination directory path.
            src_host (str | None): Hostname of the source machine if remote;
                None if the source is local.
            dest_host (str | None): Hostname of the destination machine if remote;
                None if the destination is local.
            relative_excluded (list[Path] | None): List of paths relative to `src_dir`
                to exclude from syncing. Can be None.

            Returns:
                list[str]: List of command arguments for rsync, suitable for `subprocess.run`.
        """

        # not using --checksum nor --ignore-times for performance reasons
        # some files may potentially not be correctly synced if they were
        # modified in both src_dir and dest_dir at the same time and have
        # the same size -> this should be so extremely rare that we do not care
        command = ["rsync", "-a"]
        for file in relative_excluded:
            command.extend(["--exclude", str(file)])

        src = src_host + ":" + str(src_dir) + "/" if src_host else str(src_dir) + "/"
        dest = dest_host + ":" + str(dest_dir) if dest_host else str(dest_dir)
        command.extend([src, dest])

        return command


class QQBatchMeta(ABCMeta):
    """
    Metaclass for batch system classes.
    """

    # registry of supported batch systems
    _registry: dict[str, type[QQBatchInterface]] = {}

    def __str__(cls: type[QQBatchInterface]):
        """
        Get the string representation of the batch system class.
        """
        return cls.envName()

    @classmethod
    def register(cls, batch_cls: type[QQBatchInterface]):
        """
        Register a batch system class in the metaclass registry.

        Args:
            batch_cls: Subclass of QQBatchInterface to register.
        """
        cls._registry[batch_cls.envName()] = batch_cls

    @classmethod
    def fromStr(mcs, name: str) -> type[QQBatchInterface]:
        """
        Return the batch system class registered with the given name.

        Raises:
            QQError: If no class is registered for the given name.
        """
        if name not in mcs._registry:
            raise QQError(f"No batch system registered as '{name}'.")
        return mcs._registry[name]

    @classmethod
    def guess(mcs) -> type[QQBatchInterface]:
        """
        Attempt to select an appropriate batch system implementation.

        The method scans through all registered batch systems in the order
        they were registered and returns the first one that reports itself
        as available.

        Raises:
            QQError: If no available batch system is found among the registered ones.

        Returns:
            type[QQBatchInterface]: The first available batch system class.
        """
        for BatchSystem in mcs._registry.values():
            if BatchSystem.isAvailable():
                logger.debug(f"Guessed batch system: {str(BatchSystem)}.")
                return BatchSystem

        # raise error if there is no available batch system
        raise QQError(
            "Could not guess a batch system. No registered batch system available."
        )

    @classmethod
    def fromEnvVarOrGuess(mcs) -> type[QQBatchInterface]:
        """
        Select a batch system based on the environment variable or by guessing.

        This method first checks the `BATCH_SYSTEM` environment variable. If it is set,
        the method returns the registered batch system class corresponding to its value.
        If the variable is not set, it falls back to `guess` to select an available
        batch system from the registered classes.

        Returns:
            type[QQBatchInterface]: The selected batch system class.

        Raises:
            QQError: If the environment variable is set to an unknown batch system name,
                    or if no available batch system can be guessed.
        """
        name = os.environ.get(BATCH_SYSTEM)
        if name:
            logger.debug(
                f"Using batch system name from an environment variable: {name}."
            )
            return QQBatchMeta.fromStr(name)

        return QQBatchMeta.guess()

    @classmethod
    def obtain(mcs, name: str | None) -> type[QQBatchInterface]:
        """
        Obtain a batch system class by name, environment variable, or guessing.

        Args:
            name (str | None): Optional name of the batch system to obtain.
                - If provided, returns the class registered under this name.
                - If `None`, falls back to `fromEnvVarOrGuess` to determine
                the batch system from the environment variable or by guessing.

        Returns:
            type[QQBatchInterface]: The selected batch system class.

        Raises:
            QQError: If `name` is provided but no batch system with that name is registered,
                    or if `name` is `None` and `fromEnvVarOrGuess` fails.
        """
        if name:
            return QQBatchMeta.fromStr(name)

        return QQBatchMeta.fromEnvVarOrGuess()
