# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import socket
import subprocess
from abc import ABC
from pathlib import Path

from qq_lib.core.common import convert_absolute_to_relative
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.depend import Depend
from qq_lib.properties.resources import QQResources

from .job import BatchJobInterface
from .node import BatchNodeInterface
from .queue import BatchQueueInterface

logger = get_logger(__name__)


class QQBatchInterface[
    TBatchJob: BatchJobInterface,
    TBatchQueue: BatchQueueInterface,
    TBatchNode: BatchNodeInterface,
](ABC):
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
    def envName() -> str:
        """
        Return the name of the batch system environment.

        Returns:
            str: The batch system name.
        """
        raise NotImplementedError(
            "envName method is not implemented for this batch system implementation"
        )

    @staticmethod
    def isAvailable() -> bool:
        """
        Determine whether the batch system is available on the current host.

        Implementations typically verify this by checking for the presence
        of required commands or other environment-specific indicators.

        Returns:
            bool: True if the batch system is available, False otherwise.
        """
        raise NotImplementedError(
            "isAvailable method is not implemented for this batch system implementation"
        )

    @staticmethod
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
        raise NotImplementedError(
            "getScratchDir method is not implemented for this batch system implementation"
        )

    @staticmethod
    def jobSubmit(
        res: QQResources,
        queue: str,
        script: Path,
        job_name: str,
        depend: list[Depend],
        env_vars: dict[str, str],
    ) -> str:
        """
        Submit a job to the batch system.

        Can also perform additional validation of the job's resources.

        Args:
            res (QQResources): Resources required for the job.
            queue (str): Target queue for the job submission.
            script (Path): Path to the script to execute.
            job_name (str): Name of the job to use.
            depend (list[Depend]): List of job dependencies.
            env_vars (dict[str, str]): Dictionary of environment variables to propagate to the job.

        Returns:
            str: Unique ID of the submitted job.

        Raises:
            QQError: If the job submission fails.
        """
        raise NotImplementedError(
            "jobSubmit method is not implemented for this batch system implementation"
        )

    @staticmethod
    def jobKill(job_id: str) -> None:
        """
        Terminate a job gracefully. This assumes that job has time for cleanup.

        Args:
            job_id (str): Identifier of the job to terminate.

        Raises:
            QQError: If the job could not be killed.
        """
        raise NotImplementedError(
            "jobKill method is not implemented for this batch system implementation"
        )

    @staticmethod
    def jobKillForce(job_id: str) -> None:
        """
        Forcefully terminate a job. This assumes that the job has no time for cleanup.

        Args:
            job_id (str): Identifier of the job to forcefully terminate.

        Raises:
            QQError: If the job could not be killed.
        """
        raise NotImplementedError(
            "jobKillForce method is not implemented for this batch system implementation"
        )

    @staticmethod
    def getBatchJob(job_id: str) -> TBatchJob:
        """
        Retrieve information about a job from the batch system.

        The returned object should be fully initialized, even if the job
        no longer exists or its information is unavailable.

        Args:
            job_id (str): Identifier of the job.

        Returns:
            TBatchJob: Object containing the job's metadata and state.
        """
        raise NotImplementedError(
            "getBatchJob method is not implemented for this batch system implementation"
        )

    @staticmethod
    def getUnfinishedBatchJobs(user: str) -> list[TBatchJob]:
        """
        Retrieve information about all unfinished jobs submitted by `user`.

        Args:
            user (str): Username for which to fetch unfinished jobs.

        Returns:
            list[TBatchJob]: A list of job info objects representing the user's unfinished jobs.
        """
        raise NotImplementedError(
            "getUnfinishedBatchJobs method is not implemented for this batch system implementation"
        )

    @staticmethod
    def getBatchJobs(user: str) -> list[TBatchJob]:
        """
        Retrieve information about all jobs submitted by a specific user (including finished jobs).

        Args:
            user (str): Username for which to fetch all jobs.

        Returns:
            list[TBatchJob]: A list of job info objects representing all jobs of the user.
        """
        raise NotImplementedError(
            "getBatchJobs method is not implemented for this batch system implementation"
        )

    @staticmethod
    def getAllUnfinishedBatchJobs() -> list[TBatchJob]:
        """
        Retrieve information about unfinished jobs of all users.

        Returns:
            list[TBatchJob]: A list of job info objects representing unfinished jobs of all users.
        """
        raise NotImplementedError(
            "getAllUnfinishedBatchJobs method is not implemented for this batch system implementation"
        )

    @staticmethod
    def getAllBatchJobs() -> list[TBatchJob]:
        """
        Retrieve information about all jobs of all users.

        Returns:
            list[TBatchJob]: A list of job info objects representing all jobs of all users.
        """
        raise NotImplementedError(
            "getAllJobsInfo method is not implemented for this batch system implementation"
        )

    @staticmethod
    def getQueues() -> list[TBatchQueue]:
        """
        Retrieve all queues managed by the batch system.

        Returns:
            list[TBatchQueue]: A list of queue objects existing in the batch system.
        """
        raise NotImplementedError(
            "getQueues method is not implemented for this batch system implementation"
        )

    @staticmethod
    def getNodes() -> list[TBatchNode]:
        """ "
        Retrieve all nodes managed by the batch system.

        Returns:
            list[TBatchNode]: A list of node objects existing in the batch system.
        """
        raise NotImplementedError(
            "getNodes method is not implemented for this batch system implementations"
        )

    @staticmethod
    def navigateToDestination(host: str, directory: Path) -> None:
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
                f"-o ConnectTimeout={CFG.timeouts.ssh}",
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
    def writeRemoteFile(host: str, file: Path, content: str) -> None:
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
                f"-o ConnectTimeout={CFG.timeouts.ssh}",
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
    def makeRemoteDir(host: str, directory: Path) -> None:
        """
        Create a directory at the specified path on a remote host.

        The default implementation uses SSH to run `mkdir` on the remote host.
        This approach may be inefficient on shared storage or high-latency networks.
        Note that the timeout for the SSH connection is set to `SSH_TIMEOUT` seconds.

        Subclasses should override this method to provide a more efficient implementation
        if possible.

        Args:
            host (str): The hostname of the remote machine where the directory should be created.
            directory (Path): The path of the directory to create on the remote host.

        Raises:
            QQError: If the directory cannot be created but does not already exist or the SSH command fails.
        """
        result = subprocess.run(
            [
                "ssh",
                "-o PasswordAuthentication=no",
                f"-o ConnectTimeout={CFG.timeouts.ssh}",
                host,
                # ignore an error if the directory already exists
                f"mkdir {directory} 2>/dev/null || [ -d {directory} ]",
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise QQError(
                f"Could not make remote directory '{directory}' on '{host}': {result.stderr.strip()}."
            )

    @staticmethod
    def listRemoteDir(host: str, directory: Path) -> list[Path]:
        """
        List all files and directories (absolute paths) in the specified directory on a remote host.

        The default implementation uses SSH to run `ls -A` on the remote host.
        This approach may be inefficient on shared storage or high-latency networks.
        Note that the timeout for the SSH connection is set to `SSH_TIMEOUT` seconds.

        Subclasses should override this method to provide a more efficient implementation
        if possible.

        Args:
            host (str): The hostname of the remote machine where the directory resides.
            directory (Path): The remote directory to list.

        Returns:
            list[Path]: A list of `Path` objects representing the entries inside the directory.
                        Entries are relative to the given `directory`.

        Raises:
            QQError: If the directory cannot be listed or the SSH command fails.
        """
        result = subprocess.run(
            [
                "ssh",
                "-o PasswordAuthentication=no",
                f"-o ConnectTimeout={CFG.timeouts.ssh}",
                host,
                f"ls -A {directory}",
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise QQError(
                f"Could not list remote directory '{directory}' on '{host}': {result.stderr.strip()}."
            )

        # split by newline and filter out empty lines
        return [
            (Path(directory) / line).resolve()
            for line in result.stdout.splitlines()
            if line.strip()
        ]

    @staticmethod
    def moveRemoteFiles(host: str, files: list[Path], moved_files: list[Path]) -> None:
        """
        Move files on a remote host from their current paths to new paths.

        The default implementation uses SSH to run a sequence of `mv` commands on the remote host.
        This approach may be inefficient on shared storage or high-latency networks.
        Note that the timeout for the SSH connection is set to `SSH_TIMEOUT` seconds.

        Subclasses should override this method to provide a more efficient implementation
        if possible.

        Args:
            host (str): The hostname of the remote machine where the files reside.
            files (list[Path]): A list of source file paths on the remote host.
            moved_files (list[Path]): A list of destination file paths on the remote host.
                                    Must be the same length as `files`.

        Raises:
            QQError: If the SSH command fails, the files cannot be moved or
                    the length of `files` does not match the length of `moved_files`.
        """
        mv_command = QQBatchInterface._translateMoveCommand(files, moved_files)

        result = subprocess.run(
            [
                "ssh",
                "-o PasswordAuthentication=no",
                f"-o ConnectTimeout={CFG.timeouts.ssh}",
                host,
                mv_command,
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise QQError(
                f"Could not move files on a remote host '{host}': {result.stderr.strip()}."
            )

    @staticmethod
    def syncWithExclusions(
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        exclude_files: list[Path] | None = None,
    ) -> None:
        """
        Synchronize the contents of two directories using rsync, optionally across remote hosts,
        while excluding specified files or subdirectories.

        All files and directories in `src_dir` are copied to `dest_dir` except
        those listed in `exclude_files`. Files are never removed from the destination.

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
            QQError: If the rsync command fails for any reason or timeouts.
        """
        # convert absolute paths of files to exclude into relative to src_dir
        relative_excluded = (
            convert_absolute_to_relative(exclude_files, src_dir)
            if exclude_files
            else []
        )

        command = QQBatchInterface._translateRsyncExcludedCommand(
            src_dir, dest_dir, src_host, dest_host, relative_excluded
        )
        logger.debug(f"Rsync command: {command}.")

        QQBatchInterface._runRsync(src_dir, dest_dir, src_host, dest_host, command)

    @staticmethod
    def syncSelected(
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        include_files: list[Path] | None = None,
    ) -> None:
        """
        Synchronize only the explicitly selected files and directories from the source
        to the destination, optionally across remote hosts.

        Only files listed in `include_files` are copied from `src_dir` to `dest_dir`.
        Files not listed are ignored. Files are never removed from the destination.

        Args:
            src_dir (Path): Source directory to sync from.
            dest_dir (Path): Destination directory to sync to.
            src_host (str | None): Optional hostname of the source machine if remote;
                None if the source is local.
            dest_host (str | None): Optional hostname of the destination machine if remote;
                None if the destination is local.
            include_files (list[Path] | None): Optional list of absolute file paths to include in syncing.
                These paths are converted relative to `src_dir`.
                This argument is optional only for consistency with syncWithExclusions.

        Raises:
            QQError: If the rsync command fails or times out.
        """
        # convert absolute paths of files to include relative to src_dir
        relative_included = (
            convert_absolute_to_relative(include_files, src_dir)
            if include_files
            else []
        )

        command = QQBatchInterface._translateRsyncIncludedCommand(
            src_dir, dest_dir, src_host, dest_host, relative_included
        )
        logger.debug(f"Rsync command: {command}.")

        QQBatchInterface._runRsync(src_dir, dest_dir, src_host, dest_host, command)

    @staticmethod
    def transformResources(queue: str, provided_resources: QQResources) -> QQResources:
        """
        Transform user-provided QQResources into a batch system-specific QQResources instance.

        This method takes the resources provided during submission and returns a new
        QQResources object with any necessary modifications or defaults applied for
        the target batch system. The original `provided_resources` object is not modified.

        Args:
            queue (str): The name of the queue for which the resources are being adapted.
            provided_resources (QQResources): The raw resources specified by the user.

        Returns:
            QQResources: A new QQResources instance with batch system-specific adjustments,
                        fully constructed and validated.

        Raises:
            QQError: If any of the provided parameters are invalid or inconsistent.
        """
        raise NotImplementedError(
            "transformResources method is not implemented for this batch system implementation"
        )

    @staticmethod
    def isShared(directory: Path) -> bool:
        """
        Determine whether a given directory resides on a shared filesystem.

        Args:
            directory (Path): The directory to check.

        Returns:
            bool: True if the directory is on a shared filesystem, False if it is local.
        """
        # df -l exits with zero if the filesystem is local; otherwise it exits with a non-zero code
        result = subprocess.run(
            ["df", "-l", directory],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        return result.returncode != 0

    @staticmethod
    def resubmit(**kwargs) -> None:
        """
        Resubmit a job to the batch system.

        The default implementation connects via SSH to the specified machine,
        changes into the job directory, and re-executes the original job
        submission command (`qq submit ...`).

        If the resubmission fails, a QQError is raised.

        Keyword Args:
            input_machine (str): The hostname of the machine where the job
                should be resubmitted.
            input_dir (str | Path): The directory on the remote machine containing
                the job data and submission files.
            command_line (list[str]): The original command-line arguments that
                should be passed to `qq submit`.

        Raises:
            QQError: If the resubmission fails (non-zero return code from the
            SSH command).
        """
        input_machine = kwargs["input_machine"]
        input_dir = kwargs["input_dir"]
        command_line = kwargs["command_line"]

        qq_submit_command = f"{CFG.binary_name} submit {' '.join(command_line)}"

        logger.debug(
            f"Navigating to '{input_machine}:{input_dir}' to execute '{qq_submit_command}'."
        )
        result = subprocess.run(
            [
                "ssh",
                "-o PasswordAuthentication=no",
                f"-o ConnectTimeout={CFG.timeouts.ssh}",
                "-q",  # suppress some SSH messages
                input_machine,
                f"cd {input_dir} && {qq_submit_command}",
            ],
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            raise QQError(
                f"Could not resubmit the job on '{input_machine}': {result.stderr.strip()}."
            )

    @staticmethod
    def _translateSSHCommand(host: str, directory: Path) -> list[str]:
        """
        Construct the SSH command to navigate to a remote directory.

        This is an internal method of `QQBatchInterface`; you typically should not override it.

        Args:
            host (str): The hostname of the remote machine.
            directory (Path): The target directory to navigate to.

        Returns:
            list[str]: SSH command as a list suitable for subprocess execution.
        """
        return [
            "ssh",
            "-o PasswordAuthentication=no",  # never ask for password
            f"-o ConnectTimeout={CFG.timeouts.ssh}",
            host,
            "-t",
            f"cd {directory} || exit {QQBatchInterface.CD_FAIL} && exec bash -l",
        ]

    @staticmethod
    def _navigateSameHost(directory: Path) -> None:
        """
        Navigate to a directory on the current host using a subprocess.

        This is an internal method of `QQBatchInterface`; you typically should not override it.

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
    def _translateMoveCommand(files: list[Path], moved_files: list[Path]) -> str:
        """
        Translate lists of source and destination file paths into a single shell
        command string for moving the files.

        This is an internal method of `QQBatchInterface`; you typically should not override it.

        Args:
            files (list[Path]): A list of source file paths to be moved.
            moved_files (list[Path]): A list of destination file paths of the same
                length as `files`.

        Returns:
            str: A single shell command string consisting of `mv` commands joined
            with `&&`.

        Raises:
            QQError: If `files` and `moved_files` do not have the same length.
        """
        if len(files) != len(moved_files):
            raise QQError(
                "The provided 'files' and 'moved_files' must have the same length."
            )

        mv_commands: list[str] = []
        for src, dst in zip(files, moved_files):
            mv_commands.append(f"mv '{src}' '{dst}'")

        return " && ".join(mv_commands)

    @staticmethod
    def _translateRsyncExcludedCommand(
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        relative_excluded: list[Path],
    ) -> list[str]:
        """
        Build an rsync command to synchronize a directory while excluding specific files.

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
            relative_excluded (list[Path]): List of paths relative to `src_dir`
                to exclude from syncing.

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

    @staticmethod
    def _translateRsyncIncludedCommand(
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        relative_included: list[Path],
    ) -> list[str]:
        """
        Build an rsync command to synchronize only the explicitly included files.

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
            relative_included (list[Path]): List of paths relative to `src_dir`
                that should be included in the sync.

        Returns:
            list[str]: List of command arguments for rsync, suitable for `subprocess.run`.
        """

        command = ["rsync", "-a"]
        for file in relative_included:
            command.extend(["--include", str(file)])
        # exclude all files not specifically included
        command.extend(["--exclude", "*"])

        src = src_host + ":" + str(src_dir) + "/" if src_host else str(src_dir) + "/"
        dest = dest_host + ":" + str(dest_dir) if dest_host else str(dest_dir)
        command.extend([src, dest])

        return command

    @staticmethod
    def _runRsync(
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        command: list[str],
    ) -> None:
        """
        Execute an rsync command to synchronize files between source and destination.

        This is an internal method of `QQBatchInterface`; you typically should not override it.

        Args:
            src_dir (Path): Source directory path.
            dest_dir (Path): Destination directory path.
            src_host (str | None): Optional hostname of the source machine if remote;
                None if the source is local.
            dest_host (str | None): Optional hostname of the destination machine if remote;
                None if the destination is local.
            command (list[str]): List of command-line arguments for rsync, typically
                generated by `_translateRsyncExcludedCommand` or `_translateRsyncIncludedCommand`.

        Raises:
            QQError: If the rsync command fails (non-zero exit code) or
                if the command times out after `RSYNC_TIMEOUT` seconds.
        """
        src = f"{src_host}:{str(src_dir)}" if src_host else str(src_dir)
        dest = f"{dest_host}:{str(dest_dir)}" if dest_host else str(dest_dir)

        try:
            result = subprocess.run(
                command, capture_output=True, text=True, timeout=CFG.timeouts.rsync
            )
        except subprocess.TimeoutExpired as e:
            raise QQError(
                f"Could not rsync files between '{src}' and '{dest}': Connection timed out after {CFG.timeouts.rsync} seconds."
            ) from e

        if result.returncode != 0:
            raise QQError(
                f"Could not rsync files between '{src}' and '{dest}': {result.stderr.strip()}."
            )
