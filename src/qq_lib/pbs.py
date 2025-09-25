# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import socket
import subprocess
from pathlib import Path

from qq_lib.batch import (
    BatchJobInfoInterface,
    QQBatchInterface,
    QQBatchMeta,
)
from qq_lib.common import equals_normalized
from qq_lib.constants import QQ_OUT_SUFFIX
from qq_lib.error import QQError
from qq_lib.logger import get_logger
from qq_lib.resources import QQResources
from qq_lib.states import BatchState

logger = get_logger(__name__)


# forward declaration
class PBSJobInfo(BatchJobInfoInterface):
    pass


class QQPBS(QQBatchInterface[PBSJobInfo], metaclass=QQBatchMeta):
    """
    Implementation of QQBatchInterface for PBS Pro batch system.
    """

    # magic number indicating unreachable directory when navigating to it
    CD_FAIL = 94
    # exit code of ssh if connection fails
    SSH_FAIL = 255
    # default work-dir type to use with PBS
    DEFAULT_WORK_DIR = "scratch_local"

    def envName() -> str:
        return "PBS"

    def getScratchDir(job_id: str) -> Path:
        scratch_dir = os.environ.get("SCRATCHDIR")
        if not scratch_dir:
            raise QQError(f"Scratch directory for job '{job_id}' is undefined")

        return Path(scratch_dir)

    def jobSubmit(res: QQResources, queue: str, script: Path) -> str:
        # get the submission command
        command = QQPBS._translateSubmit(res, queue, str(script))
        logger.debug(command)

        # submit the script
        result = subprocess.run(
            ["bash"], input=command, text=True, check=False, capture_output=True
        )

        if result.returncode != 0:
            raise QQError(
                f"Failed to submit script '{str(script)}': {result.stderr.strip()}."
            )

        return result.stdout.strip()

    def jobKill(job_id: str):
        command = QQPBS._translateKill(job_id)
        logger.debug(command)

        # run the kill command
        result = subprocess.run(
            ["bash"], input=command, text=True, check=False, capture_output=True
        )

        if result.returncode != 0:
            raise QQError(f"Failed to kill job '{job_id}': {result.stderr.strip()}.")

    def jobKillForce(job_id: str):
        command = QQPBS._translateKillForce(job_id)
        logger.debug(command)

        # run the kill command
        result = subprocess.run(
            ["bash"], input=command, text=True, check=False, capture_output=True
        )

        if result.returncode != 0:
            raise QQError(f"Failed to kill job '{job_id}': {result.stderr.strip()}.")

    def navigateToDestination(host: str, directory: Path):
        # if the directory is on the current host, we do not need to use ssh
        if host == socket.gethostname():
            QQPBS._navigateSameHost(directory)

        # the directory is on an another node
        ssh_command = QQPBS._translateSSHCommand(host, directory)
        logger.debug(f"Using ssh: '{' '.join(ssh_command)}'")
        result = subprocess.run(ssh_command)

        # the subprocess exit code can come from:
        # - SSH itself failing - returns SSH_FAIL
        # - the explicit exit code we set if 'cd' to the directory fails - returns CD_FAIL
        # - the exit code of the last command the user runs in the interactive shell
        #
        # we ignore user exit codes entirely and only treat SSH_FAIL and CD_FAIL as errors
        if result.returncode == QQPBS.SSH_FAIL:
            raise QQError(
                f"Could not reach '{host}:{str(directory)}': Could not connect to host."
            )
        if result.returncode == QQPBS.CD_FAIL:
            raise QQError(
                f"Could not reach '{host}:{str(directory)}': Could not change directory."
            )

    def buildResources(**kwargs) -> QQResources:
        try:
            res = QQBatchInterface.buildResources(**kwargs)
            QQPBS._handleWorkDirRes(res)
        except Exception as e:
            raise QQError(f"Specification of resources is invalid: {e}.") from e

        return res

    def getJobInfo(job_id: str) -> PBSJobInfo:
        return PBSJobInfo(job_id)  # ty: ignore[invalid-return-type]

    def _translateSubmit(res: QQResources, queue: str, script: str) -> str:
        """
        Generate the PBS submission command for a job.

        Args:
            res (QQResources): The resources requested for the job.
            queue (str): The queue name to submit to.
            script (str): Path to the job script.

        Returns:
            str: The fully constructed qsub command string.
        """
        qq_output = str(Path(script).with_suffix(QQ_OUT_SUFFIX))
        command = f"qsub -q {queue} -j eo -e {qq_output} -V "

        # handle resources
        trans_res = QQPBS._translateResources(res)

        if len(trans_res) > 0:
            command += "-l "

        command += ",".join(trans_res) + " " + script

        return command

    @staticmethod
    def _translateResources(res: QQResources) -> list[str]:
        """
        Convert QQResources into PBS-compatible resource strings.

        Args:
            res (QQResources): The resources requested for the job.

        Returns:
            list[str]: List of resource specifications for inclusion in the qsub command.
        """
        trans_res = []
        for name, value in res.toDict().items():
            if name in ["work_dir", "work_size"]:
                continue

            trans_res.append(f"{name}={value}")

        # translate working directory resource
        workdir = QQPBS._translateWorkDir(res)
        if workdir:
            trans_res.append(workdir)

        return trans_res

    @staticmethod
    def _translateWorkDir(res: QQResources) -> str | None:
        """
        Translate the working directory and its requested size into a PBS resource string.

        Args:
            res (QQResources): The resources requested for the job.

        Returns:
            str | None: Resource string specifying the working directory, or None if not set.
        """
        if not res.work_dir:
            return None

        # scratch in RAM (https://docs.metacentrum.cz/en/docs/computing/infrastructure/scratch-storages#scratch-in-ram)
        if res.work_dir == "scratch_shm":
            return f"{res.work_dir}=True"

        return f"{res.work_dir}={res.work_size}"

    @staticmethod
    def _translateKillForce(job_id: str) -> str:
        """
        Generate the PBS force kill command for a job.

        Args:
            job_id (str): The ID of the job to kill.

        Returns:
            str: The qdel command with force flag.
        """
        return f"qdel -W force {job_id}"

    @staticmethod
    def _translateKill(job_id: str) -> str:
        """
        Generate the standard PBS kill command for a job.

        Args:
            job_id (str): The ID of the job to kill.

        Returns:
            str: The qdel command without force flag.
        """
        return f"qdel {job_id}"

    @staticmethod
    def _translateSSHCommand(host: str, directory: Path) -> list[str]:
        """
        Construct the SSH command to navigate to a remote directory.

        Args:
            host (str): The hostname of the remote machine.
            directory (Path): The target directory to navigate to.

        Returns:
            list[str]: SSH command as a list suitable for subprocess execution.
        """
        return [
            "ssh",
            "-o PasswordAuthentication=no",  # never ask for password
            host,
            "-t",
            f"cd {directory} || exit {QQPBS.CD_FAIL} && exec bash -l",
        ]

    @staticmethod
    def _navigateSameHost(directory: Path):
        """
        Navigate to a directory on the current host using a subprocess.

        Args:
            directory (Path): Directory to navigate to.

        Returns:
            BatchOperationResult: Success if directory exists, error if directory does not exist.
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
    def _handleWorkDirRes(res: QQResources):
        # working directory was not specified by the user, select the default type
        if res.work_dir and res.work_dir == "from_batch_system":
            res.work_dir = QQPBS.DEFAULT_WORK_DIR
            logger.debug(f"Using default work-dir resource for PBS: '{res.work_dir}'.")

        # scratch in RAM (https://docs.metacentrum.cz/en/docs/computing/infrastructure/scratch-storages#scratch-in-ram)
        if res.work_dir and res.work_dir == "shared_hsm" and res.work_size is not None:
            raise QQError(
                f"Setting work-size is not supported for work-dir='{res.work_dir}'.\n"
                "Size of the in-RAM scratch is specified using the --mem property."
            )

        # running the job in the submission directory
        if res.work_dir and equals_normalized(res.work_dir, "jobdir"):
            res.work_dir = None
            if res.work_size is not None:
                raise QQError(
                    "Setting work-size is not supported for work-dir='job-dir'.\n"
                    'Job will run in the submission directory with "unlimited" capacity.'
                )


class PBSJobInfo(BatchJobInfoInterface):
    """
    Implementation of BatchJobInterface for PBS.
    """

    def __init__(self, job_id: str):
        self._job_id = job_id
        self._info: dict[str, str] = {}

        self.update()

    def update(self):
        # get job info from PBS
        command = f"qstat -fxw {self._job_id}"

        result = subprocess.run(
            ["bash"], input=command, text=True, check=False, capture_output=True
        )

        if result.returncode != 0:
            # if qstat fails, information is empty
            self._info: dict[str, str] = {}
        else:
            self._info = PBSJobInfo._parse_pbs_dump_to_dictionary(result.stdout)  # ty: ignore[possibly-unbound-attribute]

    def getJobState(self) -> BatchState:
        state = self._info.get("job_state")
        if not state:
            return BatchState.UNKNOWN

        return BatchState.fromCode(state)

    @staticmethod
    def _parse_pbs_dump_to_dictionary(text: str) -> dict[str, str]:
        """
        Parse a PBS job status dump into a dictionary.

        Returns:
            Dictionary mapping keys to values.
        """
        result: dict[str, str] = {}

        for raw_line in text.splitlines():
            line = raw_line.rstrip()

            if " = " not in line:
                continue

            key, value = line.split(" = ", 1)
            result[key.strip()] = value.strip()

        logger.debug(f"PBS qstat dump file: {result}")
        return result
