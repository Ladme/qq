# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import shutil
import socket
import subprocess
from pathlib import Path

from qq_lib.batch import (
    BatchJobInfoInterface,
    QQBatchInterface,
    QQBatchMeta,
)
from qq_lib.common import equals_normalized
from qq_lib.constants import QQ_OUT_SUFFIX, SHARED_SUBMIT
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

    # default work-dir type to use with PBS
    DEFAULT_WORK_DIR = "scratch_local"

    def envName() -> str:
        return "PBS"

    def isAvailable() -> bool:
        return shutil.which("qsub") is not None

    def getScratchDir(job_id: str) -> Path:
        scratch_dir = os.environ.get("SCRATCHDIR")
        if not scratch_dir:
            raise QQError(f"Scratch directory for job '{job_id}' is undefined")

        return Path(scratch_dir)

    def jobSubmit(res: QQResources, queue: str, script: Path) -> str:
        QQPBS._setShared()

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
        QQBatchInterface.navigateToDestination(host, directory)

    def buildResources(**kwargs) -> QQResources:
        try:
            res = QQBatchInterface.buildResources(**kwargs)
            QQPBS._handleWorkDirRes(res)
        except Exception as e:
            raise QQError(f"Specification of resources is invalid: {e}.") from e

        return res

    def getJobInfo(job_id: str) -> PBSJobInfo:
        return PBSJobInfo(job_id)  # ty: ignore[invalid-return-type]

    def readRemoteFile(host: str, file: Path) -> str:
        if os.environ.get(SHARED_SUBMIT):
            # file is on shared storage, we can read it directly
            # this assumes that this method is only used to read files in job_dir
            logger.debug(f"Reading a file '{file}' from shared storage.")
            try:
                return file.read_text()
            except Exception as e:
                raise QQError(f"Could not read file '{file}': {e}.") from e
        else:
            # otherwise, we fall back to the default implementation
            logger.debug(f"Reading a remote file '{file}' on '{host}'.")
            return QQBatchInterface.readRemoteFile(host, file)

    def writeRemoteFile(host: str, file: Path, content: str):
        if os.environ.get(SHARED_SUBMIT):
            # file should be written to shared storage
            # this assumes that the method is only used to write files into job_dir
            logger.debug(f"Writing a file '{file}' to shared storage.")
            try:
                file.write_text(content)
            except Exception as e:
                raise QQError(f"Could not write file '{file}': {e}.") from e
        else:
            # otherwise, we fall back to the default implementation
            logger.debug(f"Writing a remote file '{file}' on '{host}'.")
            QQBatchInterface.writeRemoteFile(host, file, content)

    def syncDirectories(
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        exclude_files: list[Path] | None = None,
    ):
        if os.environ.get(SHARED_SUBMIT):
            # job_dir is on shared storage -> we can copy files from/to it without connecting to the remote host
            logger.debug("Syncing directories on local and shared filesystem.")
            QQBatchInterface.syncDirectories(
                src_dir, dest_dir, None, None, exclude_files
            )
        else:
            # job_dir is not on shared storage -> fall back to the default implementation
            logger.debug("Syncing directories on local filesystems.")

            # convert local hosts to none
            local_hostname = socket.gethostname()
            src = None if src_host == local_hostname else src_host
            dest = None if dest_host == local_hostname else dest_host

            if src is None or dest is None:
                QQBatchInterface.syncDirectories(
                    src_dir, dest_dir, src, dest, exclude_files
                )
            else:
                raise QQError(
                    f"The source '{src_host}' and destination '{dest_host}' cannot be both remote."
                )

    @staticmethod
    def _setShared():
        """
        Set an environment variable indicating whether the job is submitted from shared storage.

        This information is used internally by QQPBS to determine how to copy data
        to the working directory when booting the job.

        Notes:
            If the current working directory is on shared storage, the environment
            variable `SHARED_SUBMIT` is set.
        """
        if QQPBS._isShared(Path()):
            os.environ[SHARED_SUBMIT] = "true"

    @staticmethod
    def _isShared(directory: Path) -> bool:
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
        Also performs additional validation.

        Args:
            res (QQResources): The resources requested for the job.

        Returns:
            list[str]: List of resource specifications for inclusion in the qsub command.
        """
        trans_res = []
        for name, value in res.toDict().items():
            # work_dir handled separately
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

        if res.work_size:
            return f"{res.work_dir}={res.work_size}"
        if res.ncpus:
            return f"{res.work_dir}={res.ncpus}gb"
        # TODO: choose a better default
        return f"{res.work_dir}=8gb"

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
