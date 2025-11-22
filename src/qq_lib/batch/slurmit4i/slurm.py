# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import getpass
import os
import shutil
import subprocess
from pathlib import Path

from qq_lib.batch.interface import BatchInterface
from qq_lib.batch.interface.meta import BatchMeta, batch_system
from qq_lib.batch.slurm import Slurm
from qq_lib.batch.slurm.queue import SlurmQueue
from qq_lib.core.common import equals_normalized
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.resources import Resources

logger = get_logger(__name__)


@batch_system
class SlurmIT4I(Slurm, metaclass=BatchMeta):
    """
    Implementation of BatchInterface for Slurm on IT4I clusters.
    """

    # all scratch directory types supported by SlurmIT4I
    SUPPORTED_SCRATCHES = ["scratch"]

    @classmethod
    def envName(cls) -> str:
        return "SlurmIT4I"

    @classmethod
    def isAvailable(cls) -> bool:
        return shutil.which("it4ifree") is not None

    @classmethod
    def getScratchDir(cls, job_id: str) -> Path:
        if not (account := os.environ.get(CFG.env_vars.slurm_job_account)):
            raise QQError(f"No account is defined for job '{job_id}'.")

        user = getpass.getuser()

        # we attempt to create the scratch directory multiple times in different user directory;
        # if the user directory is already created but the user does not have permissions
        # to write into it, we append a number to the user's name and try creating a new directory
        last_exception = None
        for attempt in range(CFG.it4i_scratch_dir_attempts):
            user_component = (
                user if attempt == 0 else f"{user}{attempt + 1}"
            )  # appended number is 2 for the second attempt

            scratch = Path(
                f"/scratch/project/{account.lower()}/{user_component}/qq-jobs/job_{job_id}"
            )

            try:
                scratch.mkdir(parents=True, exist_ok=True)
                return scratch
            except Exception as e:
                last_exception = e

        # if all attempts failed
        raise QQError(
            f"Could not create a scratch directory for job '{job_id}' after {CFG.it4i_scratch_dir_attempts} attempts: {last_exception}"
        ) from last_exception

    @classmethod
    def getSupportedWorkDirTypes(cls) -> list[str]:
        return cls.SUPPORTED_SCRATCHES + [
            "input_dir",
            "job_dir",  # same as input_dir
        ]

    @classmethod
    def navigateToDestination(cls, host: str, directory: Path) -> None:
        logger.info(
            f"Host '{host}' is not reachable in this environment. Navigating to '{directory}' on the current machine."
        )
        BatchInterface._navigateSameHost(directory)

    @classmethod
    def readRemoteFile(cls, host: str, file: Path) -> str:
        # file is always on shared storage
        _ = host
        try:
            return file.read_text()
        except Exception as e:
            raise QQError(f"Could not read file '{file}': {e}.") from e

    @classmethod
    def writeRemoteFile(cls, host: str, file: Path, content: str) -> None:
        # file is always on shared storage
        _ = host
        try:
            file.write_text(content)
        except Exception as e:
            raise QQError(f"Could not write file '{file}': {e}.") from e

    @classmethod
    def makeRemoteDir(cls, host: str, directory: Path) -> None:
        # directory is always on shared storage
        _ = host
        try:
            directory.mkdir(exist_ok=True)
        except Exception as e:
            raise QQError(f"Could not create a directory '{directory}': {e}.") from e

    @classmethod
    def listRemoteDir(cls, host: str, directory: Path) -> list[Path]:
        # directory is always on shared storage
        _ = host
        try:
            return list(directory.iterdir())
        except Exception as e:
            raise QQError(f"Could not list a directory '{directory}': {e}.") from e

    @classmethod
    def deleteRemoteDir(cls, host: str, directory: Path) -> None:
        # directory is always on shared storage
        _ = host
        try:
            shutil.rmtree(directory)
        except Exception as e:
            raise QQError(f"Could not delete directory '{directory}': {e}.") from e

    @classmethod
    def moveRemoteFiles(
        cls, host: str, files: list[Path], moved_files: list[Path]
    ) -> None:
        if len(files) != len(moved_files):
            raise QQError(
                "The provided 'files' and 'moved_files' must have the same length."
            )

        # always on shared storage
        _ = host
        for src, dst in zip(files, moved_files):
            shutil.move(str(src), str(dst))

    @classmethod
    def syncWithExclusions(
        cls,
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        exclude_files: list[Path] | None = None,
    ) -> None:
        # always on shared storage
        _ = src_host
        _ = dest_host
        BatchInterface.syncWithExclusions(src_dir, dest_dir, None, None, exclude_files)

    @classmethod
    def syncSelected(
        cls,
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        include_files: list[Path] | None = None,
    ) -> None:
        # always on shared storage
        _ = src_host
        _ = dest_host
        BatchInterface.syncSelected(src_dir, dest_dir, None, None, include_files)

    @classmethod
    def transformResources(cls, queue: str, provided_resources: Resources) -> Resources:
        # default resources of the queue
        default_queue_resources = SlurmQueue(queue).getDefaultResources()
        # default server or hard-coded resources
        default_batch_resources = cls._getDefaultServerResources()

        # fill in default parameters
        resources = Resources.mergeResources(
            provided_resources, default_queue_resources, default_batch_resources
        )
        if not resources.work_dir:
            raise QQError(
                "Work-dir is not set after filling in default attributes. This is a bug."
            )

        if provided_resources.work_size_per_cpu or provided_resources.work_size:
            logger.warning(
                "Setting work-size is not supported in this environment. Working directory has a virtually unlimited capacity."
            )

        if not any(
            equals_normalized(resources.work_dir, dir)
            for dir in cls.getSupportedWorkDirTypes()
        ):
            raise QQError(
                f"Unknown working directory type specified: work-dir='{resources.work_dir}'. Supported types for {cls.envName()} are: {' '.join(cls.getSupportedWorkDirTypes())}."
            )

        return resources

    @classmethod
    def isShared(cls, directory: Path) -> bool:
        _ = directory
        # always on shared storage
        return True

    @classmethod
    def resubmit(
        cls, input_machine: str, input_dir: Path, command_line: list[str]
    ) -> None:
        # input machine is unused, resubmit from the current machine
        _ = input_machine

        qq_submit_command = f"{CFG.binary_name} submit {' '.join(command_line)}"

        logger.debug(f"Navigating to '{input_dir}' to execute '{qq_submit_command}'.")
        try:
            os.chdir(input_dir)
        except Exception as e:
            raise QQError(
                f"Could not resubmit the job. Could not navigate to '{input_dir}': {e}."
            ) from e

        logger.debug(f"Navigated to {str(input_dir)}.")
        result = subprocess.run(
            ["bash"],
            input=qq_submit_command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            raise QQError(f"Could not resubmit the job: {result.stderr.strip()}.")

    @classmethod
    def _getDefaultResources(cls) -> Resources:
        return Resources(
            nnodes=1,
            ncpus_per_node=128,
            mem_per_cpu="1gb",
            work_dir="scratch",
            walltime="1d",
        )
