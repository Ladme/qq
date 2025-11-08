# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import getpass
import os
import shutil
import subprocess
from pathlib import Path

from qq_lib.batch.interface import QQBatchInterface
from qq_lib.batch.interface.meta import QQBatchMeta, batch_system
from qq_lib.batch.slurm import QQSlurm
from qq_lib.batch.slurm.common import (
    default_resources_from_dict,
    parse_slurm_dump_to_dictionary,
)
from qq_lib.batch.slurm.queue import SlurmQueue
from qq_lib.core.common import equals_normalized
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.resources import QQResources

logger = get_logger(__name__)


@batch_system
class QQSlurmIT4I(QQSlurm, metaclass=QQBatchMeta):
    def envName() -> str:
        return "SlurmIT4I"

    def isAvailable() -> bool:
        return shutil.which("it4ifree") is not None

    def getScratchDir(job_id: str) -> Path:
        if not (account := os.environ.get(CFG.env_vars.slurm_job_account)):
            raise QQError(f"No account is defined for job '{job_id}'.")

        # create a directory on scratch
        try:
            scratch = Path(
                f"/scratch/project/{account.lower()}/{getpass.getuser()}/qq-jobs/job_{job_id}"
            )
            scratch.mkdir(parents=True, exist_ok=True)
            return scratch
        except Exception as e:
            raise QQError(
                f"Could not create a scratch directory for job '{job_id}': {e}"
            ) from e

    def navigateToDestination(host: str, directory: Path) -> None:
        logger.info(
            f"Host '{host}' is not reachable in this environment. Navigating to '{directory}' on the current machine."
        )
        QQBatchInterface._navigateSameHost(directory)

    def readRemoteFile(host: str, file: Path) -> str:
        # file is always on shared storage
        _ = host
        try:
            return file.read_text()
        except Exception as e:
            raise QQError(f"Could not read file '{file}': {e}.") from e

    def writeRemoteFile(host: str, file: Path, content: str) -> None:
        # file is always on shared storage
        _ = host
        try:
            file.write_text(content)
        except Exception as e:
            raise QQError(f"Could not write file '{file}': {e}.") from e

    def makeRemoteDir(host: str, directory: Path) -> None:
        # directory is always on shared storage
        _ = host
        try:
            directory.mkdir(exist_ok=True)
        except Exception as e:
            raise QQError(f"Could not create a directory '{directory}': {e}.") from e

    def listRemoteDir(host: str, directory: Path) -> list[Path]:
        # directory is always on shared storage
        _ = host
        try:
            return list(directory.iterdir())
        except Exception as e:
            raise QQError(f"Could not list a directory '{directory}': {e}.") from e

    def moveRemoteFiles(host: str, files: list[Path], moved_files: list[Path]) -> None:
        if len(files) != len(moved_files):
            raise QQError(
                "The provided 'files' and 'moved_files' must have the same length."
            )

        # always on shared storage
        _ = host
        for src, dst in zip(files, moved_files):
            shutil.move(str(src), str(dst))

    def syncWithExclusions(
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        exclude_files: list[Path] | None = None,
    ) -> None:
        # always on shared storage
        _ = src_host
        _ = dest_host
        QQBatchInterface.syncWithExclusions(
            src_dir, dest_dir, None, None, exclude_files
        )

    def syncSelected(
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        include_files: list[Path] | None = None,
    ) -> None:
        # always on shared storage
        _ = src_host
        _ = dest_host
        QQBatchInterface.syncSelected(src_dir, dest_dir, None, None, include_files)

    def transformResources(queue: str, provided_resources: QQResources) -> QQResources:
        # default resources of the queue
        default_queue_resources = SlurmQueue(queue).getDefaultResources()
        # default server or hard-coded resources
        default_batch_resources = QQSlurmIT4I._getDefaultServerResources()

        # fill in default parameters
        resources = QQResources.mergeResources(
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

        if (
            not equals_normalized(resources.work_dir, "scratch")
            and not equals_normalized(resources.work_dir, "input_dir")
            and not equals_normalized(resources.work_dir, "job_dir")
        ):
            raise QQError(
                f"Unknown working directory type specified: work-dir='{resources.work_dir}'. Supported types for {QQSlurmIT4I.envName()} are: scratch input_dir job_dir."
            )

        return resources

    def isShared(directory: Path) -> bool:
        # always on shared storage
        return True

    def resubmit(**kwargs) -> None:
        input_dir = kwargs["input_dir"]
        command_line = kwargs["command_line"]

        qq_submit_command = f"{CFG.binary_name} submit {' '.join(command_line)}"

        logger.debug(f"Navigating to '{input_dir}' to execute '{qq_submit_command}'.")
        try:
            os.chdir(input_dir)
        except Exception as e:
            raise QQError(
                f"Could not resubmit the job. Could not navigate to '{input_dir}': {e}."
            ) from e

        logger.debug(f"Navigated to {input_dir}.")
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

    @staticmethod
    def _getDefaultServerResources() -> QQResources:
        command = "scontrol show config"

        result = subprocess.run(
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            logger.debug("Could not get server resources. Ignoring.")
            return QQResources()

        info = parse_slurm_dump_to_dictionary(result.stdout, "\n")
        server_resources = default_resources_from_dict(info)

        return QQResources.mergeResources(
            server_resources, QQSlurmIT4I._getDefaultResources()
        )

    def _getDefaultResources() -> QQResources:
        return QQResources(
            nnodes=1,
            ncpus=128,
            mem_per_cpu="1gb",
            work_dir="scratch",
            walltime="1d",
        )
