# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import os
import shutil
from pathlib import Path

from qq_lib.batch.interface import QQBatchInterface, QQBatchMeta
from qq_lib.core.error import QQError
from qq_lib.properties.depend import Depend
from qq_lib.properties.resources import QQResources

from .job import VBSJobInfo
from .system import VBSError, VirtualBatchSystem


class QQVBS(QQBatchInterface[VBSJobInfo], metaclass=QQBatchMeta):
    """
    Implementation of QQBatchInterface for the Virtual Batch System.
    """

    _batch_system = VirtualBatchSystem()

    def envName() -> str:
        return "VBS"

    def isAvailable() -> bool:
        # always available
        return True

    def getScratchDir(job_id: str) -> Path:
        job = QQVBS._batch_system.jobs.get(job_id)
        if not job:
            raise QQError(f"Job '{job_id}' does not exist.")

        scratch = job.scratch
        if not scratch:
            raise QQError(f"Job '{job_id}' does not have a scratch directory.")

        return scratch

    def jobSubmit(
        res: QQResources,
        _queue: str,
        script: Path,
        _job_name: str,
        _depend: list[Depend],
    ) -> str:
        try:
            return QQVBS._batch_system.submitJob(script, res.useScratch())
        except VBSError as e:
            raise QQError(f"Failed to submit script '{str(script)}': {e}")

    def jobKill(job_id: str) -> None:
        try:
            QQVBS._batch_system.killJob(job_id)
        except VBSError as e:
            raise QQError(f"Failed to kill job '{job_id}': {e}")

    def jobKillForce(job_id: str) -> None:
        try:
            QQVBS._batch_system.killJob(job_id, hard=True)
        except VBSError as e:
            raise QQError(f"Failed to kill job '{job_id}': {e}")

    def navigateToDestination(host: str, directory: Path) -> None:
        try:
            os.chdir(Path(host) / directory)
        except Exception:
            raise QQError(
                f"Could not reach '{host}:{str(directory)}': Could not change directory."
            )

    def readRemoteFile(_host: str, file: Path) -> str:
        # file is always local
        try:
            return file.read_text()
        except Exception as e:
            raise QQError(f"Could not read file '{file}': {e}.") from e

    def writeRemoteFile(_host: str, file: Path, content: str) -> None:
        # file is always local
        try:
            file.write_text(content)
        except Exception as e:
            raise QQError(f"Could not write file '{file}': {e}.") from e

    def makeRemoteDir(_host: str, directory: Path) -> None:
        # always local
        try:
            directory.mkdir(exist_ok=True)
        except Exception as e:
            raise QQError(f"Could not create a directory '{directory}': {e}") from e

    def listRemoteDir(_host: str, directory: Path) -> list[Path]:
        # always local
        try:
            return list(directory.iterdir())
        except Exception as e:
            raise QQError(f"Could not list a directory '{directory}': {e}") from e

    def moveRemoteFiles(host: str, files: list[Path], moved_files: list[Path]) -> None:
        if len(files) != len(moved_files):
            raise QQError(
                "The provided 'files' and 'moved_files' must have the same length."
            )

        # always local
        try:
            for src, dst in zip(files, moved_files):
                shutil.move(str(src), str(dst))
        except Exception as e:
            raise QQError("Could not move files.") from e

    def syncWithExclusions(
        src_dir: Path,
        dest_dir: Path,
        _src_host: str | None,
        _dest_host: str | None,
        exclude_files: list[Path] | None = None,
    ) -> None:
        # directories are always local
        QQBatchInterface.syncWithExclusions(
            src_dir, dest_dir, None, None, exclude_files
        )

    def syncSelected(
        src_dir: Path,
        dest_dir: Path,
        _src_host: str | None,
        _dest_host: str | None,
        include_files: list[Path],
    ) -> None:
        # directories are always local
        QQBatchInterface.syncSelected(src_dir, dest_dir, None, None, include_files)

    def transformResources(queue: str, provided_resources: QQResources) -> QQResources:
        return QQResources.mergeResources(
            provided_resources, QQVBS._getDefaultServerResources()
        )

    def isShared(_directory: Path) -> bool:
        # always shared
        return True

    def getJobInfo(job_id: str) -> VBSJobInfo:
        return VBSJobInfo(QQVBS._batch_system.jobs.get(job_id))  # ty: ignore[invalid-return-type]

    def resubmit(res: QQResources, script: Path) -> str:
        try:
            return QQVBS._batch_system.submitJob(script, res.useScratch())
        except VBSError as e:
            raise QQError(f"Failed to resubmit script '{str(script)}': {e}.")

    @staticmethod
    def _getDefaultServerResources() -> QQResources:
        """
        Return a QQResources object representing the default resources for a batch job.

        Returns:
            QQResources: Default batch job resources with predefined settings.
        """
        return QQResources(
            nnodes=1,
            ncpus=1,
            mem_per_cpu="1gb",
            work_dir="scratch_local",
            work_size_per_cpu="1gb",
            walltime="1d",
        )


# register QQVBS
QQBatchMeta.register(QQVBS)
