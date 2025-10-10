# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import re
import shutil
import socket
import subprocess
from collections.abc import Callable
from dataclasses import fields
from datetime import datetime, timedelta
from pathlib import Path
from typing import Self

import yaml

from qq_lib.core.common import equals_normalized, hhmmss_to_duration
from qq_lib.core.constants import (
    INFO_FILE,
    INPUT_DIR,
    PBS_DATE_FORMAT,
    QQ_OUT_SUFFIX,
    SHARED_SUBMIT,
)
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.resources import QQResources
from qq_lib.properties.size import Size
from qq_lib.properties.states import BatchState

from .interface import (
    BatchJobInfoInterface,
    QQBatchInterface,
    QQBatchMeta,
)

logger = get_logger(__name__)

# load faster YAML dumper
try:
    from yaml import CDumper as Dumper  # ty: ignore[possibly-unbound-import]

    logger.debug("Loaded YAML CDumper.")
except ImportError:
    from yaml import Dumper

    logger.debug("Loaded default YAML dumper.")


# forward declaration
class PBSJobInfo(BatchJobInfoInterface):
    pass


class QQPBS(QQBatchInterface[PBSJobInfo], metaclass=QQBatchMeta):
    """
    Implementation of QQBatchInterface for PBS Pro batch system.
    """

    # all standard scratch directory (excl. in RAM scratch) types supported by PBS
    SUPPORTED_SCRATCHES = ["scratch_local", "scratch_ssd", "scratch_shared"]

    def envName() -> str:
        return "PBS"

    def isAvailable() -> bool:
        return shutil.which("qsub") is not None

    def getScratchDir(job_id: str) -> Path:
        scratch_dir = os.environ.get("SCRATCHDIR")
        if not scratch_dir:
            raise QQError(f"Scratch directory for job '{job_id}' is undefined")

        return Path(scratch_dir)

    def jobSubmit(res: QQResources, queue: str, script: Path, job_name: str) -> str:
        QQPBS._sharedGuard(res)

        # get the submission command
        command = QQPBS._translateSubmit(res, queue, str(script), job_name)
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

    def getJobInfo(job_id: str) -> PBSJobInfo:
        return PBSJobInfo(job_id)  # ty: ignore[invalid-return-type]

    def getUnfinishedJobsInfo(user: str) -> list[PBSJobInfo]:
        command = f"qstat -fwu {user}"
        logger.debug(command)
        return QQPBS._getJobsInfoUsingCommand(command)

    def getJobsInfo(user: str) -> list[PBSJobInfo]:
        command = f"qstat -fwxu {user}"
        logger.debug(command)
        return QQPBS._getJobsInfoUsingCommand(command)

    def getAllUnfinishedJobsInfo() -> list[PBSJobInfo]:
        command = "qstat -fw"
        logger.debug(command)
        return QQPBS._getJobsInfoUsingCommand(command)

    def getAllJobsInfo() -> list[PBSJobInfo]:
        command = "qstat -fxw"
        logger.debug(command)
        return QQPBS._getJobsInfoUsingCommand(command)

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

    def makeRemoteDir(host: str, directory: Path):
        if os.environ.get(SHARED_SUBMIT):
            # assuming the directory is created in job_dir
            logger.debug(f"Creating a directory '{directory}' on shared storage.")
            try:
                directory.mkdir(exist_ok=True)
            except Exception as e:
                raise QQError(
                    f"Could not create a directory '{directory}': {e}."
                ) from e
        else:
            # otherwise we fall back to the default implementation
            logger.debug(f"Creating a directory '{directory}' on '{host}'.")
            QQBatchInterface.makeRemoteDir(host, directory)

    def listRemoteDir(host: str, directory: Path) -> list[Path]:
        if os.environ.get(SHARED_SUBMIT):
            # assuming we are listing job_dir or another directory on shared storage
            logger.debug(f"Listing a directory '{directory}' on shared storage.")
            try:
                return list(directory.iterdir())
            except Exception as e:
                raise QQError(f"Could not list a directory '{directory}': {e}.") from e
        else:
            # otherwise we fall back to the default implementation
            logger.debug(f"Listing a directory '{directory}' on '{host}'.")
            return QQBatchInterface.listRemoteDir(host, directory)

    def moveRemoteFiles(host: str, files: list[Path], moved_files: list[Path]):
        if len(files) != len(moved_files):
            raise QQError(
                "The provided 'files' and 'moved_files' must have the same length."
            )

        if os.environ.get(SHARED_SUBMIT):
            # assuming we are moving files inside job_dir or another directory on shared storage
            logger.debug(
                f"Moving files '{files}' -> '{moved_files}' on a shared storage."
            )
            for src, dst in zip(files, moved_files):
                shutil.move(str(src), str(dst))
        else:
            # otherwise we fall back to the default implementation
            logger.debug(f"Moving files '{files}' -> '{moved_files}' on '{host}'.")
            QQBatchInterface.moveRemoteFiles(host, files, moved_files)

    def syncWithExclusions(
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        exclude_files: list[Path] | None = None,
    ):
        QQPBS._syncDirectories(
            src_dir,
            dest_dir,
            src_host,
            dest_host,
            exclude_files,
            QQBatchInterface.syncWithExclusions,
        )

    def syncSelected(
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        include_files: list[Path] | None = None,
    ):
        QQPBS._syncDirectories(
            src_dir,
            dest_dir,
            src_host,
            dest_host,
            include_files,
            QQBatchInterface.syncSelected,
        )

    def transformResources(queue: str, provided_resources: QQResources) -> QQResources:
        # default resources of the queue
        default_queue_resources = QQPBS._getDefaultQueueResources(queue)
        # default hard-coded resources
        default_batch_resources = QQPBS._getDefaultServerResources()

        # fill in default parameters
        resources = QQResources.mergeResources(
            provided_resources, default_queue_resources, default_batch_resources
        )
        if not resources.work_dir:
            raise QQError(
                "Work-dir is not set after filling in default attributes. This is a bug."
            )

        # sanity check job_dir
        if equals_normalized(resources.work_dir, "job_dir"):
            # work-size should not be used with job_dir
            if provided_resources.work_size:
                logger.warning(
                    "Setting work-size is not supported for work-dir='job_dir'.\n"
                    'Job will run in the submission directory with "unlimited" capacity.\n'
                    "The work-size attribute will be ignored."
                )

            resources.work_dir = "job_dir"
            return resources

        # scratch in RAM (https://docs.metacentrum.cz/en/docs/computing/infrastructure/scratch-storages#scratch-in-ram)
        if equals_normalized(resources.work_dir, "scratch_shm"):
            # work-size should not be used with scratch_shm
            if provided_resources.work_size:
                logger.warning(
                    "Setting work-size is not supported for work-dir='scratch_shm'.\n"
                    "Size of the in-RAM scratch is specified using the --mem property.\n"
                    "The work-size attribute will be ignored."
                )

            resources.work_dir = "scratch_shm"
            resources.work_size = None
            return resources

        # if work-dir matches any of the "standard" scratches supported by PBS
        if match := next(
            (
                x
                for x in QQPBS.SUPPORTED_SCRATCHES
                if equals_normalized(x, resources.work_dir)
            ),
            None,
        ):
            resources.work_dir = match
            return resources

        # unknown work-dir type
        supported_types = QQPBS.SUPPORTED_SCRATCHES + ["scratch_shm", "job_dir"]
        raise QQError(
            f"Unknown working directory type specified: work-dir='{resources.work_dir}'. Supported types for PBS are: '{' '.join(supported_types)}'."
        )

    def isShared(directory: Path) -> bool:
        return QQBatchInterface.isShared(directory)

    def resubmit(input_machine: str, job_dir: str, command_line: list[str]):
        QQBatchInterface.resubmit(
            input_machine=input_machine, job_dir=job_dir, command_line=command_line
        )

    @staticmethod
    def _sharedGuard(res: QQResources):
        """
        Ensure correct handling of shared vs. local submission directories.

        If the current working directory is on shared storage, sets the
        environment variable `SHARED_SUBMIT`. This environment variable
        is later used e.g. to select the appropriate data copying method.

        If the job is configured to use the submission directory as a working directory
        (`work-dir=job_dir`) but that directory is not shared, a `QQError` is raised.

        Args:
            res (QQResources): The job's resource configuration.

        Raises:
            QQError: If the job is set to run directly in the submission
                    directory while submission is from a non-shared filesystem.
        """
        if QQPBS.isShared(Path()):
            os.environ[SHARED_SUBMIT] = "true"
        else:
            # if job directory is used as working directory, it must always be shared
            if not res.useScratch():
                raise QQError(
                    "Job was requested to run directly in the submission directory (work-dir='job_dir'), but submission is done from a local filesystem."
                )

    @staticmethod
    def _translateSubmit(
        res: QQResources, queue: str, script: str, job_name: str
    ) -> str:
        """
        Generate the PBS submission command for a job.

        Args:
            res (QQResources): The resources requested for the job.
            queue (str): The queue name to submit to.
            script (str): Path to the job script.
            job_name (str): Name of the job.

        Returns:
            str: The fully constructed qsub command string.
        """
        qq_output = str(Path(job_name).with_suffix(QQ_OUT_SUFFIX))
        command = f"qsub -N {job_name} -q {queue} -j eo -e {qq_output} -V "

        # handle per-chunk resources, incl. workdir
        translated = QQPBS._translatePerChunkResources(res)

        # handle properties
        if res.props:
            translated.extend([f"{k}={v}" for k, v in res.props.items()])

        if len(translated) > 0 and res.nnodes and res.nnodes > 1:
            # we only use the select syntax when multiple nodes are requested
            command += f"-l select={res.nnodes}:"
            join_char = ":"
        else:
            command += "-l "
            join_char = ","

        command += join_char.join(translated) + " "

        # handle walltime
        if res.walltime:
            command += f"-l walltime={res.walltime} "

        if res.nnodes and res.nnodes > 1:
            # 'place=scatter' causes each chunk to be placed on a different node
            command += "-l place=scatter "

        # add script
        command += script

        return command

    @staticmethod
    def _translatePerChunkResources(res: QQResources) -> list[str]:
        """
        Convert a QQResources object into a list of per-node resource specifications.

        Each resource that can be divided by the number of nodes (nnodes) is split
        accordingly.

        Args:
            res (QQResources): The resource specification for the job.

        Returns:
            list[str]: A list of per-node resource strings suitable for inclusion
                    in a PBS submission command.

        Raises:
            QQError: If sanity checks fail or required memory attributes are missing.
        """

        trans_res = []

        # sanity checking per-chunk resources
        if not res.nnodes:
            raise QQError(
                "Attribute 'nnodes' should not be undefined. This is a bug, please report it."
            )
        if res.nnodes == 0:
            raise QQError("Attribute 'nnodes' cannot be 0.")

        if res.ncpus and res.ncpus != 0 and res.ncpus % res.nnodes != 0:
            raise QQError(
                f"Attribute 'ncpus' ({res.ncpus}) must be divisible by 'nnodes' ({res.nnodes})."
            )
        if res.ngpus and res.ngpus != 0 and res.ngpus % res.nnodes != 0:
            raise QQError(
                f"Attribute 'ngpus' ({res.ngpus}) must be divisible by 'nnodes' ({res.nnodes})."
            )

        # translate per-chunk resources
        if res.ncpus:
            trans_res.append(f"ncpus={res.ncpus // res.nnodes}")

        if res.mem:
            trans_res.append(f"mem={str(res.mem // res.nnodes)}")
        elif res.mem_per_cpu and res.ncpus:
            trans_res.append(f"mem={str(res.mem_per_cpu * res.ncpus // res.nnodes)}")
        else:
            # memory not set in any way
            raise QQError(
                "Attribute 'mem' or attributes 'mem-per-cpu' and 'ncpus' are not defined."
            )

        if res.ngpus:
            trans_res.append(f"ngpus={res.ngpus // res.nnodes}")

        # translate work-dir
        if workdir := QQPBS._translateWorkDir(res):
            trans_res.append(workdir)

        return trans_res

    def _translateWorkDir(res: QQResources) -> str | None:
        """
        Translate the working directory and its requested size into a PBS resource string.

        Args:
            res (QQResources): The resources requested for the job.

        Returns:
            str | None: Resource string specifying the working directory, or None if job_dir is used.
        """
        assert res.nnodes is not None

        if res.work_dir == "job_dir":
            return None

        if res.work_dir == "scratch_shm":
            return f"{res.work_dir}=true"

        if res.work_size:
            return f"{res.work_dir}={str(res.work_size // res.nnodes)}"

        if res.work_size_per_cpu and res.ncpus:
            return (
                f"{res.work_dir}={str(res.work_size_per_cpu * res.ncpus // res.nnodes)}"
            )

        raise QQError(
            "Attribute 'work-size' or attributes 'work-size-per-cpu' and 'ncpus' are not defined."
        )

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

    @staticmethod
    def _getDefaultQueueResources(queue: str) -> QQResources:
        """
        Query PBS for the default resources of a given queue.

        Args:
            queue (str): The name of the PBS queue to query.

        Returns:
            QQResources: A QQResources object populated with the queue's default resources.
                        If the queue cannot be queried or an error occurs, returns an empty QQResources object.
        """
        command = f"qstat -Qfw {queue}"

        result = subprocess.run(
            ["bash"], input=command, text=True, check=False, capture_output=True
        )

        if result.returncode != 0:
            # no default resources for a queue
            logger.warning(f"Could not get information about the queue {queue}.")
            return QQResources()
        info = QQPBS._parseQueueInfoToDictionary(result.stdout)
        # ignore fields not defined in the dataclass
        field_names = {f.name for f in fields(QQResources)}
        filtered = {k: v for k, v in info.items() if k in field_names}
        return QQResources(**filtered)

    @staticmethod
    def _parseQueueInfoToDictionary(text: str) -> dict[str, str]:
        """
        Parse the output of a PBS queue query and extract default resource values.

        Args:
            text (str): Raw string output from a PBS qstat command.

        Returns:
            dict[str, str]: A dictionary mapping resource names to their default values
                            extracted from the queue info.
        """
        result: dict[str, str] = {}

        for raw_line in text.splitlines():
            line = raw_line.rstrip()

            if " = " not in line:
                continue

            key, value = line.split(" = ", 1)
            if "resources_default" in key:
                resource = key.split(".")[-1]
                result[resource.strip()] = value.strip()

        logger.debug(f"PBS queue info: {result}")
        return result

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
    def _syncDirectories(
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        files: list[Path] | None,
        sync_function: Callable[
            [Path, Path, str | None, str | None, list[Path] | None], None
        ],
    ):
        """
        Synchronize directories either locally or across remote hosts, depending on the environment and setup.

        Args:
            src_dir (Path): Source directory to sync from.
            dest_dir (Path): Destination directory to sync to.
            src_host (str | None): Hostname of the source machine if remote; None if local.
            dest_host (str | None): Hostname of the destination machine if remote; None if local.
            files (list[Path] | None): Optional list of file paths to include or exclude, depending on `sync_function`.
            sync_function (Callable): Function to perform the actual synchronization.

        Raises:
            QQError: If both source and destination hosts are remote and cannot be
                accessed simultaneously, or if syncing fails internally.
        """
        if os.environ.get(SHARED_SUBMIT):
            # job_dir is on shared storage -> we can copy files from/to it without connecting to the remote host
            logger.debug("Syncing directories on local and shared filesystem.")
            sync_function(src_dir, dest_dir, None, None, files)
        else:
            # job_dir is not on shared storage -> fall back to the default implementation
            logger.debug("Syncing directories on local filesystems.")

            # convert local hosts to none
            local_hostname = socket.gethostname()
            src = None if src_host == local_hostname else src_host
            dest = None if dest_host == local_hostname else dest_host

            if src is None or dest is None:
                sync_function(src_dir, dest_dir, src, dest, files)
            else:
                raise QQError(
                    f"The source '{src_host}' and destination '{dest_host}' cannot be both remote."
                )

    @staticmethod
    def _getJobsInfoUsingCommand(command: str) -> list[PBSJobInfo]:
        """
        Execute a shell command to retrieve information about PBS jobs and parse it.

        Args:
            command (str): The shell command to execute, typically a PBS query command.

        Returns:
            list[PBSJobInfo]: A list of `PBSJobInfo` instances corresponding to the jobs
                            returned by the command.

        Raises:
            QQError: If the command fails (non-zero return code) or if the output
                    cannot be parsed into valid job information.
        """
        ...
        result = subprocess.run(
            ["bash"], input=command, text=True, check=False, capture_output=True
        )

        if result.returncode != 0:
            raise QQError(
                f"Could not retrieve information about jobs: {result.stderr.strip()}."
            )

        jobs = []
        for data, job_id in PBSJobInfo._parseMultiPBSDumpToDictionaries(  # ty: ignore[possibly-unbound-attribute]
            result.stdout.strip()
        ):
            jobs.append(PBSJobInfo.fromDict(job_id, data))  # ty: ignore[possibly-unbound-attribute]

        return jobs


class PBSJobInfo(BatchJobInfoInterface):
    """
    Implementation of BatchJobInterface for PBS.
    """

    def __init__(self, job_id: str):
        self._job_id = job_id
        self._info: dict[str, str] = {}

        self.update()

    def isEmpty(self) -> bool:
        return not self._info

    def getJobId(self) -> str:
        return self._job_id

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
            self._info = PBSJobInfo._parsePBSDumpToDictionary(result.stdout)  # ty: ignore[possibly-unbound-attribute]

    def getJobState(self) -> BatchState:
        if not (state := self._info.get("job_state")):
            return BatchState.UNKNOWN

        # if the job is finished and the return code is not zero, return FAILED
        if state == "F":
            exit_code = self.getExitCode()
            # if exit code does not exist, the job never ran and was likely killed
            if exit_code is None or exit_code != 0:
                return BatchState.FAILED

        return BatchState.fromCode(state)

    def getJobComment(self) -> str | None:
        return self._info.get("comment")

    def getJobEstimated(self) -> tuple[datetime, str] | None:
        if not (raw_time := self._info.get("estimated.start_time")):
            logger.debug("No 'estimated.start_time' found.")
            return None

        try:
            time = datetime.strptime(raw_time, PBS_DATE_FORMAT)
        except Exception as e:
            logger.debug(f"Could not parse 'estimated.start_time': {e}.")
            return None

        if not (raw_vnode := self._info.get("estimated.exec_vnode")):
            logger.debug("No 'estimated.exec_vnode' found.")
            return None

        vnodes = []
        for split in raw_vnode.split("+"):
            vnodes.append(PBSJobInfo._cleanNodeName(split.strip()))  # ty: ignore[possibly-unbound-attribute]

        return (time, " + ".join(vnodes))

    def getMainNode(self) -> str | None:
        if raw_node := self._info.get("exec_host2"):
            return PBSJobInfo._cleanNodeName(raw_node.split("+")[0].strip())  # ty: ignore[possibly-unbound-attribute]

        return None

    def getNodes(self) -> list[str] | None:
        if not (raw_nodes := self._info.get("exec_host2")):
            return None

        nodes = []
        for node in raw_nodes.split("+"):
            nodes.append(PBSJobInfo._cleanNodeName(node.strip()))  # ty: ignore[possibly-unbound-attribute]

        return nodes

    def getShortNodes(self) -> list[str] | None:
        if not (raw_nodes := self._info.get("exec_host")):
            return None

        nodes = []
        for node in raw_nodes.split("+"):
            nodes.append(PBSJobInfo._cleanNodeName(node.strip()))  # ty: ignore[possibly-unbound-attribute]

        return nodes

    def getJobName(self) -> str:
        if not (name := self._info.get("Job_Name")):
            logger.warning(f"Could not get job name for '{self._job_id}'.")
            return "?????"

        return name

    def getNCPUs(self) -> int:
        return self._getIntProperty("Resource_List.ncpus", "the number of CPUs")

    def getNGPUs(self) -> int:
        return self._getIntProperty("Resource_List.ngpus", "the number of GPUs")

    def getNNodes(self) -> int:
        return self._getIntProperty("Resource_List.nodect", "the number of nodes")

    def getMem(self) -> Size:
        if not (mem := self._info.get("Resource_List.mem")):
            logger.debug(
                f"Could not get information about the amount of memory from the batch system for '{self._job_id}'."
            )
            return Size(1, "kb")

        try:
            return Size.from_string(mem)
        except Exception as e:
            logger.warning(f"Could not parse memory for '{self._job_id}': {e}.")
            return Size(1, "kb")

    def getStartTime(self) -> datetime | None:
        return self._getDatetimeProperty("stime", "the job starting time")

    def getSubmissionTime(self) -> datetime:
        return (
            self._getDatetimeProperty("ctime", "the job submission time")
            or datetime.min  # arbitrary datetime - submission time should always be available
        )

    def getCompletionTime(self) -> datetime | None:
        return self._getDatetimeProperty("obittime", "the job completion time")

    def getModificationTime(self) -> datetime:
        return (
            self._getDatetimeProperty("mtime", "the job modification time")
            or self.getSubmissionTime()
        )

    def getUser(self) -> str:
        if not (user := self._info.get("Job_Owner")):
            logger.warning(f"Could not get user for '{self._job_id}'.")
            return "?????"

        return user.split("@")[0]

    def getWalltime(self) -> timedelta:
        if not (walltime := self._info.get("Resource_List.walltime")):
            logger.warning(f"Could not get walltime for '{self._job_id}'.")
            return timedelta(0)

        try:
            return hhmmss_to_duration(walltime)
        except QQError as e:
            logger.warning(f"Could not parse walltime for '{self._job_id}': {e}.")
            return timedelta(0)

    def getQueue(self) -> str:
        if not (queue := self._info.get("queue")):
            logger.warning(f"Could not get queue for '{self._job_id}'.")
            return "?????"

        return queue

    def getUtilCPU(self) -> int | None:
        if not (util_cpu := self._info.get("resources_used.cpupercent")):
            logger.debug(
                f"Information about CPU utilization is not available for '{self._job_id}'."
            )
            return None

        try:
            # PBS report CPU utilization in the same way as `top` - we have to divide by number of CPUs
            return int(util_cpu) // self.getNCPUs()
        except Exception as e:
            # this catches both invalid util_cpu and invalid getNCPUs
            logger.warning(
                f"Could not parse information about CPU utilization for '{self._job_id}': {e}."
            )
            return None

    def getUtilMem(self) -> int | None:
        if not (util_mem := self._info.get("resources_used.mem")):
            logger.debug(
                f"Information about memory utilization is not available for '{self._job_id}'."
            )
            return None

        try:
            # we assume that resources_used.mem is always in kb (or in b if 0)
            util_mem_kb = int(util_mem.replace("kb", "").replace("b", ""))
            return int(util_mem_kb / self.getMem().to_kb() * 100.0)
        except Exception as e:
            logger.warning(
                f"Could not parse information about memory utilization for '{self._job_id}': {e}."
            )
            return None

    def getExitCode(self) -> int | None:
        if not (exit := self._info.get("Exit_status")):
            return None

        try:
            return int(exit)
        except Exception as e:
            logger.warning(f"Could not parse exit code for '{self._job_id}': {e}.")
            return None

    def getInputMachine(self) -> str:
        if not (input_machine := self._info.get("Submit_Host")):
            logger.warning(f"Could not get input machine for '{self._job_id}'.")
            return "?????"

        return input_machine

    def getInputDir(self) -> Path:
        if not (env_vars := self._getEnvVars()):
            logger.warning(
                f"Could not get list of environment variables for '{self._job_id}'."
            )
            return Path("???")

        if not (
            job_dir := env_vars.get("PBS_O_WORKDIR")  # try PBS first
            or env_vars.get(INPUT_DIR)  # if this fails, try qq
            or env_vars.get("INF_INPUT_DIR")  # if this fails, try Infinity
        ):
            logger.warning(f"Could not obtain input directory for '{self._job_id}'.")
            return Path("???")

        return Path(job_dir).resolve()

    def getInfoFile(self) -> Path | None:
        if not (env_vars := self._getEnvVars()):
            logger.warning(
                f"Could not get list of environment variables for '{self._job_id}'."
            )
            return None

        if not (info_file := env_vars.get(INFO_FILE)):
            logger.debug(
                f"Job '{self._job_id}' does not have an assigned qq info file."
            )
            return None

        return Path(info_file)

    def toYaml(self) -> str:
        # we need to add job id to the start of the dictionary
        to_dump = {"Job Id": self._job_id} | self._info
        return yaml.dump(
            to_dump, default_flow_style=False, sort_keys=False, Dumper=Dumper
        )

    @classmethod
    def fromDict(cls, job_id: str, info: dict[str, str]) -> Self:
        """
        Construct a new instance of PBSJobInfo from a job ID and a dictionary of job information.

        This method bypasses the standard initializer and directly sets the `_job_id` and `_info`
        attributes of the new instance.

        Args:
            job_id (str): The unique identifier of the job.
            info (dict[str, str]): A dictionary containing PBS job metadata as key-value pairs.

        Returns:
            Self: A new instance of PBSJobInfo.

        Note:
            This method does not perform any validation or processing of the provided dictionary.
        """
        job_info = cls.__new__(cls)
        job_info._job_id = job_id
        job_info._info = info

        return job_info

    def _getEnvVars(self) -> dict[str, str] | None:
        if not (variable_list := self._info.get("Variable_List")):
            return None

        return dict(
            item.split("=", 1) for item in variable_list.split(",") if "=" in item
        )

    def _getIntProperty(self, property: str, property_name: str) -> int:
        try:
            return int(self._info[property])
        except Exception:
            logger.debug(
                f"Could not get information about {property_name} from the batch system for '{self._job_id}'."
            )
            # if not specified, we assume 0
            return 0

    def _getDatetimeProperty(
        self, property: str, property_name: str
    ) -> datetime | None:
        if not (raw_datetime := self._info.get(property)):
            return None

        try:
            return datetime.strptime(raw_datetime, PBS_DATE_FORMAT)
        except Exception:
            logger.warning(
                f"Could not parse information about {property_name} for '{self._job_id}'."
            )
            return None

    @staticmethod
    def _parsePBSDumpToDictionary(text: str) -> dict[str, str]:
        """
        Parse a PBS job status dump into a dictionary.

        Returns:
            dict[str, str]: Dictionary mapping keys to values.
        """
        result: dict[str, str] = {}

        for raw_line in text.splitlines():
            line = raw_line.rstrip()

            if " = " not in line:
                continue

            key, value = line.split(" = ", 1)
            result[key.strip()] = value.strip()

        # logger.debug(f"PBS qstat dump: {result}")
        return result

    @staticmethod
    def _parseMultiPBSDumpToDictionaries(text: str) -> list[tuple[dict[str, str], str]]:
        """
        Parse a PBS job dump containing metadata for multiple jobs into structured dictionaries.

        Args:
            text (str): The raw PBS job dump containing information about one or more jobs.

        Returns:
            list[tuple[dict[str, str], str]]: A list of tuples, each containing:
                - dict[str, str]: Parsed job metadata for a single job.
                - str: The job ID extracted from job information.

        Raises:
            QQError: If the job ID cannot be extracted.
        """
        if text.strip() == "":
            return []

        data = []

        job_id_pattern = re.compile(r"^\s*Job Id:\s*(.*)$")
        for chunk in text.rstrip().split("\n\n"):
            try:
                first_line = chunk.splitlines()[0]
                match = job_id_pattern.match(first_line)
                if not match:
                    raise

                job_id = match.group(1)
            except Exception as e:
                raise QQError(
                    f"Invalid PBS dump format. Could not extract job id from:\n{chunk}"
                ) from e

            data.append((PBSJobInfo._parsePBSDumpToDictionary(chunk), job_id))  # ty: ignore[possibly-unbound-attribute]

        logger.debug(f"Detected and parsed metadata for {len(data)} PBS jobs.")
        return data

    @staticmethod
    def _cleanNodeName(raw: str) -> str:
        """
        Normalize a raw node string to extract the clean hostname.

        Args:
            raw (str): Raw node string reported by the batch system.

        Returns:
            str: Cleaned node name.
        """
        return raw.split(":", 1)[0].split("/", 1)[0].replace("(", "").replace(")", "")
