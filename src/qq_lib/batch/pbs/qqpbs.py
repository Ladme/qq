# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import os
import shutil
import socket
import subprocess
from collections.abc import Callable
from pathlib import Path

from qq_lib.batch.interface import QQBatchInterface, QQBatchMeta
from qq_lib.batch.interface.meta import batch_system
from qq_lib.batch.pbs.common import parseMultiPBSDumpToDictionaries
from qq_lib.batch.pbs.node import PBSNode
from qq_lib.batch.pbs.queue import PBSQueue
from qq_lib.core.common import equals_normalized
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.depend import Depend
from qq_lib.properties.resources import QQResources

from .job import PBSJobInfo

logger = get_logger(__name__)


@batch_system
class QQPBS(QQBatchInterface[PBSJobInfo, PBSQueue, PBSNode], metaclass=QQBatchMeta):
    """
    Implementation of QQBatchInterface for PBS Pro batch system.
    """

    # all standard scratch directory (excl. in RAM scratch) types supported by PBS
    SUPPORTED_SCRATCHES = ["scratch_local", "scratch_ssd", "scratch_shared"]

    def envName() -> str:
        return "PBS"

    def isAvailable() -> bool:
        return shutil.which("qsub") is not None

    def getJobId() -> str | None:
        return os.environ.get("PBS_JOBID")

    def getScratchDir(job_id: str) -> Path:
        scratch_dir = os.environ.get(CFG.env_vars.pbs_scratch_dir)
        if not scratch_dir:
            raise QQError(f"Scratch directory for job '{job_id}' is undefined")

        return Path(scratch_dir)

    def jobSubmit(
        res: QQResources,
        queue: str,
        script: Path,
        job_name: str,
        depend: list[Depend],
        env_vars: dict[str, str],
    ) -> str:
        QQPBS._sharedGuard(res, env_vars)

        # set env vars required for Infinity modules
        env_vars.update(QQPBS._collectAMSEnvVars())

        # get the submission command
        command = QQPBS._translateSubmit(
            res,
            queue,
            script.parent,
            str(script),
            job_name,
            depend,
            env_vars,
        )
        logger.debug(command)

        # submit the script
        result = subprocess.run(
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            raise QQError(
                f"Failed to submit script '{str(script)}': {result.stderr.strip()}."
            )

        return result.stdout.strip()

    def jobKill(job_id: str) -> None:
        command = QQPBS._translateKill(job_id)
        logger.debug(command)

        # run the kill command
        result = subprocess.run(
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            raise QQError(f"Failed to kill job '{job_id}': {result.stderr.strip()}.")

    def jobKillForce(job_id: str) -> None:
        command = QQPBS._translateKillForce(job_id)
        logger.debug(command)

        # run the kill command
        result = subprocess.run(
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            raise QQError(f"Failed to kill job '{job_id}': {result.stderr.strip()}.")

    def navigateToDestination(host: str, directory: Path) -> None:
        QQBatchInterface.navigateToDestination(host, directory)

    def getBatchJob(job_id: str) -> PBSJobInfo:
        return PBSJobInfo(job_id)  # ty: ignore[invalid-return-type]

    def getUnfinishedBatchJobs(user: str) -> list[PBSJobInfo]:
        command = f"qstat -fwu {user}"
        logger.debug(command)
        return QQPBS._getBatchJobsUsingCommand(command)

    def getBatchJobs(user: str) -> list[PBSJobInfo]:
        command = f"qstat -fwxu {user}"
        logger.debug(command)
        return QQPBS._getBatchJobsUsingCommand(command)

    def getAllUnfinishedBatchJobs() -> list[PBSJobInfo]:
        command = "qstat -fw"
        logger.debug(command)
        return QQPBS._getBatchJobsUsingCommand(command)

    def getAllBatchJobs() -> list[PBSJobInfo]:
        command = "qstat -fxw"
        logger.debug(command)
        return QQPBS._getBatchJobsUsingCommand(command)

    def getQueues() -> list[PBSQueue]:
        command = "qstat -Qfw"
        logger.debug(command)

        result = subprocess.run(
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            raise QQError(
                f"Could not retrieve information about queues: {result.stderr.strip()}."
            )

        queues = []
        for data, name in parseMultiPBSDumpToDictionaries(
            result.stdout.strip(), "Queue"
        ):
            queues.append(PBSQueue.fromDict(name, data))

        return queues

    def getNodes() -> list[PBSNode]:
        command = "pbsnodes -a"
        logger.debug(command)

        result = subprocess.run(
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            raise QQError(
                f"Could not retrieve information about nodes: {result.stderr.strip()}."
            )

        queues = []
        for data, name in parseMultiPBSDumpToDictionaries(result.stdout.strip(), None):
            queues.append(PBSNode.fromDict(name, data))

        return queues

    def readRemoteFile(host: str, file: Path) -> str:
        if os.environ.get(CFG.env_vars.shared_submit):
            # file is on shared storage, we can read it directly
            # this assumes that this method is only used to read files in input_dir
            logger.debug(f"Reading a file '{file}' from shared storage.")
            try:
                return file.read_text()
            except Exception as e:
                raise QQError(f"Could not read file '{file}': {e}.") from e
        else:
            # otherwise, we fall back to the default implementation
            logger.debug(f"Reading a remote file '{file}' on '{host}'.")
            return QQBatchInterface.readRemoteFile(host, file)

    def writeRemoteFile(host: str, file: Path, content: str) -> None:
        if os.environ.get(CFG.env_vars.shared_submit):
            # file should be written to shared storage
            # this assumes that the method is only used to write files into input_dir
            logger.debug(f"Writing a file '{file}' to shared storage.")
            try:
                file.write_text(content)
            except Exception as e:
                raise QQError(f"Could not write file '{file}': {e}.") from e
        else:
            # otherwise, we fall back to the default implementation
            logger.debug(f"Writing a remote file '{file}' on '{host}'.")
            QQBatchInterface.writeRemoteFile(host, file, content)

    def makeRemoteDir(host: str, directory: Path) -> None:
        if os.environ.get(CFG.env_vars.shared_submit):
            # assuming the directory is created in input_dir
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
        if os.environ.get(CFG.env_vars.shared_submit):
            # assuming we are listing input_dir or another directory on shared storage
            logger.debug(f"Listing a directory '{directory}' on shared storage.")
            try:
                return list(directory.iterdir())
            except Exception as e:
                raise QQError(f"Could not list a directory '{directory}': {e}.") from e
        else:
            # otherwise we fall back to the default implementation
            logger.debug(f"Listing a directory '{directory}' on '{host}'.")
            return QQBatchInterface.listRemoteDir(host, directory)

    def moveRemoteFiles(host: str, files: list[Path], moved_files: list[Path]) -> None:
        if len(files) != len(moved_files):
            raise QQError(
                "The provided 'files' and 'moved_files' must have the same length."
            )

        if os.environ.get(CFG.env_vars.shared_submit):
            # assuming we are moving files inside input_dir or another directory on shared storage
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
    ) -> None:
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
    ) -> None:
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

        # sanity check input_dir
        if equals_normalized(resources.work_dir, "job_dir") or equals_normalized(
            resources.work_dir, "input_dir"
        ):
            # work-size should not be used with job_dir
            if provided_resources.work_size:
                logger.warning(
                    "Setting work-size is not supported for work-dir='job_dir' or 'input_dir'.\n"
                    "Job will run in the submission directory with unlimited capacity.\n"
                    "The work-size attribute will be ignored."
                )

            resources.work_dir = "input_dir"
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
        supported_types = QQPBS.SUPPORTED_SCRATCHES + [
            "scratch_shm",
            "job_dir",
            "input_dir",  # same as job_dir
        ]
        raise QQError(
            f"Unknown working directory type specified: work-dir='{resources.work_dir}'. Supported types for PBS are: '{' '.join(supported_types)}'."
        )

    def isShared(directory: Path) -> bool:
        return QQBatchInterface.isShared(directory)

    def resubmit(input_machine: str, input_dir: str, command_line: list[str]) -> None:
        QQBatchInterface.resubmit(
            input_machine=input_machine, input_dir=input_dir, command_line=command_line
        )

    @staticmethod
    def _sharedGuard(res: QQResources, env_vars: dict[str, str]) -> None:
        """
        Ensure correct handling of shared vs. local submission directories.

        If the current working directory is on shared storage, adds the
        environment variable `SHARED_SUBMIT` to the list of env vars to propagate to the job.
        This environment variable is later used e.g. to select the appropriate data copying method.

        If the job is configured to use the submission directory as a working directory
        (`work-dir=input_dir` or 'job_dir') but that directory is not shared, a `QQError` is raised.

        Args:
            res (QQResources): The job's resource configuration.
            env_vars (dict[str, str]): Dictionary of environment variables to propagate to the job.

        Raises:
            QQError: If the job is set to run directly in the submission
                    directory while submission is from a non-shared filesystem.
        """
        if QQPBS.isShared(Path()):
            env_vars[CFG.env_vars.shared_submit] = "true"
        elif not res.usesScratch():
            # if job directory is used as working directory, it must always be shared
            raise QQError(
                "Job was requested to run directly in the submission directory (work-dir='job_dir' or 'input_dir'), but submission is done from a local filesystem."
            )

    @staticmethod
    def _translateSubmit(
        res: QQResources,
        queue: str,
        input_dir: Path,
        script: str,
        job_name: str,
        depend: list[Depend],
        env_vars: dict[str, str],
    ) -> str:
        """
        Generate the PBS submission command for a job.

        Args:
            res (QQResources): The resources requested for the job.
            queue (str): The queue name to submit to.
            input_dir (Path): The directory from which the job is being submitted.
            script (str): Path to the job script.
            job_name (str): Name of the job.
            depend (list[Depend]): List of dependencies of the job.

        Returns:
            str: The fully constructed qsub command string.
        """
        qq_output = str((input_dir / job_name).with_suffix(CFG.suffixes.qq_out))
        command = f"qsub -N {job_name} -q {queue} -j eo -e {qq_output} "

        # translate environment variables
        if env_vars:
            command += f"-v {QQPBS._translateEnvVars(env_vars)} "

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
            command += "-l place=vscatter "

        # handle dependencies
        if converted_depend := QQPBS._translateDependencies(depend):
            command += f"-W depend={converted_depend} "

        # add script
        command += script

        return command

    @staticmethod
    def _translateEnvVars(env_vars: dict[str, str]) -> str:
        converted = []
        for key, value in env_vars.items():
            converted.append(f"\"{key}='{value}'\"")

        return ",".join(converted)

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
            trans_res.append(f"mem={(res.mem // res.nnodes).toStrExact()}")
        elif res.mem_per_cpu and res.ncpus:
            trans_res.append(
                f"mem={(res.mem_per_cpu * res.ncpus // res.nnodes).toStrExact()}"
            )
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
            str | None: Resource string specifying the working directory, or None if input_dir is used.
        """
        assert res.nnodes is not None

        if res.work_dir == "job_dir" or res.work_dir == "input_dir":
            return None

        if res.work_dir == "scratch_shm":
            return f"{res.work_dir}=true"

        if res.work_size:
            return f"{res.work_dir}={(res.work_size // res.nnodes).toStrExact()}"

        if res.work_size_per_cpu and res.ncpus:
            return f"{res.work_dir}={(res.work_size_per_cpu * res.ncpus // res.nnodes).toStrExact()}"

        raise QQError(
            "Attribute 'work-size' or attributes 'work-size-per-cpu' and 'ncpus' are not defined."
        )

    @staticmethod
    def _translateDependencies(depend: list[Depend]) -> str | None:
        """
        Convert a list of `Depend` objects into a PBS-compatible dependency string.

        Args:
            depend (list[Depend]): List of dependency objects to translate.

        Returns:
            str | None: PBS-style dependency string (e.g., "after:12345,afterok:1:2:3"),
                        or None if the input list is empty.
        """
        if not depend:
            return None

        return ",".join(Depend.toStr(x).replace("=", ":") for x in depend)

    @staticmethod
    def _collectAMSEnvVars() -> dict[str, str]:
        """
        Collect environment variables for Infinity AMS.
        This allows importing Infinity AMS modules in qq jobs.

        Returns:
            dict[str, str]: Dictionary of AMS environment variables and their values.
        """
        ams_vars = {
            key: value
            for key, value in os.environ.items()
            if key
            in {
                "AMS_ACTIVE_MODULES",
                "AMS_SITE",
                "AMS_SITE_SUPPORT",
                "AMS_EXIT_CODE",
                "AMS_USER_CONFIG_DIR",
                "AMS_GROUPNS",
                "AMS_BUNDLE_NAME",
                "AMS_HOST_GROUP",
                "AMS_ROOT",
                "AMS_ROOT_V9",
                "AMS_BUNDLE_PATH",
            }
        }
        logger.debug(f"AMS vars: {ams_vars}")

        return ams_vars

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
        return QQResources(**PBSQueue(queue).getDefaultResources())

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
    ) -> None:
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
        if os.environ.get(CFG.env_vars.shared_submit):
            # input_dir is on shared storage -> we can copy files from/to it without connecting to the remote host
            logger.debug("Syncing directories on local and shared filesystem.")
            sync_function(src_dir, dest_dir, None, None, files)
        else:
            # input_dir is not on shared storage -> fall back to the default implementation
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
    def _getBatchJobsUsingCommand(command: str) -> list[PBSJobInfo]:
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
            ["bash"],
            input=command,
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )

        if result.returncode != 0:
            raise QQError(
                f"Could not retrieve information about jobs: {result.stderr.strip()}."
            )

        jobs = []
        for data, job_id in parseMultiPBSDumpToDictionaries(
            result.stdout.strip(), "Job Id"
        ):
            jobs.append(PBSJobInfo.fromDict(job_id, data))

        return jobs
