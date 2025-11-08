# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import shutil
import subprocess
from pathlib import Path

from qq_lib.batch.interface import QQBatchInterface
from qq_lib.batch.interface.meta import QQBatchMeta, batch_system
from qq_lib.batch.pbs.qqpbs import QQPBS
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.depend import Depend
from qq_lib.properties.resources import QQResources

from .common import (
    SACCT_FIELDS,
    default_resources_from_dict,
    parse_slurm_dump_to_dictionary,
)
from .job import SlurmJob
from .node import SlurmNode
from .queue import SlurmQueue

logger = get_logger(__name__)


@batch_system
class QQSlurm(QQBatchInterface[SlurmJob, SlurmQueue, SlurmNode], metaclass=QQBatchMeta):
    def envName() -> str:
        return "Slurm"

    def isAvailable() -> bool:
        return shutil.which("sbatch") is not None and shutil.which("it4ifree") is None

    def getJobId() -> str | None:
        return os.environ.get("SLURM_JOB_ID")

    def getScratchDir(job_id: str) -> Path:
        raise NotImplementedError(
            "getScratchDir method is not implemented for this batch system implementation"
        )

    def jobSubmit(
        res: QQResources,
        queue: str,
        script: Path,
        job_name: str,
        depend: list[Depend],
        env_vars: dict[str, str],
        account: str | None = None,
    ) -> str:
        # intentionally using QQPBS
        QQPBS._sharedGuard(res, env_vars)

        command = QQSlurm._translateSubmit(
            res, queue, script.parent, str(script), job_name, depend, env_vars, account
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

        return result.stdout.split()[-1]

    def jobKill(job_id: str) -> None:
        command = QQSlurm._translateKill(job_id)
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
        command = QQSlurm._translateKillForce(job_id)
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

    def getBatchJob(job_id: str) -> SlurmJob:
        return SlurmJob(job_id)  # ty: ignore[invalid-return-type]

    def getUnfinishedBatchJobs(user: str) -> list[SlurmJob]:
        command = f'squeue -u {user} -t PENDING,RUNNING -h -o "%i"'
        logger.debug(command)

        return QQSlurm._getBatchJobsUsingSqueueCommand(command)

    def getBatchJobs(user: str) -> list[SlurmJob]:
        # get all jobs, except pending which are not available from sacct
        command = f"sacct -u {user} --allocations --noheader --parsable2 --format={SACCT_FIELDS}"
        logger.debug(command)

        sacct_jobs = QQSlurm._getBatchJobsUsingSacctCommand(command)

        # get pending jobs using squeue
        command = f'squeue -u {user} -t PENDING -h -o "%i"'
        logger.debug(command)

        squeue_jobs = QQSlurm._getBatchJobsUsingSqueueCommand(command)

        # filter out duplicate jobs
        merged = {job.getId(): job for job in sacct_jobs + squeue_jobs}
        return list(merged.values())

    def getAllUnfinishedBatchJobs() -> list[SlurmJob]:
        command = 'squeue -t PENDING,RUNNING -h -o "%i"'
        logger.debug(command)

        return QQSlurm._getBatchJobsUsingSqueueCommand(command)

    def getAllBatchJobs() -> list[SlurmJob]:
        # get all jobs, except pending which are not available from sacct
        command = f"sacct --allusers --allocations --noheader --parsable2 --format={SACCT_FIELDS}"
        logger.debug(command)

        sacct_jobs = QQSlurm._getBatchJobsUsingSacctCommand(command)

        # get pending jobs using squeue
        command = 'squeue -t PENDING -h -o "%i"'
        logger.debug(command)

        squeue_jobs = QQSlurm._getBatchJobsUsingSqueueCommand(command)

        # filter out duplicate jobs
        merged = {job.getId(): job for job in sacct_jobs + squeue_jobs}
        return list(merged.values())

    def getQueues() -> list[SlurmQueue]:
        command = "scontrol show partition -o"
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
        for line in result.stdout.splitlines():
            info = parse_slurm_dump_to_dictionary(line)
            queues.append(SlurmQueue.fromDict(info["PartitionName"], info))

        return queues

    def getNodes() -> list[SlurmNode]:
        raise NotImplementedError(
            "getNodes method is not implemented for this batch system implementations"
        )

    def readRemoteFile(host: str, file: Path) -> str:
        return QQPBS.readRemoteFile(host, file)

    def writeRemoteFile(host: str, file: Path, content: str) -> None:
        QQPBS.writeRemoteFile(host, file, content)

    def makeRemoteDir(host: str, directory: Path) -> None:
        QQPBS.makeRemoteDir(host, directory)

    def listRemoteDir(host: str, directory: Path) -> list[Path]:
        return QQPBS.listRemoteDir(host, directory)

    def moveRemoteFiles(host: str, files: list[Path], moved_files: list[Path]) -> None:
        QQPBS.moveRemoteFiles(host, files, moved_files)

    def syncWithExclusions(
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        exclude_files: list[Path] | None = None,
    ) -> None:
        QQPBS.syncWithExclusions(src_dir, dest_dir, src_host, dest_host, exclude_files)

    def syncSelected(
        src_dir: Path,
        dest_dir: Path,
        src_host: str | None,
        dest_host: str | None,
        include_files: list[Path] | None = None,
    ) -> None:
        QQPBS.syncSelected(src_dir, dest_dir, src_host, dest_host, include_files)

    def transformResources(queue: str, provided_resources: QQResources) -> QQResources:
        raise NotImplementedError(
            "transformResources method is not implemented for this batch system implementations"
        )

    def isShared(directory: Path) -> bool:
        return QQBatchInterface.isShared(directory)

    def resubmit(input_machine: str, input_dir: str, command_line: list[str]) -> None:
        QQBatchInterface.resubmit(
            input_machine=input_machine, input_dir=input_dir, command_line=command_line
        )

    def sortJobs(jobs: list[SlurmJob]) -> None:
        jobs.sort(key=lambda job: job.getIdsForSorting())

    @staticmethod
    def _translateKill(job_id: str) -> str:
        """
        Generate the Slurm kill command for a job using SIGTERM.

        Args:
            job_id (str): The ID of the job to kill.

        Returns:
            str: The scancel command sending SIGTERM.
        """
        return f"scancel {job_id}"

    @staticmethod
    def _translateKillForce(job_id: str) -> str:
        """
        Generate the Slurm kill command for a job using SIGKILL.

        Args:
            job_id (str): The ID of the job to kill.

        Returns:
            str: The scancel command sending SIGKILL.
        """
        return f"scancel --signal=KILL {job_id}"

    @staticmethod
    def _translateSubmit(
        res: QQResources,
        queue: str,
        input_dir: Path,
        script: str,
        job_name: str,
        depend: list[Depend],
        env_vars: dict[str, str],
        account: str | None,
    ) -> str:
        """
        Generate the Slurm submission command for a job.

        Args:
            res (QQResources): The resources requested for the job.
            queue (str): The queue name to submit to.
            input_dir (Path): The directory from which the job is being submitted.
            script (str): Path to the job script.
            job_name (str): Name of the job.
            depend (list[Depend]): List of dependencies of the job.
            env_vars (dict[str, str]): Dictionary of environment variables and their values to propagate to the job's environment.
            account (str | None): Optional name of the account to use for the job.

        Returns:
            str: The fully constructed sbatch command string.
        """
        qq_output = str((input_dir / job_name).with_suffix(CFG.suffixes.qq_out))
        command = f"sbatch -J {job_name} -p {queue} -e {qq_output} -o {qq_output} "

        if account:
            command += f"--account {account} "

        # translate environment variables
        if env_vars:
            command += f"--export ALL,{QQSlurm._translateEnvVars(env_vars)} "

        # handle number of nodes
        command += f"--nodes {res.nnodes} "

        # handle per-chunk resources
        translated = QQSlurm._translatePerChunkResources(res)
        command += " ".join(translated) + " "

        # handle properties
        if res.props:
            constraints = []
            for k, v in res.props.items():
                if v != "true":
                    raise QQError(
                        f"Slurm only supports properties with a value of 'true', not '{k}={v}'."
                    )
                constraints.append(k)

            command += f'--constraint="{"&".join(constraints)}" '

        # handle walltime
        if res.walltime:
            command += f"--time={res.walltime} "

        # handle dependencies
        if converted_depend := QQSlurm._translateDependencies(depend):
            command += f"--dependency={converted_depend} "

        # add script
        command += script

        return command

    @staticmethod
    def _translateEnvVars(env_vars: dict[str, str]) -> str:
        """
        Convert a dictionary of environment variables into a formatted string.

        Args:
            env_vars (dict[str, str]): A mapping of environment variable names
                to their corresponding values.

        Returns:
            str: A comma-separated string of environment variable assignments,
                suitable for inclusion in the sbatch command.
        """
        converted = []
        for key, value in env_vars.items():
            converted.append(f'{key}="{value}"')

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
                    in the sbatch command.

        Raises:
            QQError: If sanity checks fail or required memory attributes are missing.
        """

        trans_res = []

        # sanity checking per-chunk resources
        if res.nnodes is None:
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
            # we set MPI ranks and OpenMPI threads here, but these can be overriden
            # in the body of the script
            # this setup is here only to allow for better accounting by Slurm
            trans_res.append("--ntasks-per-node=1")
            trans_res.append(f"--cpus-per-task={res.ncpus // res.nnodes}")

        if res.mem:
            trans_res.append(f"--mem={(res.mem // res.nnodes).toStrExactSlurm()}")
        elif res.mem_per_cpu:
            trans_res.append(f"--mem-per-cpu={res.mem_per_cpu.toStrExactSlurm()}")
        else:
            # memory not set in any way
            raise QQError(
                "Attribute 'mem' and attribute 'mem-per-cpu' are not defined."
            )

        if res.ngpus:
            trans_res.append(f"--gpus-per-node={res.ngpus // res.nnodes}")

        return trans_res

    @staticmethod
    def _translateDependencies(depend: list[Depend]) -> str | None:
        """
        Convert a list of `Depend` objects into a Slurm-compatible dependency string.

        Args:
            depend (list[Depend]): List of dependency objects to translate.

        Returns:
            str | None: Slurm-style dependency string (e.g., "after:12345,afterok:1:2:3"),
                        or None if the input list is empty.
        """
        if not depend:
            return None

        return ",".join(Depend.toStr(x).replace("=", ":") for x in depend)

    @staticmethod
    def _getDefaultServerResources() -> QQResources:
        """
        Return a QQResources object representing the default resources for a batch job.

        Returns:
            QQResources: Default batch job resources obtained from `slurm.conf`.
        """
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
            server_resources, QQSlurm._getDefaultResources()
        )

    @staticmethod
    def _getDefaultResources() -> QQResources:
        """
        Return a QQResources object representing the default, hard-coded resources for a batch job.
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
    def _getBatchJobsUsingSacctCommand(command: str) -> list[SlurmJob]:
        """
        Execute `sacct` to retrieve information about Slurm jobs and parse it.

        Args:
            command (str): A Slurm command to get the relevant jobs.

        Returns:
            list[SlurmJob]: A list of `SlurmJob` instances corresponding to the jobs
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
        for sacct_string in result.stdout.split("\n"):
            if sacct_string.strip() == "":
                continue

            jobs.append(SlurmJob.fromSacctString(sacct_string))

        return jobs

    @staticmethod
    def _getBatchJobsUsingSqueueCommand(command: str) -> list[SlurmJob]:
        """
        Execute `squeue` and `scontrol show job` to retrieve information about Slurm jobs.

        Args:
            command (str): A Slurm command to get the relevant job IDs.

        Returns:
            list[SlurmJob]: A list of `SlurmJob` instances corresponding to the jobs
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
        for id in result.stdout.split("\n"):
            if id.strip() == "":
                continue

            jobs.append(SlurmJob(id))

        return jobs
