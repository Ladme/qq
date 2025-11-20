# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from dataclasses import fields
from pathlib import Path

from qq_lib.batch.interface import BatchInterface, BatchMeta
from qq_lib.core.common import split_files_list
from qq_lib.core.error import QQError
from qq_lib.properties.depend import Depend
from qq_lib.properties.job_type import JobType
from qq_lib.properties.loop import LoopInfo
from qq_lib.properties.resources import Resources

from .parser import Parser
from .submitter import Submitter


class SubmitterFactory:
    """
    Factory class to construct a Submitter instance based on parameters from
    the command-line and from the script itself.
    """

    def __init__(self, script: Path, command_line: list[str], **kwargs):
        """
        Initialize the factory with the script, command-line parameters, and additional options.

        Args:
            script (Path): Path to the script to submit.
            command_line (list[str]): All the arguments and options specified on the command line.
            **kwargs: Keyword arguments from the command line.
        """
        from qq_lib.submit.cli import submit

        self._parser = Parser(script, submit.params)
        self._script = script
        self._input_dir = script.parent
        self._kwargs = kwargs
        self._command_line = command_line

    def makeSubmitter(self) -> Submitter:
        """
        Construct and return a Submitter instance.

        Returns:
            Submitter: A fully initialized submitter object ready to submit a job.

        Raises:
            QQError: If required information, such as the submission queue, is missing.
        """
        self._parser.parse()

        BatchSystem = self._getBatchSystem()
        queue = self._getQueue()

        if (job_type := self._getJobType()) == JobType.LOOP:
            loop_info = self._getLoopInfo()
        else:
            loop_info = None

        return Submitter(
            BatchSystem,
            queue,
            self._getAccount(),
            self._script,
            job_type,
            self._getResources(BatchSystem, queue),
            self._command_line,
            loop_info,
            self._getExclude(),
            self._getInclude(),
            self._getDepend(),
        )

    def _getBatchSystem(self) -> type[BatchInterface]:
        """
        Determine which batch system to use for the job submission.

        Priority:
            1. Command-line specification
            2. Batch system specified in the script
            3. Environment variable
            4. Guessed batch system

        Returns:
            type[BatchInterface]: The selected batch system class.
        """
        if batch_system := self._kwargs.get("batch_system"):
            return BatchMeta.fromStr(batch_system)
        return self._parser.getBatchSystem() or BatchMeta.fromEnvVarOrGuess()

    def _getJobType(self) -> JobType:
        """
        Determine the type of job to submit.

        Priority:
            1. Command-line specification
            2. Job type specified in the script
            3. Default to `JobType.STANDARD`

        Returns:
            JobType: The determined job type.
        """
        if job_type := self._kwargs.get("job_type"):
            return JobType.fromStr(job_type)
        return self._parser.getJobType() or JobType.STANDARD

    def _getQueue(self) -> str:
        """
        Determine the submission queue to use.

        Priority:
            1. Command-line specification
            2. Queue specified in the script

        Returns:
            str: Name of the submission queue.

        Raises:
            QQError: If no queue is specified either in kwargs or in the script.
        """
        if not (queue := self._kwargs.get("queue") or self._parser.getQueue()):
            raise QQError("Submission queue not specified.")
        return queue

    def _getResources(self, BatchSystem: type[BatchInterface], queue: str) -> Resources:
        """
        Get the resource requirements for the job by merging the requirements specified on the command
        line with requirements specified inside the submitted script.

        The resources are then further modified to conform to the provided `BatchSystem` and submission `queue`.

        Args:
            BatchSystem (type[BatchInterface]): The batch system class to use.
            queue (str): The submission queue.

        Returns:
            Resources: A merged Resources object containing the final resource requirements.
        """
        field_names = {f.name for f in fields(Resources)}
        command_line_resources = Resources(
            **{k: v for k, v in self._kwargs.items() if k in field_names}
        )

        return BatchSystem.transformResources(
            queue,
            Resources.mergeResources(
                command_line_resources, self._parser.getResources()
            ),
        )

    def _getLoopInfo(self) -> LoopInfo:
        """
        Construct LoopInfo holding information about the loop job.

        Returns:
            LoopInfo: An object containing loop job parameters.

        Raises:
            QQError: If required loop job parameters are missing or invalid.
        """
        return LoopInfo(
            self._kwargs.get("loop_start") or self._parser.getLoopStart() or 1,
            self._kwargs.get("loop_end") or self._parser.getLoopEnd(),
            self._input_dir
            / (self._kwargs.get("archive") or self._parser.getArchive() or "storage"),
            self._kwargs.get("archive_format")
            or self._parser.getArchiveFormat()
            or "job%04d",
            input_dir=self._input_dir,
        )

    def _getExclude(self) -> list[Path]:
        """
        Determine the files to exclude from being copied to the job's working directory.

        Merges the list of files specified in command-line arguments with
        the list parsed from the script.

        Returns:
            list[Path]: List of relative file paths to exclude.
        """
        return list(
            set(
                split_files_list(self._kwargs.get("exclude"))
                + self._parser.getExclude()
            )
        )

    def _getInclude(self) -> list[Path]:
        """
        Determine the files to explicitly copy to the job's working directory.

        Merges the list of files specified in command-line arguments with
        the list parsed from the script.

        Returns:
            list[Path]: List of file paths to include.
        """
        return list(
            set(
                split_files_list(self._kwargs.get("include"))
                + self._parser.getInclude()
            )
        )

    def _getDepend(self) -> list[Depend]:
        """
        Determine the list of dependencies.

        Merges the list of dependencies specified in command-line arguments
        with the list parsed from the script.

        Returns:
            list[Depend]: List of job dependencies.
        """
        return (
            Depend.multiFromStr(self._kwargs.get("depend") or "") or []
        ) + self._parser.getDepend()

    def _getAccount(self) -> str | None:
        """
        Determine the account name to use for the job.

        Returns:
            str | None: The account name or None if not defined.
        """
        return self._kwargs.get("account") or self._parser.getAccount()
