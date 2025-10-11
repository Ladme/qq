# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from dataclasses import fields
from pathlib import Path

from click import Parameter

from qq_lib.batch.interface import QQBatchInterface, QQBatchMeta
from qq_lib.core.common import split_files_list
from qq_lib.core.error import QQError
from qq_lib.properties.job_type import QQJobType
from qq_lib.properties.loop import QQLoopInfo
from qq_lib.properties.resources import QQResources
from qq_lib.submit.parser import QQParser

from .submitter import QQSubmitter


class QQSubmitterFactory:
    """
    Factory class to construct a QQSubmitter instance based on parameters from
    the command-line and from the script itself.
    """

    def __init__(
        self, script: Path, params: list[Parameter], command_line: list[str], **kwargs
    ):
        """
        Initialize the factory with the script, command-line parameters, and additional options.

        Args:
            script (Path): Path to the script to submit.
            params (list[Parameter]): List of all known submission parameters.
            command_line (list[str]): All the arguments and options specified on the command line.
            **kwargs: Keyword arguments from the command line.
        """
        self._parser = QQParser(script, params)
        self._script = script
        self._kwargs = kwargs
        self._command_line = command_line

    def makeSubmitter(self) -> QQSubmitter:
        """
        Construct and return a QQSubmitter instance.

        Returns:
            QQSubmitter: A fully initialized submitter object ready to submit a job.

        Raises:
            QQError: If required information, such as the submission queue, is missing.
        """
        self._parser.parse()

        BatchSystem = self._getBatchSystem()
        queue = self._getQueue()

        if (job_type := self._getJobType()) == QQJobType.LOOP:
            loop_info = self._getLoopInfo()
        else:
            loop_info = None

        return QQSubmitter(
            BatchSystem,
            queue,
            self._script,
            job_type,
            self._getResources(BatchSystem, queue),
            loop_info,
            self._getExclude(),
            self._command_line,
            self._getInteractive(),
        )

    def _getBatchSystem(self) -> type[QQBatchInterface]:
        """
        Determine which batch system to use for the job submission.

        Priority:
            1. Command-line specification
            2. Batch system specified in the script
            3. Environment variable
            4. Guessed batch system

        Returns:
            type[QQBatchInterface]: The selected batch system class.
        """
        if batch_system := self._kwargs.get("batch_system"):
            return QQBatchMeta.fromStr(batch_system)
        return self._parser.getBatchSystem() or QQBatchMeta.fromEnvVarOrGuess()

    def _getJobType(self) -> QQJobType:
        """
        Determine the type of job to submit.

        Priority:
            1. Command-line specification
            2. Job type specified in the script
            3. Default to `QQJobType.STANDARD`

        Returns:
            QQJobType: The determined job type.
        """
        if job_type := self._kwargs.get("job_type"):
            return QQJobType.fromStr(job_type)
        return self._parser.getJobType() or QQJobType.STANDARD

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

    def _getResources(
        self, BatchSystem: type[QQBatchInterface], queue: str
    ) -> QQResources:
        """
        Get the resource requirements for the job by merging the requirements specified on the command
        line with requirements specified inside the submitted script.

        The resources are then further modified to conform to the provided `BatchSystem` and submission `queue`.

        Args:
            BatchSystem (type[QQBatchInterface]): The batch system class to use.
            queue (str): The submission queue.

        Returns:
            QQResources: A merged QQResources object containing the final resource requirements.
        """
        field_names = {f.name for f in fields(QQResources)}
        command_line_resources = QQResources(
            **{k: v for k, v in self._kwargs.items() if k in field_names}
        )

        return BatchSystem.transformResources(
            queue,
            QQResources.mergeResources(
                command_line_resources, self._parser.getResources()
            ),
        )

    def _getLoopInfo(self) -> QQLoopInfo:
        """
        Construct QQLoopInfo holding information about the loop job.

        Returns:
            QQLoopInfo: An object containing loop job parameters.

        Raises:
            QQError: If required loop job parameters are missing or invalid.
        """
        return QQLoopInfo(
            self._kwargs.get("loop_start") or self._parser.getLoopStart() or 1,
            self._kwargs.get("loop_end") or self._parser.getLoopEnd(),
            Path(self._kwargs.get("archive") or self._parser.getArchive() or "storage"),
            self._kwargs.get("archive_format")
            or self._parser.getArchiveFormat()
            or "job%04d",
            input_dir=Path.cwd(),
        )

    def _getExclude(self) -> list[Path]:
        """
        Determine the files to exclude from the job submission.

        Merges the list of files specified in command-line arguments with
        the list parsed from the script.

        Returns:
            list[Path]: List of absolute file paths to exclude.
        """
        return list(
            set(
                split_files_list(self._kwargs.get("exclude"))
                + self._parser.getExclude()
            )
        )

    def _getInteractive(self) -> bool:
        """
        Determine whether the job should be submitted interactively.

        Returns:
            bool: True if interactive mode should be used, False if non-interactive.
        """
        return not (
            self._kwargs.get("non_interactive") or self._parser.getNonInteractive()
        )
