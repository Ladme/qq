# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import sys
from dataclasses import dataclass, fields
from datetime import datetime
from pathlib import Path
from typing import Self

import click
import yaml
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from qq_lib.batch import BatchJobInfoInterface, QQBatchInterface, QQBatchMeta
from qq_lib.common import format_duration, get_info_files
from qq_lib.constants import DATE_FORMAT
from qq_lib.error import QQError
from qq_lib.logger import get_logger
from qq_lib.resources import QQResources
from qq_lib.states import BatchState, NaiveState, RealState

logger = get_logger(__name__)


@click.command(help="Get information about the qq job.")
def info():
    """
    Get information about the qq job submitted from this directory.
    """
    try:
        for file in get_info_files(Path()):
            informer = QQInformer.fromFile(file)
            console = Console()
            panel = informer.createJobStatusPanel(console)
            console.print(panel)
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        sys.exit(91)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(99)


@dataclass
class QQInfo:
    """
    Dataclass storing information about a qq job.

    Exposes only minimal functionality for loading, exporting, and basic access.
    More complex operations, such as transforming or combining the data
    should be implemented in QQInformer.
    """

    # The batch system class used
    batch_system: type[QQBatchInterface]

    # Version of qq that submitted the job
    qq_version: str

    # Name of the user who submitted the job
    username: str

    # Job identifier inside the batch system
    job_id: str

    # Job name
    job_name: str

    # Name of the script executed
    script_name: str

    # Type of job (standard, loop...)
    job_type: str

    # Host from which the job was submitted
    input_machine: str

    # Directory from which the job was submitted
    job_dir: Path

    # Job state according to qq
    job_state: NaiveState

    # Job submission timestamp
    submission_time: datetime

    # Name of the file for storing standard output of the executed script
    stdout_file: str

    # Name of the file for storing error output of the executed script
    stderr_file: str

    # Resources allocated to the job
    resources: QQResources

    # List of files to not copy to the working directory
    excluded_files: list[Path] | None = None

    # Job start time
    start_time: datetime | None = None

    # Main node assigned to the job
    main_node: str | None = None

    # Working directory
    work_dir: Path | None = None

    # Job completion time
    completion_time: datetime | None = None

    # Exit code of qq run
    job_exit_code: int | None = None

    @classmethod
    def fromFile(cls, file: Path, host: str | None = None) -> Self:
        """
        Load a QQInfo instance from a YAML file, either locally or on a remote host.

        If 'host' is provided, the file will be read from the remote host using
        the batch system's 'readRemoteFile' method. Otherwise, the file is read locally.

        Args:
            file (Path): Path to the YAML qq info file.
            host (str | None): Optional hostname of the remote machine where the file resides.
                If None, the file is assumed to be local.

        Returns:
            QQInfo: Instance constructed from the file.

        Raises:
            QQError: If the file does not exist, cannot be reached, cannot be parsed,
                    or does not contain all mandatory information.
        """
        try:
            if host:
                # remote file
                logger.debug(f"Loading qq info from '{file}' on '{host}'.")

                BatchSystem = QQBatchMeta.guess()
                data: dict[str, object] = yaml.safe_load(
                    BatchSystem.readRemoteFile(host, file)
                )
            else:
                # local file
                logger.debug(f"Loading qq info from '{file}'.")

                if not file.exists():
                    raise QQError(f"qq info file '{file}' does not exist.")

                with file.open("r") as input:
                    data: dict[str, object] = yaml.safe_load(input)

            return cls._fromDict(data)
        except yaml.YAMLError as e:
            raise QQError(f"Could not parse the qq info file '{file}': {e}.") from e
        except TypeError as e:
            raise QQError(
                f"Mandatory information missing from the qq info file '{file}': {e}."
            ) from e

    def toFile(self, file: Path, host: str | None = None):
        """
        Export this QQInfo instance to a YAML file, either locally or on a remote host.

        If 'host' is provided, the file will be written to the remote host using
        the batch system's 'writeRemoteFile' method. Otherwise, the file is written locally.

        Args:
            file (Path): Path to write the YAML file.
            host (str | None): Optional hostname of the remote machine where the file should be written.
                If None, the file is written locally.

        Raises:
            QQError: If the file cannot be created, reached, or written to.
        """
        try:
            content = "# qq job info file\n" + self._toYaml() + "\n"

            if host:
                # remote file
                logger.debug(f"Exporting qq info into '{file}' on '{host}'.")
                self.batch_system.writeRemoteFile(host, file, content)
            else:
                # local file
                logger.debug(f"Exporting qq info into '{file}'.")
                with file.open("w") as output:
                    output.write(content)
        except Exception as e:
            raise QQError(f"Cannot create or write to file '{file}': {e}") from e

    def _toYaml(self) -> str:
        """
        Serialize the QQInfo instance to a YAML string.

        Returns:
            YAML representation of the QQInfo object.
        """
        return yaml.dump(self._toDict(), default_flow_style=False, sort_keys=False)

    def _toDict(self) -> dict[str, object]:
        """
        Convert the QQInfo instance into a dictionary of string-object pairs.
        Fields that are None are ignored.

        Returns:
            Dictionary containing all fields with non-None values, converting
            enums and nested objects appropriately.
        """
        result: dict[str, object] = {}

        for f in fields(self):
            value = getattr(self, f.name)
            # ignore None fields
            if value is None:
                continue

            # convert resources
            if f.type == QQResources:
                result[f.name] = value.toDict()
            # convert the state and the batch system
            elif (
                f.type == NaiveState
                or f.type == type[QQBatchInterface]
                or f.type == Path
                or f.type == Path | None
            ):
                result[f.name] = str(value)
            # convert list of excluded files
            elif f.type == list[Path] or f.type == list[Path] | None:
                result[f.name] = [str(x) if hasattr(x, "__str__") else x for x in value]
            # convert timestamp
            elif f.type == datetime or f.type == datetime | None:
                result[f.name] = value.strftime(DATE_FORMAT)
            else:
                result[f.name] = value

        return result

    @classmethod
    def _fromDict(cls, data: dict[str, object]) -> Self:
        """
        Construct a QQInfo instance from a dictionary.

        Args:
            data: Dictionary containing field names and values.

        Returns:
            A QQInfo instance.

        Raises:
            TypeError: If required fields are missing.
        """
        init_kwargs = {}
        for f in fields(cls):
            name = f.name
            # skip undefined fields
            if name not in data:
                continue

            value = data[name]

            # convert resources
            if f.type == QQResources:
                init_kwargs[name] = QQResources(**value)
            # convert the batch system
            elif f.type == type[QQBatchInterface] and isinstance(value, str):
                init_kwargs[name] = QQBatchMeta.fromStr(value)
            # convert the job state
            elif f.type == NaiveState and isinstance(value, str):
                init_kwargs[name] = (
                    NaiveState.fromStr(value) if value else NaiveState.UNKNOWN
                )
            # convert paths (incl. optional paths)
            elif f.type == Path or f.type == Path | None:
                init_kwargs[name] = Path(value)
            # convert the list of excluded paths
            elif f.type == list[Path] | None and isinstance(value, list):
                init_kwargs[name] = [
                    Path(v) if isinstance(v, str) else v for v in value
                ]
            # convert timestamp
            elif (f.type == datetime or f.type == datetime | None) and isinstance(
                value, str
            ):
                init_kwargs[name] = datetime.strptime(value, DATE_FORMAT)
            else:
                init_kwargs[name] = value

        return cls(**init_kwargs)


class QQInformer:
    """
    Provides an interface to access and manipulate qq job information.

    Wraps a QQInfo object and exposes convenience methods for updating job state,
    retrieving real-time state from the batch system, and creating visual job status panels.
    """

    def __init__(self, info: QQInfo):
        """
        Initialize the informer with job information.

        Args:
            info: A QQInfo object containing raw job data.
        """
        self.info = info
        self._batch_info: BatchJobInfoInterface | None = None

    @property
    def batch_system(self) -> type[QQBatchInterface]:
        """
        Return the batch system class used for this job.

        Returns:
            The QQBatchInterface implementation associated with the job.
        """
        return self.info.batch_system

    @classmethod
    def fromFile(cls, file: Path, host: str | None = None) -> Self:
        """
        Create a QQInformer by loading job information from a file.

        If 'host' is provided, the file is read from the remote host; otherwise, it is read locally.

        Args:
            file (Path): Path to a YAML file containing job information.
            host (str | None): Optional remote host from which to read the file.

        Returns:
            QQInformer: An instance initialized with the loaded QQInfo.

        Raises:
            QQError: If the file cannot be read, reached, or parsed correctly.
        """
        return cls(QQInfo.fromFile(file, host))

    def toFile(self, file: Path, host: str | None = None):
        """
        Export the job information to a file.

        If 'host' is provided, the file is written to the remote host; otherwise, it is written locally.

        Args:
            file (Path): Path to the output YAML file.
            host (str | None): Optional remote host where the file should be written.

        Raises:
            QQError: If the file cannot be created, reached, or written to.
        """
        self.info.toFile(file, host)

    def setRunning(self, time: datetime, main_node: str, work_dir: Path):
        """
        Mark the job as running and set associated metadata.

        Args:
            time: Job start time.
            main_node: Main node assigned to the job.
            work_dir: Working directory used by the job.
        """
        self.info.job_state = NaiveState.RUNNING
        self.info.start_time = time
        self.info.main_node = main_node
        self.info.work_dir = work_dir

    def setFinished(self, time: datetime):
        """
        Mark the job as finished successfully.

        Args:
            time: Job completion time.
        """
        self.info.job_state = NaiveState.FINISHED
        self.info.completion_time = time
        self.info.job_exit_code = 0

    def setFailed(self, time: datetime, exit_code: int):
        """
        Mark the job as failed.

        Args:
            time: Job completion (failure) time.
            exit_code: Exit code of the failed job.
        """
        self.info.job_state = NaiveState.FAILED
        self.info.completion_time = time
        self.info.job_exit_code = exit_code

    def setKilled(self, time: datetime):
        """
        Mark the job as killed.

        Args:
            time: Time when the job was killed.
        """
        self.info.job_state = NaiveState.KILLED
        self.info.completion_time = time

    def useScratch(self) -> bool:
        """
        Determine if the job uses a scratch directory.

        Returns:
            True if a scratch is used, False if it is not.
        """
        return self.info.resources.useScratch()

    def getDestination(self) -> tuple[str, Path] | None:
        """
        Retrieve the job's main node and working directory.

        Returns:
            A tuple of (main_node, work_dir) if both are set, otherwise None.
        """
        if all((self.info.main_node, self.info.work_dir)):
            return self.info.main_node, self.info.work_dir
        return None

    def getBatchState(self) -> BatchState:
        """
        Return the job's state as reported by the batch system.

        Uses cached information if available; otherwise queries the batch system
        via `batch_system.getJobInfo`. This avoids unnecessary remote calls.
        """
        if not self._batch_info:
            self._batch_info = self.batch_system.getJobInfo(self.info.job_id)

        return self._batch_info.getJobState()

    def getRealState(self) -> RealState:
        """
        Get the job's real state by combining qq's internal state (`NaiveState`)
        with the state reported by the batch system (`BatchState`).

        Uses cached information if available; otherwise queries the batch system
        via `batch_system.getJobInfo`. This avoids unnecessary remote calls.
        """
        # shortcut: if the naive state is finished, failed, killed or unknown,
        # there is no need to check batch state
        if self.info.job_state in {
            NaiveState.FINISHED,
            NaiveState.FAILED,
            NaiveState.KILLED,
            NaiveState.UNKNOWN,
        }:
            logger.debug(
                "Short-circuiting getRealState: the batch state will not affect the result."
            )
            return RealState.fromStates(self.info.job_state, BatchState.UNKNOWN)

        return RealState.fromStates(self.info.job_state, self.getBatchState())

    def createJobStatusPanel(self, console: Console | None = None) -> Group:
        """
        Create a textual status panel showing the job state and details.

        Args:
            console: Optional Rich Console object.

        Returns:
            A Rich Group object containing a panel with job status information.
        """
        state = self.getRealState()

        (message, details) = self._getStateMessages(
            state,
            self.info.start_time or self.info.submission_time,
            self.info.completion_time or datetime.now(),
        )

        console = console or Console()
        term_width = console.size.width
        panel_width = max(60, term_width // 3)

        table = Table(show_header=False, box=None, padding=(0, 1))
        table.add_column(justify="right", style="bold")
        table.add_column(justify="left")

        table.add_row("Job state:", Text(message, style=f"{state.color} bold"))
        if details.strip():
            table.add_row("", Text(details, style="white"))

        panel = Panel(
            table,
            title=Text(f"JOB: {self.info.job_id}", style="bold", justify="center"),
            border_style="white",
            padding=(1, 2),
            width=panel_width,
        )

        return Group(Text(""), panel, Text(""))

    def _getStateMessages(
        self, state: RealState, start_time: datetime, end_time: datetime
    ) -> tuple[str, str]:
        """
        Map a RealState to user-friendly messages for display.

        Args:
            state: The RealState of the job.
            start_time: Start time of the job or relevant state period.
            end_time: End time of the job or relevant state period.

        Returns:
            Tuple containing a short message and a message with additional information.
        """
        match state:
            case RealState.QUEUED:
                return (
                    "Job is queued",
                    f"In queue for {format_duration(end_time - start_time)}",
                )
            case RealState.HELD:
                return (
                    "Job is held",
                    f"In queue for {format_duration(end_time - start_time)}",
                )
            case RealState.SUSPENDED:
                return ("Job is suspended", "")
            case RealState.WAITING:
                return (
                    "Job is waiting",
                    f"In queue for {format_duration(end_time - start_time)}",
                )
            case RealState.RUNNING:
                return (
                    "Job is running",
                    f"Running for {format_duration(end_time - start_time)} on '{self.info.main_node}'",
                )
            case RealState.BOOTING:
                return ("Job is booting", "Preparing the working directory...")
            case RealState.KILLED:
                return ("Job has been killed", f"Killed at {end_time}")
            case RealState.FAILED:
                return (
                    "Job has failed",
                    f"Failed at {end_time} [exit code: {self.info.job_exit_code}]",
                )
            case RealState.FINISHED:
                return ("Job has finished", f"Completed at {end_time}")
            case RealState.IN_AN_INCONSISTENT_STATE:
                return (
                    "Job is in an inconsistent state",
                    "The batch system and qq disagree on the status of the job",
                )
            case RealState.UNKNOWN:
                return (
                    "Job is in an unknown state",
                    "Job is in a state that qq does not recognize",
                )

        return (
            "Job is in an unknown state",
            "Job is in a state that qq does not recognize",
        )
