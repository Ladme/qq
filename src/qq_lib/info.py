# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import sys
from dataclasses import dataclass, fields
from datetime import datetime
from pathlib import Path
from typing import Any, Self

import click
import yaml
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

import qq_lib
from qq_lib.batch import QQBatchMeta, QQBatchInterface
from qq_lib.common import convert_to_batch_system, get_info_file
from qq_lib.error import QQError
from qq_lib.logger import get_logger
from qq_lib.resources import QQResources
from qq_lib.states import BatchState, NaiveState, QQState

# TODO: Move to shared location
DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

logger = get_logger(__name__)


@click.command(help="Get information about the qq job.")
def info():
    """
    Get information about the qq job submitted from this directory.
    """
    try:
        info_file = get_info_file(Path())
        informer = QQInformerOLD.loadFromFile(info_file)
        console = Console()
        panel = informer.getJobStatusPanel(console)
        console.print(panel)
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        sys.exit(91)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(99)


class QQInformerOLD:
    """
    Handles collecting, loading and printing information
    about the qq run.
    """

    def __init__(
        self, batch_system: type[QQBatchInterface], info: dict[str, Any] | None = None
    ):
        self.batch_system = batch_system
        self.batch_info = None

        if info:
            self.info = info
        else:
            self.info = {}

    def setSubmitted(
        self,
        job_name: str,
        job_type: str,
        input_machine: str,
        job_dir: Path,
        resources: QQResources,
        excluded_files: list[Path] | None,
        time: datetime,
        jobid: str,
        stdout_file: str,
        stderr_file: str,
    ):
        self.info["batch_system"] = self.batch_system.envName()
        self.info["qq_version"] = qq_lib.__version__
        self.info["job_name"] = job_name
        self.info["job_type"] = job_type
        self.info["input_machine"] = input_machine
        self.info["job_dir"] = str(job_dir)
        self.info["excluded_files"] = (
            [str(x) for x in excluded_files] if excluded_files else None
        )

        self.info["job_state"] = str(NaiveState.QUEUED)
        self.info["submission_time"] = time.strftime(DATE_FORMAT)
        self.info["job_id"] = jobid
        self.info["stdout_file"] = stdout_file
        self.info["stderr_file"] = stderr_file
        self.info["resources"] = resources.toDict()

    def setRunning(self, time: datetime, main_node: str, work_dir: Path):
        self.info["job_state"] = str(NaiveState.RUNNING)
        self.info["start_time"] = time.strftime(DATE_FORMAT)
        self.info["main_node"] = main_node
        self.info["work_dir"] = str(work_dir)

    def setFinished(self, time: datetime):
        self.info["job_state"] = str(NaiveState.FINISHED)
        self.info["completion_time"] = time.strftime(DATE_FORMAT)
        self.info["job_exit_code"] = 0

    def setFailed(self, time: datetime, return_code: int):
        self.info["job_state"] = str(NaiveState.FAILED)
        self.info["completion_time"] = time.strftime(DATE_FORMAT)
        self.info["job_exit_code"] = return_code

    def setKilled(self, time: datetime):
        self.info["job_state"] = str(NaiveState.KILLED)
        self.info["completion_time"] = time.strftime(DATE_FORMAT)

    def exportToConsole(self):
        print("\nqq job info\n")
        print(self._convertToYaml())
        print()

    def exportToFile(self, file: Path):
        logger.debug(f"Exporting qq info into '{file}'.")
        with Path.open(file, "w") as output:
            output.write("# qq job info file\n")
            output.write(self._convertToYaml())
            output.write("\n")

    def getJobStatusPanel(self, console: Console | None = None) -> Group:
        (message, details, state) = self._getStateMessages()

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
            title=Text(f"JOB: {self.getJobId()}", style="bold", justify="center"),
            border_style="white",
            padding=(1, 2),
            width=panel_width,
        )

        return Group(Text(""), panel, Text(""))

    @classmethod
    def loadFromFile(cls, file: Path) -> Self:
        logger.debug(f"Loading qq info from '{file}'.")
        with Path.open(file) as input:
            info: dict = yaml.safe_load(input)

        if not isinstance(info, dict):
            raise QQError(f"Could not read the qqinfo file '{file}'.")

        # get the batch system
        try:
            batch_system_string = info["batch_system"]
        except KeyError:
            raise QQError(
                "Undefined batch system: 'batch_system' missing from qqinfo file."
            )

        try:
            batch_system = convert_to_batch_system(batch_system_string)
        except KeyError:
            raise QQError(
                f"Unknown batch system: '{batch_system_string}' does not match any known batch system."
            )

        return cls(batch_system, info)

    def getResources(self) -> dict[str, Any]:
        res = self.info.get("resources")

        if not res:
            raise QQError("Property 'resources' not available in qq info.")

        return res

    def getDestination(self) -> tuple[str, str] | None:
        """
        Returns the main node and working directory for the job.
        If not set up, returns None.
        """
        main_node = self.info.get("main_node")
        work_dir = self.info.get("work_dir")

        if not main_node or not work_dir:
            return None

        return (main_node, work_dir)

    def getJobId(self) -> str:
        jobid = self.info.get("job_id")

        if not jobid:
            raise QQError("Property 'job_id' not available in qq info.")

        return jobid

    def getJobDir(self) -> str:
        job_dir = self.info.get("job_dir")

        if not job_dir:
            raise QQError("Property 'job_dir' not available in qq info.")

        return job_dir

    def getStdout(self) -> str:
        stdout_file = self.info.get("stdout_file")

        if not stdout_file:
            raise QQError("Property 'stdout_file' not available in qq info.")

        return stdout_file

    def getStderr(self) -> str:
        stderr_file = self.info.get("stderr_file")

        if not stderr_file:
            raise QQError("Property 'stderr_file' not available in qq info.")

        return stderr_file

    def getJobName(self) -> str:
        job_name = self.info.get("job_name")

        if not job_name:
            raise QQError("Property 'job_name' not available in qq info.")

        return job_name

    def useScratch(self) -> bool:
        try:
            return self.getResources()["work_dir"] is not None
        except Exception:
            raise QQError("Could not get working directory from the info file.")

    def getNaiveState(self) -> NaiveState:
        """
        Get the naive state of the job as defined in the qqinfo file.
        """
        state = self.info.get("job_state")
        if not state:
            return NaiveState.UNKNOWN

        return NaiveState.fromStr(state)

    def getBatchState(self) -> BatchState:
        """
        Get the state of the job according to the batch system.
        """
        if not self.batch_info:
            self.batch_info = self.batch_system.getJobInfo(self.getJobId())

        state_code = self.batch_info.get(self.batch_system.jobState())
        logger.debug(f"Batch state code: '{state_code}'")
        if not state_code:
            return BatchState.UNKNOWN

        return self.batch_system.translateJobState(state_code)

    def getRealState(self) -> QQState:
        """
        Get the real state of the job as understood from the qqinfo and the batch system.
        """
        naive = self.getNaiveState()
        batch = self.getBatchState()
        return QQState.fromStates(naive, batch)

    def _convertToYaml(self) -> str:
        cleaned = {k: v for k, v in self.info.items() if v is not None}

        return yaml.dump(cleaned, default_flow_style=False, sort_keys=False)

    def _getStateMessages(self) -> tuple[str, str, QQState]:
        submission_time = self.info.get("submission_time")
        if not submission_time:
            raise QQError(
                f"Submission time '{submission_time}' is not available in qq info."
            )
        start_time = self.info.get("start_time")
        completion_time = self.info.get("completion_time")

        if start_time:
            start = datetime.strptime(start_time, DATE_FORMAT)
            if completion_time:
                end = datetime.strptime(completion_time, DATE_FORMAT)
            else:
                end = datetime.now()
        else:
            start = datetime.strptime(submission_time, DATE_FORMAT)
            if completion_time:
                end = datetime.strptime(completion_time, DATE_FORMAT)
            else:
                end = datetime.now()

        real_state = self.getRealState()
        (message, details) = real_state.info(
            start, end, self.info.get("job_exit_code"), self.info.get("main_node")
        )
        return (message, details, real_state)


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
    submission_time: str

    # Name of the file for storing standard output of the executed script
    stdout_file: str

    # Name of the file for storing error output of the executed script
    stderr_file: str

    # Resources allocated to the job
    resources: QQResources

    # List of files to not copy to the working directory
    excluded_files: list[Path] | None = None

    # Job start time
    start_time: str | None = None

    # Main node assigned to the job
    main_node: str | None = None

    # Working directory
    work_dir: Path | None = None

    # Job completion time
    completion_time: str | None = None

    # Exit code of qq run
    job_exit_code: int | None = None

    @classmethod
    def fromFile(cls, file: Path) -> Self:
        """
        Load a QQInfo instance from a YAML file.

        This method always returns a valid QQInfo
        or raises an Exception.

        Args:
            file: Path to the YAML qq info file.

        Returns:
            QQInfo instance constructed from the file.

        Raises:
            QQError: If the file does not exist, cannot be parsed
            or does not contain all mandatory information.
        """
        logger.debug(f"Loading qq info from '{file}'.")

        if not file.exists():
            raise QQError(f"qq info file '{file}' does not exist.")

        try:
            with file.open("r") as input:
                data: dict[str, object] = yaml.safe_load(input)
                return cls._fromDict(data)
        except yaml.YAMLError as e:
            raise QQError(f"Could not parse the qq info file '{file}': {e}.") from e
        except TypeError as e:
            raise QQError(
                f"Mandatory information missing from the qq info file '{file}': {e}."
            ) from e

    def toFile(self, file: Path):
        """
        Export this QQInfo instance to a YAML file.

        Args:
            file: Path to write the YAML file.

        Raises:
            QQError: If the file cannot be created or written to.
        """
        logger.debug(f"Exporting qq info into '{file}'.")

        try:
            with file.open("w") as output:
                output.write("# qq job info file\n")
                output.write(self._toYaml())
                output.write("\n")
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
            if hasattr(value, "_toDict") and callable(value._toDict):
                result[f.name] = value._toDict()
            # convert the state and the batch system
            elif f.type == NaiveState or f.type == type[QQBatchInterface]:
                result[f.name] = str(value)
            # convert paths (incl. optional paths)
            elif f.type == Path or f.type == Path | None:
                result[f.name] = str(value)
            # convert list of excluded files
            elif isinstance(value, list):
                result[f.name] = [str(x) if hasattr(x, "__str__") else x for x in value]
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
            if name == "resources" and isinstance(value, dict):
                init_kwargs[name] = QQResources(**value)
            # convert the batch system
            elif f.type == type[QQBatchInterface]:
                init_kwargs[name] = QQBatchMeta.fromStr(value)
            # convert the job state
            elif f.type == NaiveState:
                init_kwargs[name] = (
                    NaiveState.fromStr(value) if value else NaiveState.UNKNOWN
                )
            # convert paths (incl. optional paths)
            elif f.type == Path or f.type == Path | None:
                init_kwargs[name] = Path(value)
            # convert the list of excluded paths
            elif f.type == list[Path] | None:
                init_kwargs[name] = [
                    Path(v) if isinstance(v, str) else v for v in value
                ]
            else:
                init_kwargs[name] = value

        return cls(**init_kwargs)

class QQInformer:
    def __init__(self, info: QQInfo):
        self.info = info
        self._batch_info = None

    @property
    def batch_system(self) -> type[QQBatchInterface]:
        return self.info.batch_system

    @classmethod
    def fromFile(cls, file: Path) -> Self:
        return cls(QQInfo.fromFile(file))
    
    def toFile(self, file: Path):
        self.info.toFile(file)
    
    def setRunning(self, time: datetime, main_node: str, work_dir: Path):
        self.info.job_state = NaiveState.RUNNING
        self.info.start_time = time.strftime(DATE_FORMAT)
        self.info.main_node = main_node
        self.info.work_dir = work_dir

    def setFinished(self, time: datetime):
        self.info.job_state = NaiveState.FINISHED
        self.info.completion_time = time.strftime(DATE_FORMAT)
        self.info.job_exit_code = 0

    def setFailed(self, time: datetime, exit_code: int):
        self.info.job_state = NaiveState.FAILED
        self.info.completion_time = time.strftime(DATE_FORMAT)
        self.info.job_exit_code = exit_code

    def setKilled(self, time: datetime):
        self.info.job_state = NaiveState.KILLED
        self.info.completion_time = time.strftime(DATE_FORMAT)

    def useScratch(self) -> bool:
        """
        Determine if the job uses a scratch directory.

        Returns:
            True if a work_dir is defined in the resources, False otherwise.
        """
        return self.info.resources.work_dir is not None

    def getDestination(self) -> tuple[str, str] | None:
        """
        Retrieve the job's main node and working directory.

        Returns:
            A tuple of (main_node, work_dir) if both are set, otherwise None.
        """
        if all((self.info.main_node, self.info.work_dir)):
            return self.info.main_node, self.info.work_dir
        return None