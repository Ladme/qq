# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from dataclasses import dataclass, field, fields
from datetime import datetime
from pathlib import Path
from typing import Self

import yaml

from qq_lib.batch.interface import BatchInterface, BatchMeta
from qq_lib.core.common import load_yaml_dumper, load_yaml_loader
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.depend import Depend

from .job_type import JobType
from .loop import LoopInfo
from .resources import QQResources
from .states import NaiveState

logger = get_logger(__name__)

SafeLoader: type[yaml.SafeLoader] = load_yaml_loader()
Dumper: type[yaml.Dumper] = load_yaml_dumper()


@dataclass
class Info:
    """
    Dataclass storing information about a qq job.

    Exposes only minimal functionality for loading, exporting, and basic access.
    More complex operations, such as transforming or combining the data
    should be implemented in Informer.
    """

    # The batch system class used
    batch_system: type[BatchInterface]

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

    # Queue the job was submitted to
    queue: str

    # Type of the qq job
    job_type: JobType

    # Host from which the job was submitted
    input_machine: str

    # Directory from which the job was submitted
    input_dir: Path

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

    # Command line arguments and options provided when submitting.
    command_line: list[str]

    # List of files to not copy to the working directory
    excluded_files: list[Path] = field(default_factory=list)

    # List of dependencies.
    depend: list[Depend] = field(default_factory=list)

    # Loop job-associated information.
    loop_info: LoopInfo | None = None

    # Account associated with the job
    account: str | None = None

    # Job start time
    start_time: datetime | None = None

    # Main node assigned to the job
    main_node: str | None = None

    # All nodes assigned to the job
    all_nodes: list[str] | None = None

    # Working directory
    work_dir: Path | None = None

    # Job completion time
    completion_time: datetime | None = None

    # Exit code of qq run
    job_exit_code: int | None = None

    @classmethod
    def fromFile(cls, file: Path, host: str | None = None) -> Self:
        """
        Load an Info instance from a YAML file, either locally or on a remote host.

        If `host` is provided, the file will be read from the remote host using
        the batch system's `readRemoteFile` method. Otherwise, the file is read locally.

        Args:
            file (Path): Path to the YAML qq info file.
            host (str | None): Optional hostname of the remote machine where the file resides.
                If None, the file is assumed to be local.

        Returns:
            Info: Instance constructed from the file.

        Raises:
            QQError: If the file does not exist, cannot be reached, cannot be parsed,
                    or does not contain all mandatory information.
        """
        try:
            if host:
                # remote file
                logger.debug(f"Loading qq info from '{file}' on '{host}'.")

                BatchSystem = BatchMeta.fromEnvVarOrGuess()
                data: dict[str, object] = yaml.load(
                    BatchSystem.readRemoteFile(host, file),
                    Loader=SafeLoader,
                )
            else:
                # local file
                logger.debug(f"Loading qq info from '{file}'.")

                if not file.exists():
                    raise QQError(f"qq info file '{file}' does not exist.")

                with file.open("r") as input:
                    data: dict[str, object] = yaml.load(input, Loader=SafeLoader)

            return cls._fromDict(data)
        except yaml.YAMLError as e:
            raise QQError(f"Could not parse the qq info file '{file}': {e}.") from e
        except TypeError as e:
            raise QQError(
                f"Mandatory information missing from the qq info file '{file}': {e}."
            ) from e

    def toFile(self, file: Path, host: str | None = None) -> None:
        """
        Export this Info instance to a YAML file, either locally or on a remote host.

        If `host` is provided, the file will be written to the remote host using
        the batch system's `writeRemoteFile` method. Otherwise, the file is written locally.

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
        Serialize the Info instance to a YAML string.

        Returns:
            str: YAML representation of the Info object.
        """
        return yaml.dump(
            self._toDict(), default_flow_style=False, sort_keys=False, Dumper=Dumper
        )

    def _toDict(self) -> dict[str, object]:
        """
        Convert the Info instance into a dictionary of string-object pairs.
        Fields that are None are ignored.

        Returns:
            dict[str, object]: Dictionary containing all fields with non-None values,
            converting enums and nested objects appropriately.
        """
        result: dict[str, object] = {}

        for f in fields(self):
            value = getattr(self, f.name)
            # ignore None fields
            if value is None:
                continue

            # empty lists are ignored
            if isinstance(value, list) and not value:
                continue

            # convert job type
            if f.type == JobType:
                result[f.name] = str(value)
            # convert resources
            elif f.type == QQResources or f.type == LoopInfo | None:
                result[f.name] = value.toDict()
            # convert the state and the batch system
            elif (
                f.type == NaiveState
                or f.type == type[BatchInterface]
                or f.type == Path
                or f.type == Path | None
            ):
                result[f.name] = str(value)
            # convert list of excluded files
            elif f.type == list[Path]:
                result[f.name] = [str(x) for x in value]
            elif f.type == list[Depend]:
                result[f.name] = [Depend.toStr(x) for x in value]
            # convert timestamp
            elif f.type == datetime or f.type == datetime | None:
                result[f.name] = value.strftime(CFG.date_formats.standard)
            else:
                result[f.name] = value

        return result

    @classmethod
    def _fromDict(cls, data: dict[str, object]) -> Self:
        """
        Construct an Info instance from a dictionary.

        Args:
            data: Dictionary containing field names and values.

        Returns:
            Info: An Info instance.

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

            # convert job type
            if f.type == JobType and isinstance(value, str):
                init_kwargs[name] = JobType.fromStr(value)
            # convert optional loop job info
            elif f.type == LoopInfo | None and isinstance(value, dict):
                # 'archive' must be converted to Path
                init_kwargs[name] = LoopInfo(  # ty: ignore[missing-argument]
                    **{k: Path(v) if k == "archive" else v for k, v in value.items()}
                )
            # convert resources
            elif f.type == QQResources:
                init_kwargs[name] = QQResources(**value)  # ty: ignore[invalid-argument-type]
            # convert the batch system
            elif f.type == type[BatchInterface] and isinstance(value, str):
                init_kwargs[name] = BatchMeta.fromStr(value)
            # convert the job state
            elif f.type == NaiveState and isinstance(value, str):
                init_kwargs[name] = (
                    NaiveState.fromStr(value) if value else NaiveState.UNKNOWN
                )
            # convert paths (incl. optional paths)
            elif f.type == Path or f.type == Path | None:
                init_kwargs[name] = Path(value)
            # convert the list of excluded paths
            elif f.type == list[Path] and isinstance(value, list):
                init_kwargs[name] = [
                    Path(v) if isinstance(v, str) else v for v in value
                ]
            # convert dependencies
            elif f.type == list[Depend] and isinstance(value, list):
                init_kwargs[name] = [Depend.fromStr(x) for x in value]  # ty: ignore[invalid-argument-type]
            # convert timestamp
            elif (f.type == datetime or f.type == datetime | None) and isinstance(
                value, str
            ):
                init_kwargs[name] = datetime.strptime(value, CFG.date_formats.standard)
            else:
                init_kwargs[name] = value

        return cls(**init_kwargs)
