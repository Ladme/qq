# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import re
from dataclasses import asdict, dataclass, fields
from typing import Self

from qq_lib.core.common import equals_normalized, wdhms_to_hhmmss
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger

from .size import Size

logger = get_logger(__name__)


@dataclass(init=False)
class QQResources:
    """
    Dataclass representing computational resources requested for a qq job.
    """

    # Number of computing nodes to use
    nnodes: int | None = None

    # Number of CPU cores to use for the job
    ncpus: int | None = None

    # Absolute amount of memory to allocate for the job (overrides mem_per_cpu)
    mem: Size | None = None

    # Amount of memory to allocate per CPU core
    mem_per_cpu: Size | None = None

    # Number of GPUs to use
    ngpus: int | None = None

    # Maximum allowed runtime for the job
    walltime: str | None = None

    # Type of working directory to use (e.g., scratch_local, scratch_shared, job_dir)
    work_dir: str | None = None

    # Absolute size of storage requested for the job (overrides work_size_per_cpu)
    work_size: Size | None = None

    # Storage size requested per CPU core
    work_size_per_cpu: Size | None = None

    # Dictionary of other properties the nodes must include or exclude
    props: dict[str, str] | None = None

    def __init__(
        self,
        nnodes: int | str | None = None,
        ncpus: int | str | None = None,
        mem: Size | str | dict[str, object] | None = None,
        mem_per_cpu: Size | str | dict[str, object] | None = None,
        ngpus: int | str | None = None,
        walltime: str | None = None,
        work_dir: str | None = None,
        work_size: Size | str | dict[str, object] | None = None,
        work_size_per_cpu: Size | str | dict[str, object] | None = None,
        props: dict[str, str] | str | None = None,
    ):
        # convert sizes
        mem = QQResources._parseSize(mem)
        mem_per_cpu = QQResources._parseSize(mem_per_cpu)
        work_size = QQResources._parseSize(work_size)
        work_size_per_cpu = QQResources._parseSize(work_size_per_cpu)

        # convert walltime
        if isinstance(walltime, str) and ":" not in walltime:
            walltime = wdhms_to_hhmmss(walltime)

        # convert properties to dictionary
        if isinstance(props, str):
            props = QQResources._parseProps(props)

        # convert nnodes, ncpus, and ngpus to integer
        if isinstance(nnodes, str):
            nnodes = int(nnodes)
        if isinstance(ncpus, str):
            ncpus = int(ncpus)
        if isinstance(ngpus, str):
            ngpus = int(ngpus)

        # set attributes
        self.nnodes = nnodes
        self.ncpus = ncpus
        self.mem = mem
        self.mem_per_cpu = mem_per_cpu
        self.ngpus = ngpus
        self.walltime = walltime
        self.work_dir = work_dir
        self.work_size = work_size
        self.work_size_per_cpu = work_size_per_cpu
        self.props = props

        logger.debug(f"QQResources: {self}")

    def toDict(self) -> dict[str, object]:
        """Return all fields as a dict, excluding fields set to None."""
        return {k: v for k, v in asdict(self).items() if v is not None}

    def useScratch(self) -> bool:
        """
        Determine if the job uses a scratch directory.

        Returns:
            bool: True if a work_dir is not 'job-dir', otherwise False.
        """
        return not equals_normalized(self.work_dir, "jobdir")

    @staticmethod
    def mergeResources(*resources: "QQResources") -> "QQResources":
        """
        Merge multiple QQResources objects.

        Earlier resources take precedence over later ones. Properties are merged.

        Args:
            *resources (QQResources): One or more QQResources objects, in order of precedence.

        Returns:
            QQResources: A new QQResources object with merged fields.
        """
        merged_data = {}
        for f in fields(QQResources):
            if f.name == "props":
                # merge all props dictionaries; first occurence of each key wins
                merged_props: dict[str, str] = {}
                for r in resources:
                    if r.props:
                        # only add keys that have not been added yet
                        merged_props |= {
                            k: v for k, v in r.props.items() if k not in merged_props
                        }
                merged_data[f.name] = merged_props or None
                continue

            # only set mem and work_size if mem_per_cpu / work_size_per_cpu has not already been set
            if f.name in ("mem", "work_size"):
                merged_data[f.name] = QQResources._firstNonblocked(
                    resources,
                    f.name,
                    "mem_per_cpu" if f.name == "mem" else "work_size_per_cpu",
                )
                continue

            # default: ick the first non-None value for this field
            merged_data[f.name] = next(
                (
                    getattr(r, f.name)
                    for r in resources
                    if getattr(r, f.name) is not None
                ),
                None,
            )

        return QQResources(**merged_data)

    @staticmethod
    def _firstNonblocked(
        resources: tuple[Self, ...], field: str, block_field: str
    ) -> object | None:
        """
        Return the first value of `field` from `resources` where `block_field` has not appeared yet.

        Args:
            resources (tuple[QQResources, ...]): Resources in order of precedence.
            field (str): Name of the field to pick.
            block_field (str): Name of the per-CPU field that blocks later values.

        Returns:
            Optional[object]: The first valid value, or None if none found.
        """
        blocked = False
        for r in resources:
            if getattr(r, block_field) is not None:
                blocked = True
            val = getattr(r, field)
            if val is not None and not blocked:
                return val
        return None

    @staticmethod
    def _parseSize(value: object) -> Size | None:
        """
        Convert a raw value into a `Size` instance if possible.

        Args:
            value (object): A Size object or a raw size value (a string or a dictionary).

        Returns:
            Size | None: A `Size` object if the input could be parsed,
            otherwise `None`.
        """
        if isinstance(value, str):
            return Size.from_string(value)
        if isinstance(value, dict):
            return Size(**value)  # ty: ignore[missing-argument]
        if isinstance(value, Size):
            return value
        return None

    @staticmethod
    def _parseProps(props: str) -> dict[str, str]:
        """
        Parse a properties string into a dictionary of key/value pairs.

        The input may contain multiple properties separated by commas,
        whitespace, or colons. Each property can be one of the following forms:
        - "key=value" - stored as {"key": "value"}
        - "key"       - stored as {"key": "true"}
        - "^key"      - stored as {"key": "false"}

        Args:
            props (str): A string containing job properties.

        Returns:
            dict[str, str]: Parsed properties as key/value pairs.

        Raises:
            QQError: If a property key is defined multiple times.
        """
        # split into parts by commas, whitespace, or colons
        parts = filter(None, re.split(r"[,\s:]+", props))

        result = {}
        for part in parts:
            if "=" in part:
                # explicit key=value pair
                key, value = part.split("=", 1)
            elif part.startswith("^"):
                # ^key means false
                key, value = part.lstrip("^"), "false"
            else:
                # bare key means true
                key, value = part, "true"

            if key in result:
                raise QQError(f"Property '{key}' is defined multiple times.")
            result[key] = value

        return result
