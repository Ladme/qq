# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

"""
Structured representation of job resource requirements.

This module defines the `Resources` dataclass, which captures all CPU, GPU,
memory, storage, walltime, and property requirements associated with a qq job.
"""

import re
from dataclasses import asdict, dataclass, fields

from qq_lib.core.common import equals_normalized, wdhms_to_hhmmss
from qq_lib.core.error import QQError
from qq_lib.core.field_coupling import FieldCoupling, HasCouplingMethods, coupled_fields
from qq_lib.core.logger import get_logger

from .size import Size

logger = get_logger(__name__)


# dataclass decorator has to come before `@coupled_fields`!
@dataclass(init=False)
@coupled_fields(
    # if mem is set, ignore other mem properties; if mem_per_node is set, ignore mem_per_cpu
    FieldCoupling("mem", "mem_per_node", "mem_per_cpu"),
    # if work_size is set, ignore other work_size properties; if work_size_per_node is set, ignore work_size_per_cpu
    FieldCoupling("work_size", "work_size_per_node", "work_size_per_cpu"),
    # if ncpus is set, ignore ncpus_per_node
    FieldCoupling("ncpus", "ncpus_per_node"),
    # if ngpus is set, ignore ngpus_per_node
    FieldCoupling("ngpus", "ngpus_per_node"),
)
class Resources(HasCouplingMethods):
    """
    Dataclass representing computational resources requested for a qq job.
    """

    # Number of computing nodes to use
    nnodes: int | None = None

    # Number of CPU cores to use for the job
    ncpus: int | None = None

    # Number of CPU cores to use per node
    ncpus_per_node: int | None = None

    # Absolute amount of memory to allocate for the job (overrides mem_per_cpu)
    mem: Size | None = None

    # Amount of memory to allocate per node
    mem_per_node: Size | None = None

    # Amount of memory to allocate per CPU core
    mem_per_cpu: Size | None = None

    # Number of GPUs to use
    ngpus: int | None = None

    # Number of GPUs to use per node
    ngpus_per_node: int | None = None

    # Maximum allowed runtime for the job
    walltime: str | None = None

    # Type of working directory to use (e.g., scratch_local, scratch_shared, input_dir)
    work_dir: str | None = None

    # Absolute size of storage requested for the job (overrides work_size_per_cpu)
    work_size: Size | None = None

    # Storage size requested per node
    work_size_per_node: Size | None = None

    # Storage size requested per CPU core
    work_size_per_cpu: Size | None = None

    # Dictionary of other properties the nodes must include or exclude
    props: dict[str, str] | None = None

    def __init__(
        self,
        nnodes: int | str | None = None,
        ncpus: int | str | None = None,
        ncpus_per_node: int | str | None = None,
        mem: Size | str | dict[str, object] | None = None,
        mem_per_node: Size | str | dict[str, object] | None = None,
        mem_per_cpu: Size | str | dict[str, object] | None = None,
        ngpus: int | str | None = None,
        ngpus_per_node: int | str | None = None,
        walltime: str | None = None,
        work_dir: str | None = None,
        work_size: Size | str | dict[str, object] | None = None,
        work_size_per_node: Size | str | dict[str, object] | None = None,
        work_size_per_cpu: Size | str | dict[str, object] | None = None,
        props: dict[str, str] | str | None = None,
    ):
        # convert sizes
        mem = Resources._parseSize(mem)
        mem_per_node = Resources._parseSize(mem_per_node)
        mem_per_cpu = Resources._parseSize(mem_per_cpu)
        work_size = Resources._parseSize(work_size)
        work_size_per_node = Resources._parseSize(work_size_per_node)
        work_size_per_cpu = Resources._parseSize(work_size_per_cpu)

        # convert walltime
        if isinstance(walltime, str) and ":" not in walltime:
            walltime = wdhms_to_hhmmss(walltime)

        # convert properties to dictionary
        if isinstance(props, str):
            props = Resources._parseProps(props)

        # convert nnodes, ncpus, and ngpus to integers
        if isinstance(nnodes, str):
            nnodes = int(nnodes)
        if isinstance(ncpus, str):
            ncpus = int(ncpus)
        if isinstance(ncpus_per_node, str):
            ncpus_per_node = int(ncpus_per_node)
        if isinstance(ngpus, str):
            ngpus = int(ngpus)
        if isinstance(ngpus_per_node, str):
            ngpus_per_node = int(ngpus_per_node)

        # set attributes
        self.nnodes = nnodes
        self.ncpus = ncpus
        self.ncpus_per_node = ncpus_per_node
        self.mem = mem
        self.mem_per_node = mem_per_node
        self.mem_per_cpu = mem_per_cpu
        self.ngpus = ngpus
        self.ngpus_per_node = ngpus_per_node
        self.walltime = walltime
        self.work_dir = work_dir
        self.work_size = work_size
        self.work_size_per_node = work_size_per_node
        self.work_size_per_cpu = work_size_per_cpu
        self.props = props

        # enforce coupling rules
        self.__post_init__()  # ty: ignore[unresolved-attribute]

        logger.debug(f"Resources: {self}")

    def toDict(self) -> dict[str, object]:
        """Return all fields as a dict, excluding fields set to None."""
        return {k: v for k, v in asdict(self).items() if v is not None}

    def usesScratch(self) -> bool:
        """
        Determine if the job uses a scratch directory.

        Returns:
            bool: True if a work_dir is not 'job_dir' or 'input_dir', otherwise False.
        """
        return not equals_normalized(
            str(self.work_dir), "job_dir"
        ) and not equals_normalized(str(self.work_dir), "input_dir")

    @staticmethod
    def mergeResources(*resources: "Resources") -> "Resources":
        """
        Merge multiple Resources objects.

        Earlier resources take precedence over later ones. Properties are merged.

        If either field in a coupling is set in an earlier resource, both fields of
        that coupling are taken from that resource and ignore later resources.
        (This means that if e.g. a `mem-per-cpu` is set by the user,
        it will not be overwritten by a default absolute `mem` value set by a queue,
        even though `mem` is a dominant attribute and `mem-per-cpu` is recessive.)

        Args:
            *resources (Resources): One or more Resources objects, in order of precedence.

        Returns:
            Resources: A new Resources object with merged fields.
        """
        merged_data = {}
        processed_couplings: set[FieldCoupling] = set()

        for f in fields(Resources):
            # handle props
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

            # check if this field is part of a coupling
            if coupling := Resources.getCouplingForField(f.name):
                # skip if coupling already processed
                if coupling in processed_couplings:
                    continue
                processed_couplings.add(coupling)

                # find first resource where either field in the coupling is set
                source_resource = next(
                    (r for r in resources if coupling.hasValue(r)), None
                )

                # set all fields of the coupling
                if source_resource:
                    for field in coupling.fields:
                        merged_data[field] = getattr(source_resource, field)
                # if no resource has any attribute set for this coupling
                else:
                    for field in coupling.fields:
                        merged_data[field] = None
                continue

            # default: pick the first non-None value for this field
            merged_data[f.name] = next(
                (
                    getattr(r, f.name)
                    for r in resources
                    if getattr(r, f.name) is not None
                ),
                None,
            )

        return Resources(**merged_data)

    def toCommandLine(self) -> list[str]:
        """
        Convert resource settings into a command-line argument list for `qq submit`.

        Returns:
            list[str]: A list of command-line arguments ready to pass to ``qq submit``.
        """
        command_line: list[str] = []
        for f in fields(Resources):
            field_name = f.name.replace("_", "-")
            value = getattr(self, f.name)
            if value is None:
                continue

            if isinstance(value, Size):
                command_line.extend([f"--{field_name}", value.toStrExact()])
            elif isinstance(value, int):
                command_line.extend([f"--{field_name}", str(value)])
            elif isinstance(value, dict):
                if value := self._propsToValue():
                    command_line.extend([f"--{field_name}", value])
            elif isinstance(value, str):
                command_line.extend([f"--{field_name}", value])
            else:
                raise QQError(
                    f"Unknown value type detected: {field_name}={value} of type {type(value)} when converting Resources to command line options. This is a bug, please report this."
                )

        return command_line

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
            return Size.fromString(value)
        if isinstance(value, dict):
            return Size(**value)  # ty: ignore[invalid-argument-type]
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

    def _propsToValue(self) -> str | None:
        """
        Convert a properties dictionary into a command-line raw value string.

        Args:
            props (dict[str, str]): Mapping of property names to their string values.

        Returns:
            str | None: A comma-separated command-line representation of the property definitions
            or None if the dictionary is empty.
        """
        if not self.props:
            return None

        properties = []
        for key, value in self.props.items():
            if value == "true":
                properties.append(key)
            elif value == "false":
                properties.append(f"^{key}")
            else:
                properties.append(f"{key}={value}")

        return ",".join(properties)
