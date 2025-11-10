# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from dataclasses import fields

from qq_lib.core.common import dhhmmss_to_duration, format_duration_wdhhmmss
from qq_lib.core.logger import get_logger
from qq_lib.properties.resources import Resources

logger = get_logger(__name__)

# field requested in sacct
SACCT_FIELDS = "JobID,Account,State,User,JobName,Partition,WorkDir,AllocCPUs,ReqCPUs,AllocTRES,ReqTRES,AllocNodes,ReqNodes,Submit,Start,End,TimeLimit,NodeList,Reason,ExitCode"


def parse_slurm_dump_to_dictionary(
    text: str, separator: str | None = None
) -> dict[str, str]:
    """
    Parse a Slurm info dump into a dictionary.

    Returns:
        dict[str, str]: Dictionary mapping keys to values.
    """
    result: dict[str, str] = {}

    for pair in text.split(separator):
        if "=" not in pair:
            continue

        key, value = pair.split("=", 1)
        result[key.strip()] = value.strip()

    logger.debug(f"Parsed slurm dump: {result}.")
    return result


def default_resources_from_dict(res: dict[str, str]) -> Resources:
    """
    Extract and convert default resource settings from a parsed Slurm info dump.

    Args:
        res (dict[str, str]): A dictionary containing default resource values
            parsed from Slurm info dump.

    Returns:
        Resources: An instance representing the default resource settings.
    """

    converter = {
        "DefMemPerCPU": "mem_per_cpu",
        "DefaultTime": "walltime",
    }
    logger.debug(f"Raw dictionary for default resources: {res}.")

    # only select fields that are part of Resources
    field_names = {f.name for f in fields(Resources)}

    converted_resources = {}
    for key, value in res.items():
        if value == "UNLIMITED":
            continue

        converted_key = converter.get(key, key)
        if converted_key in field_names:
            if converted_key in {"mem_per_cpu", "mem"} and value.isnumeric():
                # default unit for Slurm sizes is MB
                value += "mb"
            if converted_key == "walltime":
                # convert to duration format understandable to qq
                value = format_duration_wdhhmmss(dhhmmss_to_duration(value))
            converted_resources[converted_key] = value

    return Resources(**converted_resources)
