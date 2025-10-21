# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import re

from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger

logger = get_logger(__name__)


def parsePBSDumpToDictionary(text: str) -> dict[str, str]:
    """
    Parse a PBS info dump into a dictionary.

    Returns:
        dict[str, str]: Dictionary mapping keys to values.
    """
    result: dict[str, str] = {}

    for raw_line in text.splitlines():
        line = raw_line.rstrip()

        if " = " not in line:
            continue

        key, value = line.split(" = ", 1)
        result[key.strip()] = value.strip()

    return result


def parseMultiPBSDumpToDictionaries(
    text: str, keyword: str | None
) -> list[tuple[dict[str, str], str]]:
    """
    Parse a PBS dump containing metadata for multiple queues/jobs/nodes into structured dictionaries.

    Args:
        text (str): The raw PBS dump containing information about one or more queues/jobs/nodes.
        keyword (str): Keyword identifying the start of a metadata block.

    Returns:
        list[tuple[dict[str, str], str]]: A list of tuples, each containing:
            - dict[str, str]: Parsed metadata for a single PBS object (job/queue/node).
            - str: Identifier (job ID / queue name / node name) extracted from the metadata.

    Raises:
        QQError: If the identifier cannot be extracted.
    """
    if not text.strip():
        return []

    data = []
    block, identifier = [], None
    pattern = re.compile(rf"^\s*{keyword}:\s*(.*)$") if keyword else None

    for line in text.splitlines():
        # if the line is empty, start a new block
        if not line.strip():
            if block:
                data.append((parsePBSDumpToDictionary("\n".join(block)), identifier))
                block, identifier = [], None
            continue

        if not block:
            # extract the identifier
            if keyword:
                m = pattern.match(line)
                if not m:
                    raise QQError(
                        f"Invalid PBS dump format. Could not extract identifier from:\n{line}"
                    )
                identifier = m.group(1).strip()
            # if keyword is not specified, used the first line as the identifier
            else:
                identifier = line.strip()
        block.append(line)

    # last block (no trailing newline)
    if block:
        data.append((parsePBSDumpToDictionary("\n".join(block)), identifier))

    logger.debug(f"Detected and parsed metadata for {len(data)} PBS objects.")
    return data
