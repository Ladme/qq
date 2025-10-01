# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os

from qq_lib.constants import GUARD
from qq_lib.error import QQError


def guard():
    """
    Raises an exception if the script is not running inside qq environment.
    """
    if not _check_qq_env():
        raise QQError(
            "This script must be run as a qq job within the batch system. "
            "To submit it properly, use: 'qq submit'."
        )


def guard_command(command: str):
    """
    Raises an exception if a qq command is not used inside qq environment.
    """
    if not _check_qq_env():
        raise QQError(f"'qq {command}' can only be used within a running qq job.")


def _check_qq_env() -> bool:
    """
    Returns True if the `GUARD_ENV_VAR` environment variable is set.
    """

    return os.environ.get(GUARD) is not None
