# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os

from qq_lib.env_vars import GUARD
from qq_lib.error import QQError


def guard():
    """
    Raises an exception if the script is not running inside qq environment.
    """
    if not _check_qq_env():
        raise QQError(
            "This script must be run as a qq job within the batch system. "
            "To submit it properly, use: qq submit."
        )


def _check_qq_env() -> bool:
    """
    Returns True if the `GUARD_ENV_VAR` environment variable is set.
    """

    return os.environ.get(GUARD) is not None
