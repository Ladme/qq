# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import re
import sys
from pathlib import Path
from typing import NoReturn

import click

from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger

logger = get_logger(__name__)

SHEBANG = f"#!/usr/bin/env -S {CFG.binary_name} run"


@click.command(
    short_help="Display or add the qq run shebang to a script.",
    help=f"""Add the {CFG.binary_name} run shebang to SCRIPT or replace the existing one. If no SCRIPT is given, print the {CFG.binary_name} run shebang.

{click.style("SCRIPT", fg="green")}   The script to add the shebang to. Optional.
""",
    cls=GNUHelpColorsCommand,
    help_options_color="bright_blue",
)
@click.argument(
    "script",
    type=str,
    metavar=click.style("SCRIPT", fg="green"),
    required=False,
    default=None,
)
def shebang(script: str | None) -> NoReturn:
    try:
        if script:
            _replaceOrAddShebang(Path(script))
        else:
            print(SHEBANG)
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        print()
        sys.exit(CFG.exit_codes.default)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        print()
        sys.exit(CFG.exit_codes.unexpected_error)


def _replaceOrAddShebang(file: Path) -> None:
    if not file.is_file():
        raise QQError(f"File '{str(file)}' does not exist.")

    content = file.read_text().splitlines()

    # check if the first line is a shebang
    if content and re.match(r"^#!", content[0]):
        content[0] = SHEBANG
    else:
        content.insert(0, SHEBANG + "\n")

    file.write_text("\n".join(content) + "\n")
