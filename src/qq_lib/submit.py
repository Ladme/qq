# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import subprocess
import sys
import click
import os

from qq_lib.env_vars import GUARD, JOBDIR

@click.command(context_settings={"ignore_unknown_options": True})
@click.argument("args", nargs = -1, type = click.UNPROCESSED)
def submit(args):
    """
    Submit a script to the batch system.
    """
    result = subprocess.run(
        ["bash"], input=f"qsub -v {GUARD}=1,{JOBDIR}={os.path.abspath(os.getcwd())} {" ".join(args)}", text = True, check = False
    )

    sys.exit(result.returncode)

class QQSubmitter:
    pass