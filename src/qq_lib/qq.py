# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import click

from qq_lib.clear import clear
from qq_lib.go import go
from qq_lib.info import info
from qq_lib.kill import kill
from qq_lib.run import run
from qq_lib.submit import submit


@click.group()
def cli():
    """
    Run any qq subcommand. qq is a wrapper around PBS Pro
    and Slurm alowing simpler job management.
    """
    pass


cli.add_command(run)
cli.add_command(submit)
cli.add_command(clear)
cli.add_command(info)
cli.add_command(go)
cli.add_command(kill)
