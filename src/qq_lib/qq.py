# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import click
from qq_lib.run import run
from qq_lib.submit import submit


@click.group()
def cli():
    """
    Run any qq subcommand. qq is a wrapper around PBS Pro and Slurm alowing simpler job management.
    """
    pass


cli.add_command(run)
cli.add_command(submit)