# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import sys

import click
from click_help_colors import HelpColorsGroup

from qq_lib.cd import cd
from qq_lib.clear import clear
from qq_lib.go import go
from qq_lib.info import info
from qq_lib.jobs import jobs
from qq_lib.kill import kill
from qq_lib.killall import killall
from qq_lib.nodes import nodes
from qq_lib.queues import queues
from qq_lib.run import run
from qq_lib.shebang import shebang
from qq_lib.stat import stat
from qq_lib.submit import submit
from qq_lib.sync import sync

__version__ = "0.4.0-dev.2"


@click.group(
    cls=HelpColorsGroup, help_options_color="bright_blue", invoke_without_command=True
)
@click.option(
    "--version",
    is_flag=True,
    help="Print the current version of qq and exit.",
)
@click.pass_context
def cli(ctx: click.Context, version: bool):
    """
    Run any qq command.

    qq is a wrapper around batch scheduling systems, simplifying job submission and management.

    For detailed information, visit: https://ladme.github.io/qq-manual.
    """
    if version:
        print(__version__)
        sys.exit(0)

    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())
        sys.exit(0)


cli.add_command(run)
cli.add_command(submit)
cli.add_command(clear)
cli.add_command(info)
cli.add_command(go)
cli.add_command(kill)
cli.add_command(jobs)
cli.add_command(stat)
cli.add_command(cd)
cli.add_command(sync)
cli.add_command(killall)
cli.add_command(queues)
cli.add_command(nodes)
cli.add_command(shebang)
