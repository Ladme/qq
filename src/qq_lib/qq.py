# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import sys

import click
from click_help_colors import HelpColorsGroup

from qq_lib.cd.cli import cd
from qq_lib.clear.cli import clear
from qq_lib.go.cli import go
from qq_lib.info.cli import info
from qq_lib.jobs.cli import jobs
from qq_lib.kill.cli import kill
from qq_lib.killall.cli import killall
from qq_lib.nodes.cli import nodes
from qq_lib.queues.cli import queues
from qq_lib.run.cli import run
from qq_lib.shebang.cli import shebang
from qq_lib.stat.cli import stat
from qq_lib.submit.cli import submit
from qq_lib.sync.cli import sync
from qq_lib.wipe.cli import wipe

__version__ = "0.6.0-dev.2"

# support both --help and -h
_CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


@click.group(
    cls=HelpColorsGroup,
    help_options_color="bright_blue",
    invoke_without_command=True,
    context_settings=_CONTEXT_SETTINGS,
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
cli.add_command(wipe)
