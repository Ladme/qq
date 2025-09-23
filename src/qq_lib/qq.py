# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import click

from qq_lib.batch import QQBatchMeta
from qq_lib.clear import clear
from qq_lib.go import go
from qq_lib.info import info
from qq_lib.kill import kill
from qq_lib.pbs import QQPBS
from qq_lib.run import run
from qq_lib.submit import submit
from qq_lib.vbs import QQVBS


@click.group()
def cli():
    """
    Run any qq subcommand.
    """
    pass


cli.add_command(run)
cli.add_command(submit)
cli.add_command(clear)
cli.add_command(info)
cli.add_command(go)
cli.add_command(kill)

# register supported batch systems
QQBatchMeta.register(QQPBS)
QQBatchMeta.register(QQVBS)