# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import getpass
import sys
from typing import TYPE_CHECKING, NoReturn

import click
from rich.console import Console

from qq_lib.batch.interface.meta import QQBatchMeta

if TYPE_CHECKING:
    from qq_lib.batch.interface.node import BatchNodeInterface
from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.nodes.presenter import QQNodesPresenter

logger = get_logger(__name__)


@click.command(
    short_help="Display the nodes of the batch system.",
    help="""Display information about the the nodes of the batch system.

If the `--all` flag is specified, display all nodes, including those not available.""",
    cls=GNUHelpColorsCommand,
    help_options_color="bright_blue",
)
@click.option(
    "-a",
    "--all",
    is_flag=True,
    help="Display all nodes, including those that are down or inaccessible.",
)
@click.option("--yaml", is_flag=True, help="Output node metadata in YAML format.")
def nodes(all: bool, yaml: bool) -> NoReturn:
    try:
        BatchSystem = QQBatchMeta.fromEnvVarOrGuess()
        nodes: list[BatchNodeInterface] = BatchSystem.getNodes()
        user = getpass.getuser()

        if not all:
            nodes = [n for n in nodes if n.isAvailableToUser(user)]

        presenter = QQNodesPresenter(nodes, user, all)
        if yaml:
            presenter.dumpYaml()
        else:
            console = Console(record=False, markup=False)
            panel = presenter.createNodesInfoPanel(console)
            console.print(panel)
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        print()
        sys.exit(CFG.exit_codes.default)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        print()
        sys.exit(CFG.exit_codes.unexpected_error)
