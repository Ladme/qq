# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import getpass
import sys
from typing import TYPE_CHECKING, NoReturn

import click
from rich.console import Console

from qq_lib.batch.interface.meta import BatchMeta
from qq_lib.core.config import CFG

if TYPE_CHECKING:
    from qq_lib.batch.interface.queue import BatchQueueInterface
from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger

from .presenter import QQQueuesPresenter

logger = get_logger(__name__)


@click.command(
    short_help="Display the queues available to you.",
    help="""Display information about the queues available to the current user.

If the `--all` flag is specified, display all queues, including those not available.""",
    cls=GNUHelpColorsCommand,
    help_options_color="bright_blue",
)
@click.option(
    "-a",
    "--all",
    is_flag=True,
    help="Display all queues, including those not available to you.",
)
@click.option("--yaml", is_flag=True, help="Output queue metadata in YAML format.")
def queues(all: bool, yaml: bool) -> NoReturn:
    try:
        BatchSystem = BatchMeta.fromEnvVarOrGuess()
        queues: list[BatchQueueInterface] = BatchSystem.getQueues()
        user = getpass.getuser()

        if not all:
            queues = [q for q in queues if q.isAvailableToUser(user)]

        presenter = QQQueuesPresenter(queues, user, all)
        if yaml:
            presenter.dumpYaml()
        else:
            console = Console(record=False, markup=False)
            panel = presenter.createQueuesInfoPanel(console)
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
