# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import sys
from pathlib import Path

import click
from click_option_group import optgroup

from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.constants import BATCH_SYSTEM
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.submit.factory import QQSubmitterFactory

logger = get_logger(__name__)


# Note that all options must be part of an optgroup otherwise QQParser breaks.
@click.command(
    short_help="Submit a qq job to the batch system.",
    help=f"""
Submit a qq job to a batch system from the command line.

{click.style("SCRIPT", fg="green")}   Path to the script to submit.

The submitted script must be located in the directory from which
'qq submit' is invoked.
""",
    cls=GNUHelpColorsCommand,
    help_options_color="bright_blue",
)
@click.argument("script", type=str, metavar=click.style("SCRIPT", fg="green"))
@optgroup.group(f"{click.style('General settings', fg='yellow')}")
@optgroup.option(
    "--queue",
    "-q",
    type=str,
    default=None,
    help="Name of the queue to submit the job to.",
)
@optgroup.option(
    "--job-type",
    type=str,
    default=None,
    help="Type of the qq job. Defaults to 'standard'.",
)
@optgroup.option(
    "--exclude",
    type=str,
    default=None,
    help=(
        f"A colon-, comma-, or space-separated list of files and directories that should {click.style('not', bold=True)} be copied to the working directory.\n"
        "     By default, all files and directories except for the qq info file and the archive directory are copied to the working directory.\n"
    ),
)
@optgroup.option(
    "--batch-system",
    type=str,
    default=None,
    help=f"Batch system to submit the job into. If not specified, will load the batch system from the environment variable '{BATCH_SYSTEM}' or guess it.",
)
@optgroup.option(
    "--non-interactive",
    is_flag=True,
    help="Use when using qq submit in a non-interactive environment. Any interactive prompt will be automatically skipped and evaluated as 'no'.",
)
@optgroup.group(f"{click.style('Requested resources', fg='yellow')}")
@optgroup.option(
    "--nnodes", type=int, default=None, help="Number of computing nodes to use."
)
@optgroup.option(
    "--ncpus",
    type=int,
    default=None,
    help="Number of CPU cores to use.",
)
@optgroup.option(
    "--mem-per-cpu",
    type=str,
    default=None,
    help="Amount of memory to use per a single CPU core. Specify as 'Nmb' or 'Ngb' (e.g., 500mb or 2gb).",
)
@optgroup.option(
    "--mem",
    type=str,
    default=None,
    help="Absolute amount of memory to use. Specify as 'Nmb' or 'Ngb' (e.g., 500mb or 10gb). Overrides '--mem-per-cpu'.",
)
@optgroup.option("--ngpus", type=int, default=None, help="Number of GPUs to use.")
@optgroup.option(
    "--walltime",
    type=str,
    default=None,
    help="Maximum allowed runtime for the job.",
)
@optgroup.option(
    "--work-dir",
    "--workdir",
    type=str,
    default=None,
    help="Type of working directory to use.",
)
@optgroup.option(
    "--work-size-per-cpu",
    "--worksize-per-cpu",
    type=str,
    default=None,
    help="Size of the storage requested for running the job per a single CPU core. Specify as 'Ngb' (e.g., 1gb).",
)
@optgroup.option(
    "--work-size",
    "--worksize",
    type=str,
    default=None,
    help="Absolute size of the storage requested for running the job. Specify as 'Ngb' (e.g., 10gb). Overrides '--work-size-per-cpu'.",
)
@optgroup.option(
    "--props",
    type=str,
    default=None,
    help="A colon-, comma-, or space-separated list of properties that a node must include (e.g., cl_two) or exclude (e.g., ^cl_two) in order to run the job.",
)
@optgroup.group(
    f"{click.style('Loop options', fg='yellow')}",
    help="Only used when job-type is 'loop'.",
)
@optgroup.option(
    "--loop-start",
    type=int,
    default=None,
    help="The first cycle of the loop job. Defaults to 1.",
)
@optgroup.option(
    "--loop-end", type=int, default=None, help="The last cycle of the loop job."
)
@optgroup.option(
    "--archive",
    type=str,
    default=None,
    help="Name of the directory for archiving files from the loop job. Defaults to 'storage'.",
)
@optgroup.option(
    "--archive-format",
    type=str,
    default=None,
    help="Format of the archived filenames. Defaults to 'job%04d'.",
)
def submit(script: str, **kwargs):
    """
    Submit a qq job to a batch system from the command line.

    Note that the submitted script must be located in the same directory from which 'qq submit' is invoked.
    """
    try:
        if not (script_path := Path(script)).is_file():
            raise QQError(f"Script '{script}' does not exist or is not a file.")

        # parse options from the command line and from the script itself
        factory = QQSubmitterFactory(
            script_path.resolve(), submit.params, sys.argv[2:], **kwargs
        )
        submitter = factory.makeSubmitter()

        # catching multiple submissions
        submitter.guardOrClear()

        job_id = submitter.submit()
        logger.info(f"Job '{job_id}' submitted successfully.")
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        sys.exit(91)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(99)
