# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import sys
from pathlib import Path
from typing import NoReturn

import click
from click_option_group import optgroup

from qq_lib.core.click_format import GNUHelpColorsCommand
from qq_lib.core.common import get_runtime_files
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.submit.factory import QQSubmitterFactory

logger = get_logger(__name__)


# Note that all options must be part of an optgroup otherwise QQParser breaks.
@click.command(
    short_help="Submit a job to the batch system.",
    help=f"""
Submit a qq job to a batch system from the command line.

{click.style("SCRIPT", fg="green")}   Path to the script to submit.

All the options can also be specified inside the submitted script itself
using qq directives of this format: `# qq <option>=<value>`.
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
    "--account",
    type=str,
    default=None,
    help="Account to use for the job. Only needed in environments with accounting (e.g., IT4Innovations).",
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
        "By default, all files and directories except the qq info file and the archive directory are copied to the working directory.\n"
    ),
)
@optgroup.option(
    "--depend",
    type=str,
    default=None,
    help="""Specify job dependencies. You can provide one or more dependency expressions separated by commas, spaces, or both.
Each expression should follow the format `<type>=<job_id>[:<job_id>...]`, e.g., `after=1234`, `afterok=456:789`.""",
)
@optgroup.option(
    "--batch-system",
    type=str,
    default=None,
    help=f"Name of the batch system to submit the job to. If not specified, the system will use the environment variable '{CFG.env_vars.batch_system}' or attempt to auto-detect it.",
)
@optgroup.group(f"{click.style('Requested resources', fg='yellow')}")
@optgroup.option(
    "--nnodes",
    type=int,
    default=None,
    help="Number of computing nodes to allocate for the job.",
)
@optgroup.option(
    "--ncpus",
    type=int,
    default=None,
    help="Number of CPU cores to allocate for the job.",
)
@optgroup.option(
    "--mem-per-cpu",
    type=str,
    default=None,
    help="Memory to allocate per CPU core. Specify as 'Nmb' or 'Ngb' (e.g., 500mb or 2gb).",
)
@optgroup.option(
    "--mem",
    type=str,
    default=None,
    help="Total memory to allocate for the job. Specify as 'Nmb' or 'Ngb' (e.g., 500mb or 10gb). Overrides `--mem-per-cpu`.",
)
@optgroup.option(
    "--ngpus", type=int, default=None, help="Number of GPUs to allocate for the job."
)
@optgroup.option(
    "--walltime",
    type=str,
    default=None,
    help="Maximum runtime allowed for the job.",
)
@optgroup.option(
    "--work-dir",
    "--workdir",
    type=str,
    default=None,
    help="Type of working directory to use for the job.",
)
@optgroup.option(
    "--work-size-per-cpu",
    "--worksize-per-cpu",
    type=str,
    default=None,
    help="Storage to allocate per CPU core. Specify as 'Ngb' (e.g., 1gb).",
)
@optgroup.option(
    "--work-size",
    "--worksize",
    type=str,
    default=None,
    help="Total storage to allocate for the job. Specify as 'Ngb' (e.g., 10gb). Overrides `--work-size-per-cpu`.",
)
@optgroup.option(
    "--props",
    type=str,
    default=None,
    help="Colon-, comma-, or space-separated list of node properties required (e.g., cl_two) or prohibited (e.g., ^cl_two) to run the job.",
)
@optgroup.group(
    f"{click.style('Loop options', fg='yellow')}",
    help="Only used when job-type is 'loop'.",
)
@optgroup.option(
    "--loop-start",
    type=int,
    default=None,
    help="Starting cycle for a loop job. Defaults to 1.",
)
@optgroup.option(
    "--loop-end", type=int, default=None, help="Ending cycle for a loop job."
)
@optgroup.option(
    "--archive",
    type=str,
    default=None,
    help="Directory name for archiving files from a loop job. Defaults to 'storage'.",
)
@optgroup.option(
    "--archive-format",
    type=str,
    default=None,
    help="Filename format for archived files. Defaults to 'job%04d'.",
)
def submit(script: str, **kwargs) -> NoReturn:
    """
    Submit a qq job to a batch system from the command line.
    """
    try:
        if not (script_path := Path(script)).is_file():
            raise QQError(f"Script '{script}' does not exist or is not a file.")

        # parse options from the command line and from the script itself
        factory = QQSubmitterFactory(
            script_path.resolve(), submit.params, sys.argv[2:], **kwargs
        )
        submitter = factory.makeSubmitter()

        # guard against multiple submissions from the same directory
        if get_runtime_files(submitter.getInputDir()) and not submitter.continuesLoop():
            raise QQError(
                "Detected qq runtime files in the submission directory. Submission aborted."
            )

        job_id = submitter.submit()
        logger.info(f"Job '{job_id}' submitted successfully.")
        sys.exit(0)
    except QQError as e:
        logger.error(e)
        sys.exit(CFG.exit_codes.default)
    except Exception as e:
        logger.critical(e, exc_info=True, stack_info=True)
        sys.exit(CFG.exit_codes.unexpected_error)
