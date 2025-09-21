# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import socket
import subprocess
from pathlib import Path
from subprocess import CompletedProcess

from qq_lib.batch import QQBatchInterface
from qq_lib.error import QQError
from qq_lib.logger import get_logger
from qq_lib.resources import QQResources
from qq_lib.states import BatchState

logger = get_logger(__name__)

# magic number indicating unreachable directory when navigating to it
CD_FAIL = 94
# exit code of ssh if connection fails
SSH_FAIL = 255

class QQPBS(QQBatchInterface):
    """
    Implementation of QQBatchInterface for PBS Pro batch system.
    """

    def envName() -> str:
        return "PBS"

    def usernameEnvVar() -> str:
        return "PBS_O_LOGNAME"

    def jobIdEnvVar() -> str:
        return "PBS_JOBID"

    def workDirEnvVar() -> str:
        return "PBS_O_WORKDIR"

    def scratchDirEnvVar() -> str:
        return "SCRATCHDIR"

    def jobState() -> str:
        return "job_state"

    def translateJobState(state: str) -> BatchState:
        return BatchState.fromCode(state)

    def translateSubmit(res: QQResources, queue: str, script: str) -> str:
        qq_output = str(Path(script).with_suffix(".qqout"))
        command = f"qsub -q {queue} -j eo -e {qq_output} -V "

        # translate properties
        trans_props = []
        if res.ncpus:
            trans_props.append(f"ncpus={res.ncpus}")

        if res.vnode:
            trans_props.append(f"vnode={res.vnode}")

        if res.walltime:
            trans_props.append(f"walltime={res.walltime}")

        if res.workdir:
            trans_props.append(f"{res.workdir}={res.worksize}")

        if len(trans_props) > 0:
            command += "-l "

        command += ",".join(trans_props) + " " + script

        return command

    def translateKillForce(job_id: str) -> str:
        return f"qdel -W force {job_id}"

    def translateKill(job_id: str) -> str:
        return f"qdel {job_id}"

    def navigateToDestination(host: str, directory: Path) -> int:
        # if the directory is on the current host, we do not need to use ssh
        if host == socket.gethostname():
            logger.debug("Current host is the same as target host. Using 'cd'.")
            if not directory.is_dir():
                return 1
            
            subprocess.run(["bash"], cwd=directory)
            
            # if the directory exists, always return 0, no matter what the user does inside the terminal
            return 0
        
        # the directory is on an another node
        ssh_command = [
            "ssh",
            host,
            "-t",
            f"cd {directory} || exit {CD_FAIL} && exec bash -l",
        ]

        logger.debug(f"Using ssh: '{' '.join(ssh_command)}'")

        exit_code = subprocess.run(ssh_command).returncode

        # the subprocess exit code can come from:
        # - SSH itself failing - returns SSH_FAIL
        # - the explicit exit code we set if 'cd' to the directory fails - returns CD_FAIL
        # - the exit code of the last command the user runs in the interactive shell
        #
        # we ignore user exit codes entirely and only treat SSH_FAIL and CD_FAIL as errors
        if exit_code == SSH_FAIL:
            return SSH_FAIL
        if exit_code == CD_FAIL:
            return CD_FAIL
        return 0

    def getJobInfo(jobid: str) -> dict[str, str]:
        command = f"qstat -fx {jobid}"

        result = subprocess.run(
            ["bash"], input=command, text=True, check=False, capture_output=True
        )

        if result.returncode != 0:
            return {}
        return _parse_pbs_dump_to_dictionary(result.stdout)


def _parse_pbs_dump_to_dictionary(text: str) -> dict[str, str]:
    """
    Parse a PBS/Torque-style job status dump into a dictionary.

    Returns:
        Dictionary mapping keys to values.
    """
    result: dict[str, str] = {}
    current_key = None

    for raw_line in text.splitlines():
        line = raw_line.rstrip()

        if "=" in line and not line.lstrip().startswith("="):
            key, value = line.split("=", 1)
            current_key = key.strip()
            result[current_key] = value.strip()
        elif current_key is not None:
            result[current_key] += line.strip()
        else:
            pass

    logger.debug(f"PBS qstat dump file: {result}")
    return result
