# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import subprocess
from pathlib import Path
from subprocess import CompletedProcess

from qq_lib.batch import QQBatchInterface
from qq_lib.env_vars import (
    DEBUG_MODE,
    GUARD,
    INFO_FILE,
    JOBDIR,
    STDERR_FILE,
    STDOUT_FILE,
)
from qq_lib.logger import get_logger
from qq_lib.resources import QQResources

logger = get_logger(__name__)


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

    def translateSubmit(res: QQResources, queue: str, script: str) -> str:
        qq_output = str(Path(script).with_suffix(".qqout"))
        command = f"qsub -q {queue} -o {qq_output} -e {qq_output} -v {GUARD},{JOBDIR},{STDOUT_FILE},{STDERR_FILE},{INFO_FILE}"

        if os.environ.get(DEBUG_MODE):
            command += f",{DEBUG_MODE} "
        else:
            command += " "

        # translate properties
        trans_props = []
        if res.ncpus is not None:
            trans_props.append(f"ncpus={res.ncpus}")

        if res.vnode is not None:
            trans_props.append(f"vnode={res.vnode}")

        if len(trans_props) > 0:
            command += "-l "

        command += ",".join(trans_props) + " " + script

        return command

    def translateKill(job_id: str) -> str:
        return f"qdel -W force {job_id}"

    def navigateToDestination(host: str, directory: Path) -> CompletedProcess[bytes]:
        ssh_command = [
            "ssh",
            host,
            "-t",
            f"cd {directory} && exec bash -l",
        ]

        return subprocess.run(ssh_command)
