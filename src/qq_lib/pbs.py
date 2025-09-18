# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
from qq_lib.base import QQBatchInterface
from qq_lib.env_vars import DEBUG_MODE, GUARD, JOBDIR, STDERR_FILE, STDOUT_FILE
from qq_lib.logger import get_logger
from qq_lib.properties import QQProperties

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

    def translateSubmit(props: QQProperties, queue: str, script: str) -> str:
        command = f"qsub -q {queue} -o qq.out -e qq.out -v {GUARD}=1,{JOBDIR}={os.path.abspath(os.getcwd())},{STDOUT_FILE},{STDERR_FILE}"

        # export debug mode, if specified
        if os.environ.get(DEBUG_MODE) is not None:
            command += f",{DEBUG_MODE} "
        else:
            command += " "

        # translate properties
        trans_props = []
        if props.ncpus is not None:
            trans_props.append(f"ncpus={props.ncpus}")

        if props.vnode is not None:
            trans_props.append(f"vnode={props.vnode}")

        if len(trans_props) > 0:
            command += "-l "

        command += ",".join(trans_props) + " " + script

        logger.debug(command)
        return command
