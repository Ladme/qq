# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import socket
import subprocess
from dataclasses import fields
from pathlib import Path

from qq_lib.batch import BatchJobInfoInterface, BatchOperationResult, QQBatchMeta, QQBatchInterface
from qq_lib.logger import get_logger
from qq_lib.resources import QQResources
from qq_lib.states import BatchState
from qq_lib.constants import QQ_OUT_SUFFIX

logger = get_logger(__name__)

# magic number indicating unreachable directory when navigating to it
CD_FAIL = 94
# exit code of ssh if connection fails
SSH_FAIL = 255

# forward declaration
class PBSJobInfo(BatchJobInfoInterface):
    pass

class QQPBS(QQBatchInterface[PBSJobInfo], metaclass=QQBatchMeta):
    """
    Implementation of QQBatchInterface for PBS Pro batch system.
    """

    def envName() -> str:
        return "PBS"

    def getScratchDir(job_id: int) -> BatchOperationResult:
        scratch_dir = os.environ.get("SCRATCHDIR")
        if not scratch_dir:
            return BatchOperationResult.error(1, f"Scratch directory for job '{job_id}' is undefined.")
        
        return BatchOperationResult.success(scratch_dir)
    
    def jobSubmit(res: QQResources, queue: str, script: Path) -> BatchOperationResult:
        # get the submission command
        command = QQPBS._translateSubmit(
            res, queue, str(script)
        )
        logger.debug(command)

        # submit the script
        result = subprocess.run(
            ["bash"], input=command, text=True, check=False, capture_output=True
        )

        return BatchOperationResult.fromExitCode(result.returncode, error_message = result.stderr.strip(), success_message = result.stdout.strip())
        
    def jobKill(job_id: str) -> BatchOperationResult:
        command = QQPBS._translateKill(job_id)
        logger.debug(command)

        # run the kill command
        result = subprocess.run(
            ["bash"], input=command, text=True, check=False, capture_output=True
        )

        return BatchOperationResult.fromExitCode(result.returncode, success_message = result.stdout.strip(), error_message = result.stderr.strip())
    
    def jobKillForce(job_id: str) -> BatchOperationResult:
        command = QQPBS._translateKillForce(job_id)
        logger.debug(command)

        # run the kill command
        result = subprocess.run(
            ["bash"], input=command, text=True, check=False, capture_output=True
        )

        return BatchOperationResult.fromExitCode(result.returncode, error_message = result.stderr.strip())
    
    def navigateToDestination(host: str, directory: Path) -> BatchOperationResult:
        # if the directory is on the current host, we do not need to use ssh
        if host == socket.gethostname():
            logger.debug("Current host is the same as target host. Using 'cd'.")
            if not directory.is_dir():
                return BatchOperationResult.error(1)

            subprocess.run(["bash"], cwd=directory)

            # if the directory exists, always report success, 
            # no matter what the user does inside the terminal
            return BatchOperationResult.success()

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
            return BatchOperationResult.error(SSH_FAIL)
        if exit_code == CD_FAIL:
            return BatchOperationResult.error(CD_FAIL)
        return BatchOperationResult.success()

    def getJobInfo(job_id: str) -> PBSJobInfo:
        return PBSJobInfo(job_id)

    def _translateSubmit(res: QQResources, queue: str, script: str) -> str:
        qq_output = str(Path(script).with_suffix(QQ_OUT_SUFFIX))
        command = f"qsub -q {queue} -j eo -e {qq_output} -V "

        # handle resources
        trans_res = QQPBS._translateResources(res)

        if len(trans_res) > 0:
            command += "-l "

        command += ",".join(trans_res) + " " + script

        return command

    @staticmethod
    def _translateResources(res: QQResources) -> list[str]:
        trans_res = []
        for (name, value) in res.toDict().items():
            if name in ["work_dir", "work_size"]:
                continue

            trans_res.append(f"{name}={value}")
        
        # translate working directory resource
        workdir = QQPBS._translateWorkDir(res)
        if workdir:
            trans_res.append(workdir)

        return trans_res
    
    @staticmethod
    def _translateWorkDir(res: QQResources) -> str | None:
        if not res.work_dir:
            return None

        return f"{res.work_dir}={res.work_size}"

    @staticmethod
    def _translateKillForce(job_id: str) -> str:
        return f"qdel -W force {job_id}"

    @staticmethod
    def _translateKill(job_id: str) -> str:
        return f"qdel {job_id}"

class PBSJobInfo(BatchJobInfoInterface):
    """
    Implementation of BatchJobInterface for PBS.
    """
    def __init__(self, job_id: str):
        self._job_id = job_id
        self._info: dict[str, str] = {}

        self.update()
        
    def update(self):
        # get job info from PBS
        command = f"qstat -fx {self._job_id}"

        result = subprocess.run(
            ["bash"], input = command, text = True, check = False, capture_output = True
        )

        if result.returncode != 0:
            # if qstat fails, information is empty
            self._info: dict[str, str] = {}
        else:
            self._info = PBSJobInfo._parse_pbs_dump_to_dictionary(result.stdout)
    
    def getJobState(self) -> BatchState:
        state = self._info.get("job_state")
        if not state:
            return BatchState.UNKNOWN
        
        return BatchState.fromCode(state)

    @staticmethod
    def _parse_pbs_dump_to_dictionary(text: str) -> dict[str, str]:
        """
        Parse a PBS job status dump into a dictionary.

        Returns:
            Dictionary mapping keys to values.
        """
        result: dict[str, str] = {}
        current_key = None

        for raw_line in text.splitlines():
            line = raw_line.rstrip()

            if " = " in line and not line.lstrip().startswith("="):
                # new key
                key, value = line.split(" = ", 1)
                current_key = key.strip()
                result[current_key] = value.strip()
            elif current_key is not None:
                # multiline values
                result[current_key] += line.strip()
            else:
                pass

        logger.debug(f"PBS qstat dump file: {result}")
        return result
