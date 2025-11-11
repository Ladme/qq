# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import getpass
import os
import shutil
from pathlib import Path

from qq_lib.batch.interface.meta import BatchMeta, batch_system
from qq_lib.batch.slurmit4i import SlurmIT4I
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger
from qq_lib.properties.depend import Depend
from qq_lib.properties.resources import Resources

logger = get_logger(__name__)


@batch_system
class SlurmLumi(SlurmIT4I, metaclass=BatchMeta):
    # all scratch directory types supported by SlurmLumi
    SUPPORTED_SCRATCHES = ["scratch", "flash"]

    @classmethod
    def envName(cls) -> str:
        return "SlurmLumi"

    @classmethod
    def isAvailable(cls) -> bool:
        return shutil.which("lumi-allocations") is not None

    @classmethod
    def jobSubmit(
        cls,
        res: Resources,
        queue: str,
        script: Path,
        job_name: str,
        depend: list[Depend],
        env_vars: dict[str, str],
        account: str | None = None,
    ) -> str:
        # set the 'lumi_scratch_type' env var to be able to decide in getScratchDir
        # whether to create a scratch directory on /scratch or on /flash
        if res.usesScratch():
            assert res.work_dir is not None
            env_vars[CFG.env_vars.lumi_scratch_type] = res.work_dir

        return super().jobSubmit(
            res, queue, script, job_name, depend, env_vars, account
        )

    @classmethod
    def getScratchDir(cls, job_id: str) -> Path:
        if not (account := os.environ.get(CFG.env_vars.slurm_job_account)):
            raise QQError(f"No account is defined for job '{job_id}'.")

        # get the storage type
        if not (storage_type := os.environ.get(CFG.env_vars.lumi_scratch_type)):
            raise QQError(
                f"Environment variable '{CFG.env_vars.lumi_scratch_type}' is not defined. This is a bug!"
            )

        # create the scratch directory on /scratch or /flash
        try:
            scratch = Path(
                f"/{storage_type}/{account.lower()}/{getpass.getuser()}/qq-jobs/job_{job_id}"
            )
            logger.debug(
                f"Creating directory '{str(scratch)}' on '{storage_type}' storage."
            )
            scratch.mkdir(parents=True, exist_ok=True)
            return scratch
        except Exception as e:
            raise QQError(
                f"Could not create a scratch directory for job '{job_id}': {e}"
            ) from e

    @classmethod
    def _getDefaultResources(cls) -> Resources:
        return Resources(
            nnodes=1,
            ncpus=128,
            mem_per_cpu="500mb",
            work_dir="scratch",
            walltime="1d",
        )
