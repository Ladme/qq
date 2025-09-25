# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from dataclasses import asdict, dataclass

from qq_lib.logger import get_logger

logger = get_logger(__name__)


@dataclass
class QQResources:
    ncpus: int | None = None
    vnode: str | None = None
    walltime: str | None = None
    work_dir: str | None = None
    work_size: str | None = None

    def toDict(self) -> dict[str, object]:
        """Return all fields as a dict, excluding fields set to None."""
        return {
            k: v
            for k, v in asdict(self).items()
            if v is not None and k != "batch_system"
        }

    def useScratch(self) -> bool:
        """
        Determine if the job uses a scratch directory.

        Returns:
            True if a work_dir is defined, False otherwise.
        """
        return self.work_dir is not None
