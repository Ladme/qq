# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class QQResources:
    ncpus: int | None = None
    vnode: str | None = None
    walltime: str | None = None
    work_dir: str | None = None
    work_size: str | None = None

    def __post_init__(self):
        # enforce workdir logic
        if self.work_dir == "jobdir":
            self.work_dir = None

        # enforce worksize logic
        if self.work_size is None and self.ncpus is not None:
            self.work_size = f"{self.ncpus}gb"

    def _toDict(self) -> dict[str, Any]:
        """Return all fields as a dict, excluding fields set to None."""
        return {k: v for k, v in asdict(self).items() if v is not None}
