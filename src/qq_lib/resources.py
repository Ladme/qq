# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


class QQResources:
    def setNCPUs(self, ncpus: int | None):
        self.ncpus = ncpus

    def setVnode(self, vnode: str | None):
        self.vnode = vnode

    def setWalltime(self, walltime: str | None):
        self.walltime = walltime

    def setWorkdir(self, workdir: str):
        # shared is the default option
        if workdir == "jobdir":
            self.workdir = None
        else:
            self.workdir = workdir

    def setWorksize(self, worksize: str | None):
        if not worksize:
            self.worksize = f"{self.ncpus}gb"
        else:
            self.worksize = worksize
