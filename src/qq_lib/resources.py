# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


class QQResources:
    def setNCPUs(self, ncpus: int | None):
        self.ncpus = ncpus

    def setVnode(self, vnode: str | None):
        self.vnode = vnode

    def setWalltime(self, walltime: str | None):
        self.walltime = walltime
