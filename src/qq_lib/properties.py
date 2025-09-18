# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from typing import Optional, Union


class QQProperties:
    def setNCPUs(self, ncpus: Optional[int]):
        self.ncpus = ncpus

    def setVnode(self, vnode: Optional[str]):
        self.vnode = vnode
