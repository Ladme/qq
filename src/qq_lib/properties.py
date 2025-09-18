# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from typing import Union


class QQProperties:
    def setNCPUs(self, ncpus: Union[int, None]):
        self.ncpus = ncpus

    def setVnode(self, vnode: Union[str, None]):
        self.vnode = vnode
