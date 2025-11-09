# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

__all__ = [
    "Info",
    "JobType",
    "QQLoopInfo",
    "QQResources",
    "Size",
    "BatchState",
    "NaiveState",
    "RealState",
]

_MODULES = {
    "Info": "info",
    "JobType": "job_type",
    "QQLoopInfo": "loop",
    "QQResources": "resources",
    "Size": "size",
    "BatchState": "states",
    "NaiveState": "states",
    "RealState": "states",
}


def __getattr__(name: str):
    if name in _MODULES:
        return getattr(__import__(f".{_MODULES[name]}", fromlist=[name], level=1), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
