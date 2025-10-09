# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from collections.abc import Callable
from typing import Any


class QQRepeater:
    """
    Execute a given function repeatedly for a collection of items,
    with optional per-exception handling and tracking of encountered errors.

    Attributes:
        items (list[Any]): List of items to process.
        encountered_errors (dict[int, BaseException]): A dictionary mapping
            item indices to exceptions encountered during execution.
        current_iteration (int): The index of the item currently being processed.

    Args:
        items (list[Any]): A list of items to iterate over.
        func (Callable): Function to execute for each item. The item will be passed
            as the first argument, followed by any `*args` and `**kwargs`.
        *args (Any): Positional arguments forwarded to `func`.
        **kwargs (Any): Keyword arguments forwarded to `func`.
    """

    def __init__(
        self,
        items: list[Any],
        func: Callable,
        *args: Any,
        **kwargs: Any,
    ):
        self.encountered_errors = {}
        self.items = items
        self.current_iteration = 0

        self._handlers: dict[type[BaseException], Callable[[BaseException], Any]] = {}
        self._func = func
        self._args = args
        self._kwargs = kwargs

    def onException(self, exc_type: type[BaseException], handler: Callable) -> None:
        """
        Register a handler function for a specific exception type.

        Args:
            exc_type (type[BaseException]): The exception type to handle.
            handler (Callable): Function to call when `exc_type` is raised.
                The handler must accept two arguments:
                - BaseException: The caught exception instance.
                - QQRepeater: Reference to this `QQRepeater` instance.
        """
        self._handlers[exc_type] = handler

    def run(self):
        """
        Execute the target function for all items, invoking handlers for exceptions.

        Iterates over all items, calling the provided function with each one.
        If an exception is raised and a handler for that exception type
        is registered, the handler is called.

        Unhandled exceptions propagate normally and interrupt the iteration.

        Exceptions are recorded in `encountered_errors`, mapping the item's
        index to the raised exception instance.
        """
        for i, item in enumerate(self.items):
            self.current_iteration = i
            try:
                self._func(item, *self._args, **self._kwargs)
            except tuple(self._handlers.keys()) as e:
                self.encountered_errors[i] = e
                handler = self._handlers[type(e)]
                handler(e, self)
