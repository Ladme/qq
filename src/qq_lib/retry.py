# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from time import sleep
from typing import Any, Callable
from qq_lib.logger import get_logger

logger = get_logger(__name__, show_time = True)

class QQRetryer:
    """
    QQRetryer repeatedly executes a function until it succeeds or max attempts are reached.

    Attributes:
        func (Callable): The function or method to execute.
        args (Tuple): Positional arguments to pass to the function.
        kwargs (Dict): Keyword arguments to pass to the function.
        max_tries (int): Maximum number of attempts.
        wait_seconds (float): Time to wait between attempts.
    """

    def __init__(self, func: Callable, *args: Any, max_tries: int, wait_seconds: float, **kwargs: Any):
        self._func = func
        self._args = args
        self._kwargs = kwargs
        self._max_tries = max_tries
        self._wait_seconds = wait_seconds

    def run(self) -> Any:
        """
        Execute the function repeatedly until it succeeds or max_tries is reached.

        Any situation when the function does not raise an exception is considered a success.

        Returns:
            Any: The return value of the function if successful.

        Raises:
            Exception: The last exception raised if all attempts fail.
        """
        for attempt in range(1, self._max_tries + 1):
            try:
                return self._func(*self._args, **self._kwargs)
            except Exception as e:
                if attempt == self._max_tries:
                    raise
                logger.warning(
                    f"{e}\nAttempting again in {self._wait_seconds} seconds ({attempt}/{self._max_tries})"
                )
                sleep(self._wait_seconds)
