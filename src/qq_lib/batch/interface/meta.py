# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import os
from abc import ABCMeta

from qq_lib.batch.interface.interface import QQBatchInterface
from qq_lib.core.constants import BATCH_SYSTEM
from qq_lib.core.error import QQError
from qq_lib.core.logger import get_logger

logger = get_logger(__name__)


class QQBatchMeta(ABCMeta):
    """
    Metaclass for batch system classes.
    """

    # registry of supported batch systems
    _registry: dict[str, type[QQBatchInterface]] = {}

    def __str__(cls: type[QQBatchInterface]):
        """
        Get the string representation of the batch system class.
        """
        return cls.envName()

    @classmethod
    def register(cls, batch_cls: type[QQBatchInterface]):
        """
        Register a batch system class in the metaclass registry.

        Args:
            batch_cls: Subclass of QQBatchInterface to register.
        """
        cls._registry[batch_cls.envName()] = batch_cls

    @classmethod
    def fromStr(mcs, name: str) -> type[QQBatchInterface]:
        """
        Return the batch system class registered with the given name.

        Raises:
            QQError: If no class is registered for the given name.
        """
        try:
            return mcs._registry[name]
        except KeyError as e:
            raise QQError(f"No batch system registered as '{name}'.") from e

    @classmethod
    def guess(mcs) -> type[QQBatchInterface]:
        """
        Attempt to select an appropriate batch system implementation.

        The method scans through all registered batch systems in the order
        they were registered and returns the first one that reports itself
        as available.

        Raises:
            QQError: If no available batch system is found among the registered ones.

        Returns:
            type[QQBatchInterface]: The first available batch system class.
        """
        for BatchSystem in mcs._registry.values():
            if BatchSystem.isAvailable():
                logger.debug(f"Guessed batch system: {str(BatchSystem)}.")
                return BatchSystem

        # raise error if there is no available batch system
        raise QQError(
            "Could not guess a batch system. No registered batch system available."
        )

    @classmethod
    def fromEnvVarOrGuess(mcs) -> type[QQBatchInterface]:
        """
        Select a batch system based on the environment variable or by guessing.

        This method first checks the `BATCH_SYSTEM` environment variable. If it is set,
        the method returns the registered batch system class corresponding to its value.
        If the variable is not set, it falls back to `guess` to select an available
        batch system from the registered classes.

        Returns:
            type[QQBatchInterface]: The selected batch system class.

        Raises:
            QQError: If the environment variable is set to an unknown batch system name,
                    or if no available batch system can be guessed.
        """
        name = os.environ.get(BATCH_SYSTEM)
        if name:
            logger.debug(
                f"Using batch system name from an environment variable: {name}."
            )
            return QQBatchMeta.fromStr(name)

        return QQBatchMeta.guess()

    @classmethod
    def obtain(mcs, name: str | None) -> type[QQBatchInterface]:
        """
        Obtain a batch system class by name, environment variable, or guessing.

        Args:
            name (str | None): Optional name of the batch system to obtain.
                - If provided, returns the class registered under this name.
                - If `None`, falls back to `fromEnvVarOrGuess` to determine
                the batch system from the environment variable or by guessing.

        Returns:
            type[QQBatchInterface]: The selected batch system class.

        Raises:
            QQError: If `name` is provided but no batch system with that name is registered,
                    or if `name` is `None` and `fromEnvVarOrGuess` fails.
        """
        if name:
            return QQBatchMeta.fromStr(name)

        return QQBatchMeta.fromEnvVarOrGuess()
