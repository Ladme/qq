# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from typing import Any, Protocol


class FieldCoupling:
    """
    Represents a coupling relationship between a dominant and recessive field.

    If the dominant field is set in the constructor, the recessive one is automatically set to None.
    """

    def __init__(self, dominant: str, recessive: str):
        self.dominant = dominant
        self.recessive = recessive

    def contains(self, field_name: str) -> bool:
        """Check if a field name is part of this coupling."""
        return field_name in (self.dominant, self.recessive)

    def getPair(self) -> tuple[str, str]:
        """Return both field names as a tuple."""
        return (self.dominant, self.recessive)

    def hasValue(self, instance: Any) -> bool:
        """Check if either field in this coupling has a non-None value."""
        return (
            getattr(instance, self.dominant) is not None
            or getattr(instance, self.recessive) is not None
        )


def coupled_fields(*couplings: FieldCoupling):
    """
    Class decorator that enforces field coupling rules in __post_init__.
    """

    def decorator(cls):
        # save the couplings
        cls._field_couplings = couplings

        # save the original __post_init__ if it exists
        original_post_init = getattr(cls, "__post_init__", None)

        def __post_init__(self):
            # apply coupling rules
            for coupling in self._field_couplings:
                dominant_value = getattr(self, coupling.dominant)

                # if dominant is set, recessive must be None
                if dominant_value is not None:
                    setattr(self, coupling.recessive, None)

            # call original __post_init__ if it existed
            if original_post_init:
                original_post_init(self)

        @staticmethod
        def getCouplingForField(field_name: str) -> FieldCoupling | None:
            """Return the FieldCoupling that contains the given field name, or None."""
            for coupling in cls._field_couplings:
                if coupling.contains(field_name):
                    return coupling
            return None

        cls.__post_init__ = __post_init__
        cls.getCouplingForField = getCouplingForField

        return cls

    return decorator


class HasCouplingMethods(Protocol):
    """Protocol for classes decorated with @coupled_fields."""

    _field_couplings: tuple[FieldCoupling, ...]

    @staticmethod
    def getCouplingForField(field_name: str) -> FieldCoupling | None:
        """Return the FieldCoupling that contains the given field name, or None."""
        ...
