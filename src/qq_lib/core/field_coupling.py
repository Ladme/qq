# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from typing import Any, Protocol


class FieldCoupling:
    """
    Represents a coupling among multiple fields, ordered by dominance.

    The earlier a field appears in `fields`, the more dominant it is.
    If multiple fields have values in an instance, only the most dominant
    one is preserved; all others are set to None.
    """

    def __init__(self, *fields: str):
        if len(fields) < 2:
            raise ValueError("FieldCoupling requires at least two fields")
        self.fields = list(fields)

    def contains(self, field_name: str) -> bool:
        """Return True if the field participates in this coupling."""
        return field_name in self.fields

    def getFields(self) -> tuple[str, ...]:
        """Return all coupled fields as a tuple."""
        return tuple(self.fields)

    def hasValue(self, instance: Any) -> bool:
        """Return True if any of the coupled fields has a non-None value."""
        return any(getattr(instance, field) is not None for field in self.fields)

    def getMostDominantSetField(self, instance: Any) -> str | None:
        """
        Return the name of the most dominant field that has a non-None value,
        or None if none of them do.
        """
        for field in self.fields:
            if getattr(instance, field) is not None:
                return field

        return None

    def enforce(self, instance: Any):
        """
        Enforce dominance rules: only the most dominant field that is set
        keeps its value; others are reset to None.
        """
        dominant_set_field = self.getMostDominantSetField(instance)
        if dominant_set_field is None:
            return

        for field in self.fields:
            if field != dominant_set_field:
                setattr(instance, field, None)


def coupled_fields(*couplings: FieldCoupling):
    """
    Class decorator that enforces multi-field coupling rules in __post_init__.
    """

    def decorator(cls):
        cls._field_couplings = couplings
        original_post_init = getattr(cls, "__post_init__", None)

        def __post_init__(self):
            for coupling in self._field_couplings:
                coupling.enforce(self)

            if original_post_init:
                original_post_init(self)

        @staticmethod
        def getCouplingForField(field_name: str) -> FieldCoupling | None:
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
