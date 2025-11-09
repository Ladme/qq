# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from unittest.mock import Mock

import pytest

from qq_lib.core.repeater import Repeater


@pytest.fixture
def sample_items():
    return ["a", "b", "c"]


@pytest.fixture
def success_func():
    """Function that just appends to a list (side effect tracker)."""
    calls = []

    def func(x):
        calls.append(x)

    func.calls = calls  # ty: ignore[unresolved-attribute]
    return func


@pytest.fixture
def error_func():
    """Function that raises ValueError for specific inputs."""

    def func(x):
        if x == "b":
            raise ValueError("bad item")
        return x

    return func


def test_repeater_runs_all_items(sample_items, success_func):
    repeater = Repeater(sample_items, success_func)
    repeater.run()

    assert success_func.calls == sample_items
    assert repeater.current_iteration == len(sample_items) - 1
    assert repeater.encountered_errors == {}


def test_repeater_on_exception_registers_handler(sample_items):
    repeater = Repeater(sample_items, lambda _: None)
    handler = Mock()
    repeater.onException(ValueError, handler)

    assert ValueError in repeater._handlers
    assert repeater._handlers[ValueError] is handler


def test_repeater_handles_registered_exception(sample_items, error_func):
    handler = Mock()

    repeater = Repeater(sample_items, error_func)
    repeater.onException(ValueError, handler)
    repeater.run()

    handler.assert_called_once()
    exc_arg, meta_arg = handler.call_args.args

    assert isinstance(exc_arg, ValueError)
    assert meta_arg is repeater
    assert 1 in repeater.encountered_errors  # index of "b"
    assert isinstance(repeater.encountered_errors[1], ValueError)


def test_repeater_multiple_handlers(sample_items):
    def func(x):
        if x == "a":
            raise ValueError("val")
        if x == "b":
            raise TypeError("type")
        return x

    h_val = Mock()
    h_type = Mock()

    repeater = Repeater(sample_items, func)
    repeater.onException(ValueError, h_val)
    repeater.onException(TypeError, h_type)
    repeater.run()

    h_val.assert_called_once()
    h_type.assert_called_once()
    assert len(repeater.encountered_errors) == 2
    assert all(
        isinstance(e, (ValueError, TypeError))
        for e in repeater.encountered_errors.values()
    )


def test_repeater_unhandled_exception_propagates(sample_items):
    def func(x):
        if x == "b":
            raise KeyError("boom")
        return x

    repeater = Repeater(sample_items, func)

    with pytest.raises(KeyError, match="boom"):
        repeater.run()

    # only processed first two items before error
    assert repeater.current_iteration == 1
    assert repeater.encountered_errors == {}


def test_repeater_handler_raises(sample_items, error_func):
    def bad_handler(exception, metadata):
        _ = exception
        _ = metadata
        raise RuntimeError("handler failed")

    repeater = Repeater(sample_items, error_func)
    repeater.onException(ValueError, bad_handler)

    with pytest.raises(RuntimeError, match="handler failed"):
        repeater.run()

    assert 1 in repeater.encountered_errors


def test_repeater_empty_items(success_func):
    repeater = Repeater([], success_func)
    repeater.run()

    assert success_func.calls == []
    assert repeater.encountered_errors == {}
    assert repeater.current_iteration == 0


def test_repeater_multiple_handled_exceptions():
    items = [1, 2, 3, 4]

    def func(x):
        if x % 2 == 0:
            raise ValueError(f"bad {x}")

    handler = Mock()
    repeater = Repeater(items, func)
    repeater.onException(ValueError, handler)
    repeater.run()

    assert len(repeater.encountered_errors) == 2  # 2 and 4 failed
    assert all(isinstance(e, ValueError) for e in repeater.encountered_errors.values())
    assert handler.call_count == 2
