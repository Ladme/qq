# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import pytest

from qq_lib.properties.states import BatchState, NaiveState, RealState


@pytest.mark.parametrize(
    "input_str,expected_state",
    [
        ("queued", NaiveState.QUEUED),
        ("QUEUED", NaiveState.QUEUED),
        ("running", NaiveState.RUNNING),
        ("RUNNING", NaiveState.RUNNING),
        ("failed", NaiveState.FAILED),
        ("FAILED", NaiveState.FAILED),
        ("finished", NaiveState.FINISHED),
        ("FINISHED", NaiveState.FINISHED),
        ("killed", NaiveState.KILLED),
        ("KILLED", NaiveState.KILLED),
        ("unknown", NaiveState.UNKNOWN),
        ("UNKNOWN", NaiveState.UNKNOWN),
        ("nonexistent", NaiveState.UNKNOWN),
        ("", NaiveState.UNKNOWN),
        ("random", NaiveState.UNKNOWN),
    ],
)
def test_naive_state_from_str(input_str, expected_state):
    assert NaiveState.fromStr(input_str) == expected_state


@pytest.mark.parametrize(
    "code,expected_state",
    [
        ("E", BatchState.EXITING),
        ("H", BatchState.HELD),
        ("Q", BatchState.QUEUED),
        ("R", BatchState.RUNNING),
        ("T", BatchState.MOVING),
        ("W", BatchState.WAITING),
        ("S", BatchState.SUSPENDED),
        ("F", BatchState.FINISHED),
        ("X", BatchState.FAILED),
        ("e", BatchState.EXITING),
        ("g", BatchState.UNKNOWN),
        ("", BatchState.UNKNOWN),
    ],
)
def test_batch_state_from_code(code, expected_state):
    assert BatchState.fromCode(code) == expected_state


@pytest.mark.parametrize(
    "state,expected_code",
    [
        (BatchState.EXITING, "E"),
        (BatchState.HELD, "H"),
        (BatchState.QUEUED, "Q"),
        (BatchState.RUNNING, "R"),
        (BatchState.MOVING, "T"),
        (BatchState.WAITING, "W"),
        (BatchState.SUSPENDED, "S"),
        (BatchState.FINISHED, "F"),
        (BatchState.FAILED, "X"),
        (BatchState.UNKNOWN, "?"),
    ],
)
def test_batch_state_to_code(state, expected_code):
    assert state.toCode() == expected_code


@pytest.mark.parametrize(
    "naive_state,batch_state,expected_state",
    [
        # UNKNOWN naive state - always UNKNOWN
        (NaiveState.UNKNOWN, BatchState.QUEUED, RealState.UNKNOWN),
        (NaiveState.UNKNOWN, BatchState.FINISHED, RealState.UNKNOWN),
        # QUEUED naive state
        (NaiveState.QUEUED, BatchState.QUEUED, RealState.QUEUED),
        (NaiveState.QUEUED, BatchState.MOVING, RealState.QUEUED),
        (NaiveState.QUEUED, BatchState.HELD, RealState.HELD),
        (NaiveState.QUEUED, BatchState.SUSPENDED, RealState.SUSPENDED),
        (NaiveState.QUEUED, BatchState.WAITING, RealState.WAITING),
        (NaiveState.QUEUED, BatchState.RUNNING, RealState.BOOTING),
        (NaiveState.QUEUED, BatchState.EXITING, RealState.IN_AN_INCONSISTENT_STATE),
        (NaiveState.QUEUED, BatchState.FAILED, RealState.IN_AN_INCONSISTENT_STATE),
        (NaiveState.QUEUED, BatchState.UNKNOWN, RealState.IN_AN_INCONSISTENT_STATE),
        # RUNNING naive state
        (NaiveState.RUNNING, BatchState.RUNNING, RealState.RUNNING),
        (NaiveState.RUNNING, BatchState.SUSPENDED, RealState.SUSPENDED),
        (NaiveState.RUNNING, BatchState.EXITING, RealState.IN_AN_INCONSISTENT_STATE),
        (NaiveState.RUNNING, BatchState.QUEUED, RealState.IN_AN_INCONSISTENT_STATE),
        (NaiveState.RUNNING, BatchState.HELD, RealState.IN_AN_INCONSISTENT_STATE),
        (NaiveState.RUNNING, BatchState.FINISHED, RealState.IN_AN_INCONSISTENT_STATE),
        (NaiveState.RUNNING, BatchState.FAILED, RealState.IN_AN_INCONSISTENT_STATE),
        (NaiveState.RUNNING, BatchState.UNKNOWN, RealState.IN_AN_INCONSISTENT_STATE),
        # KILLED naive state
        (NaiveState.KILLED, BatchState.RUNNING, RealState.EXITING),
        (NaiveState.KILLED, BatchState.EXITING, RealState.EXITING),
        (NaiveState.KILLED, BatchState.QUEUED, RealState.KILLED),
        (NaiveState.KILLED, BatchState.FINISHED, RealState.KILLED),
        (NaiveState.KILLED, BatchState.FAILED, RealState.KILLED),
        # FINISHED naive state
        (NaiveState.FINISHED, BatchState.EXITING, RealState.EXITING),
        (NaiveState.FINISHED, BatchState.RUNNING, RealState.EXITING),
        (NaiveState.FINISHED, BatchState.QUEUED, RealState.IN_AN_INCONSISTENT_STATE),
        (NaiveState.FINISHED, BatchState.HELD, RealState.IN_AN_INCONSISTENT_STATE),
        (NaiveState.FINISHED, BatchState.WAITING, RealState.IN_AN_INCONSISTENT_STATE),
        (NaiveState.FINISHED, BatchState.FAILED, RealState.IN_AN_INCONSISTENT_STATE),
        (NaiveState.FINISHED, BatchState.FINISHED, RealState.FINISHED),
        (NaiveState.FINISHED, BatchState.UNKNOWN, RealState.FINISHED),
        # FAILED naive state
        (NaiveState.FAILED, BatchState.EXITING, RealState.EXITING),
        (NaiveState.FAILED, BatchState.RUNNING, RealState.EXITING),
        (NaiveState.FAILED, BatchState.QUEUED, RealState.IN_AN_INCONSISTENT_STATE),
        (NaiveState.FAILED, BatchState.HELD, RealState.IN_AN_INCONSISTENT_STATE),
        (NaiveState.FAILED, BatchState.WAITING, RealState.IN_AN_INCONSISTENT_STATE),
        (NaiveState.FAILED, BatchState.FINISHED, RealState.IN_AN_INCONSISTENT_STATE),
        (NaiveState.FAILED, BatchState.FAILED, RealState.FAILED),
        (NaiveState.FAILED, BatchState.UNKNOWN, RealState.FAILED),
    ],
)
def test_real_state_from_states(naive_state, batch_state, expected_state):
    result = RealState.fromStates(naive_state, batch_state)
    assert result == expected_state
