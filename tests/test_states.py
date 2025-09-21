# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from datetime import datetime, timedelta

import pytest

from qq_lib.states import BatchState, NaiveState, QQState


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
def test_naive_state_fromStr(input_str, expected_state):
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
        ("e", BatchState.EXITING),
        ("x", BatchState.UNKNOWN),
        ("", BatchState.UNKNOWN),
    ],
)
def test_batch_state_fromCode(code, expected_state):
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
        (BatchState.UNKNOWN, "?"),
    ],
)
def test_batch_state_toCode(state, expected_code):
    assert state.toCode() == expected_code


@pytest.mark.parametrize(
    "naive_state,batch_state,expected_state",
    [
        # UNKNOWN naive state - always UNKNOWN
        (NaiveState.UNKNOWN, BatchState.QUEUED, QQState.UNKNOWN),
        (NaiveState.UNKNOWN, BatchState.FINISHED, QQState.UNKNOWN),
        # QUEUED naive state
        (NaiveState.QUEUED, BatchState.QUEUED, QQState.QUEUED),
        (NaiveState.QUEUED, BatchState.MOVING, QQState.QUEUED),
        (NaiveState.QUEUED, BatchState.HELD, QQState.HELD),
        (NaiveState.QUEUED, BatchState.SUSPENDED, QQState.SUSPENDED),
        (NaiveState.QUEUED, BatchState.WAITING, QQState.WAITING),
        (NaiveState.QUEUED, BatchState.RUNNING, QQState.BOOTING),
        (NaiveState.QUEUED, BatchState.EXITING, QQState.IN_AN_INCONSISTENT_STATE),
        (NaiveState.QUEUED, BatchState.UNKNOWN, QQState.IN_AN_INCONSISTENT_STATE),
        # RUNNING naive state
        (NaiveState.RUNNING, BatchState.RUNNING, QQState.RUNNING),
        (NaiveState.RUNNING, BatchState.SUSPENDED, QQState.SUSPENDED),
        (NaiveState.RUNNING, BatchState.EXITING, QQState.IN_AN_INCONSISTENT_STATE),
        (NaiveState.RUNNING, BatchState.QUEUED, QQState.IN_AN_INCONSISTENT_STATE),
        (NaiveState.RUNNING, BatchState.HELD, QQState.IN_AN_INCONSISTENT_STATE),
        (NaiveState.RUNNING, BatchState.FINISHED, QQState.IN_AN_INCONSISTENT_STATE),
        (NaiveState.RUNNING, BatchState.UNKNOWN, QQState.IN_AN_INCONSISTENT_STATE),
        # KILLED naive state - always KILLED
        (NaiveState.KILLED, BatchState.QUEUED, QQState.KILLED),
        (NaiveState.KILLED, BatchState.FINISHED, QQState.KILLED),
        # FINISHED naive state - always FINISHED
        (NaiveState.FINISHED, BatchState.QUEUED, QQState.FINISHED),
        (NaiveState.FINISHED, BatchState.RUNNING, QQState.FINISHED),
        (NaiveState.FINISHED, BatchState.EXITING, QQState.FINISHED),
        # FAILED naive state - always FAILED
        (NaiveState.FAILED, BatchState.RUNNING, QQState.FAILED),
        (NaiveState.FAILED, BatchState.UNKNOWN, QQState.FAILED),
    ],
)
def test_qqstate_fromStates(naive_state, batch_state, expected_state):
    result = QQState.fromStates(naive_state, batch_state)
    assert result == expected_state


@pytest.mark.parametrize(
    "state,expected_first_keyword,expected_second_keyword",
    [
        (QQState.QUEUED, "queued", "queue"),
        (QQState.HELD, "held", "queue"),
        (QQState.SUSPENDED, "suspended", ""),
        (QQState.WAITING, "waiting", "queue"),
        (QQState.RUNNING, "running", "running"),
        (QQState.BOOTING, "booting", "preparing"),
        (QQState.KILLED, "killed", "killed"),
        (QQState.FAILED, "failed", "failed"),
        (QQState.FINISHED, "finished", "completed"),
        (QQState.IN_AN_INCONSISTENT_STATE, "inconsistent", "disagree"),
        (QQState.UNKNOWN, "unknown", "does not recognize"),
    ],
)
def test_qqstate_info_keywords(state, expected_first_keyword, expected_second_keyword):
    start_time = datetime.now()
    end_time = start_time + timedelta(seconds=3600)
    return_code = 1
    node = "node1"

    first, second = state.info(start_time, end_time, return_code, node)

    assert expected_first_keyword.lower() in first.lower()
    assert expected_second_keyword.lower() in second.lower()
