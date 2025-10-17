# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import socket
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.core.navigator import QQNavigator
from qq_lib.properties.states import RealState


def test_qqnavigator_init(tmp_path):
    info_file = tmp_path / "job.qqinfo"
    host = "example.host.org"

    with (
        patch("qq_lib.core.operator.QQOperator.__init__") as super_init,
        patch.object(QQNavigator, "_setDestination") as set_destination,
    ):
        QQNavigator(info_file, host)

    super_init.assert_called_once_with(info_file, host)
    set_destination.assert_called_once()


def test_qqnavigator_update_calls_super_and_set_destination():
    navigator = QQNavigator.__new__(QQNavigator)

    with (
        patch("qq_lib.core.operator.QQOperator.update") as super_update,
        patch.object(navigator, "_setDestination") as set_destination,
    ):
        navigator.update()

    super_update.assert_called_once_with()
    set_destination.assert_called_once()


@pytest.mark.parametrize(
    "work_dir, main_node",
    [
        (None, "main-node"),
        ("/work/dir", None),
        (None, None),
    ],
)
def test_qqnavigator_has_destination_false(work_dir, main_node):
    navigator = QQNavigator.__new__(QQNavigator)
    navigator._work_dir = work_dir
    navigator._main_node = main_node
    assert navigator.hasDestination() is False


def test_qqnavigator_has_destination_true():
    navigator = QQNavigator.__new__(QQNavigator)
    navigator._work_dir = "/work/dir"
    navigator._main_node = "main-node"
    assert navigator.hasDestination() is True


def test_qqnavigator_set_destination_with_value():
    navigator = QQNavigator.__new__(QQNavigator)
    navigator._informer = MagicMock()
    navigator._informer.getDestination.return_value = ("main-node", "/work/dir")

    navigator._setDestination()

    assert navigator._main_node == "main-node"
    assert navigator._work_dir == "/work/dir"


def test_qqnavigator_set_destination_none():
    navigator = QQNavigator.__new__(QQNavigator)
    navigator._informer = MagicMock()
    navigator._informer.getDestination.return_value = None

    navigator._setDestination()

    assert navigator._main_node is None
    assert navigator._work_dir is None


def test_qqnavigator_is_in_work_dir_in_input_dir():
    navigator = QQNavigator.__new__(QQNavigator)
    navigator._work_dir = Path.cwd()
    navigator._informer = MagicMock()
    navigator._informer.usesScratch.return_value = False
    navigator._main_node = "irrelevant"

    assert navigator._isInWorkDir() is True


def test_qqnavigator_is_in_work_dir_shared_not_in_input_dir(tmp_path):
    navigator = QQNavigator.__new__(QQNavigator)
    navigator._work_dir = tmp_path
    navigator._informer = MagicMock()
    navigator._informer.usesScratch.return_value = False
    navigator._main_node = "irrelevant"

    assert navigator._isInWorkDir() is False


def test_qqnavigator_is_in_work_dir_work_dir_none():
    navigator = QQNavigator.__new__(QQNavigator)
    navigator._work_dir = None
    navigator._informer = MagicMock()
    navigator._main_node = socket.gethostname()

    assert navigator._isInWorkDir() is False


def test_qqnavigator_is_in_work_dir_scratch_main_node_mismatch():
    navigator = QQNavigator.__new__(QQNavigator)
    navigator._work_dir = Path.cwd()
    navigator._informer = MagicMock()
    navigator._informer.usesScratch.return_value = True
    navigator._main_node = "otherhost"

    with patch("socket.gethostname", return_value="currenthost"):
        assert navigator._isInWorkDir() is False


def test_qqnavigator_is_in_work_dir_scratch_main_node_match():
    navigator = QQNavigator.__new__(QQNavigator)
    navigator._work_dir = Path.cwd()
    navigator._informer = MagicMock()
    navigator._informer.usesScratch.return_value = True
    navigator._main_node = socket.gethostname()

    assert navigator._isInWorkDir() is True


@pytest.mark.parametrize(
    "state,expected",
    [
        (RealState.QUEUED, True),
        (RealState.BOOTING, True),
        (RealState.HELD, True),
        (RealState.WAITING, True),
        (RealState.RUNNING, False),
    ],
)
def test_qqgoer_is_queued(state, expected):
    goer = QQNavigator.__new__(QQNavigator)
    goer._state = state
    assert goer._isQueued() is expected


@pytest.mark.parametrize(
    "state,job_exit_code,expected",
    [
        (RealState.KILLED, None, True),
        (RealState.KILLED, 0, True),
        (RealState.EXITING, None, True),
        (RealState.EXITING, 1, False),
        (RealState.FINISHED, None, False),
    ],
)
def test_qqgoer_is_killed(state, job_exit_code, expected):
    goer = QQNavigator.__new__(QQNavigator)
    goer._state = state
    goer._informer = MagicMock()
    goer._informer.info.job_exit_code = job_exit_code
    assert goer._isKilled() is expected


@pytest.mark.parametrize(
    "state,expected",
    [
        (RealState.FINISHED, True),
        (RealState.RUNNING, False),
    ],
)
def test_qqgoer_is_finished(state, expected):
    goer = QQNavigator.__new__(QQNavigator)
    goer._state = state
    assert goer._isFinished() is expected


@pytest.mark.parametrize(
    "state,expected",
    [
        (RealState.FAILED, True),
        (RealState.FINISHED, False),
    ],
)
def test_qqgoer_is_failed(state, expected):
    goer = QQNavigator.__new__(QQNavigator)
    goer._state = state
    assert goer._isFailed() is expected


@pytest.mark.parametrize(
    "state,expected",
    [
        (RealState.UNKNOWN, True),
        (RealState.IN_AN_INCONSISTENT_STATE, True),
        (RealState.RUNNING, False),
    ],
)
def test_qqgoer_is_unknown_inconsistent(state, expected):
    goer = QQNavigator.__new__(QQNavigator)
    goer._state = state
    assert goer._isUnknownInconsistent() is expected


@pytest.mark.parametrize(
    "state,job_exit_code,expected",
    [
        (RealState.EXITING, 0, True),
        (RealState.EXITING, 1, False),
        (RealState.RUNNING, 0, False),
        (RealState.FINISHED, 0, False),
        (RealState.KILLED, 0, False),
    ],
)
def test_qqgoer_is_exiting_successfully(state, job_exit_code, expected):
    goer = QQNavigator.__new__(QQNavigator)
    goer._state = state
    goer._informer = MagicMock()
    goer._informer.info.job_exit_code = job_exit_code
    assert goer._isExitingSuccessfully() is expected
