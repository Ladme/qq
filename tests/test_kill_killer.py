# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import stat
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.core.error import QQNotSuitableError
from qq_lib.kill.killer import QQKiller
from qq_lib.properties.states import RealState


def test_qqkiller_lock_file_removes_write_permissions():
    with tempfile.NamedTemporaryFile() as tmp_file:
        file_path = Path(tmp_file.name)
        # set initial permissions
        file_path.chmod(
            stat.S_IRUSR
            | stat.S_IWUSR
            | stat.S_IRGRP
            | stat.S_IWGRP
            | stat.S_IROTH
            | stat.S_IWOTH
        )

        killer = QQKiller.__new__(QQKiller)
        killer._lockFile(file_path)

        new_mode = file_path.stat().st_mode

        # all write permissions removed
        assert not (new_mode & stat.S_IWUSR)
        assert not (new_mode & stat.S_IWGRP)
        assert not (new_mode & stat.S_IWOTH)

        # read permissions are intact
        assert new_mode & stat.S_IRUSR


@pytest.mark.parametrize(
    "state,expected",
    [
        (RealState.SUSPENDED, True),
        (RealState.RUNNING, False),
        (RealState.KILLED, False),
    ],
)
def test_qqkiller_is_suspended_returns_correctly(state, expected):
    killer = QQKiller.__new__(QQKiller)
    killer._state = state
    assert killer._isSuspended() is expected


@pytest.mark.parametrize(
    "state,expected",
    [
        (RealState.QUEUED, True),
        (RealState.HELD, True),
        (RealState.WAITING, True),
        (RealState.BOOTING, True),
        (RealState.RUNNING, False),
    ],
)
def test_qqkiller_is_queued_returns_correctly(state, expected):
    killer = QQKiller.__new__(QQKiller)
    killer._state = state
    assert killer._isQueued() is expected


@pytest.mark.parametrize(
    "state,exit,expected",
    [
        (RealState.KILLED, None, True),
        (RealState.EXITING, None, True),
        (RealState.EXITING, 0, False),
        (RealState.EXITING, 1, False),
        (RealState.FAILED, 1, False),
    ],
)
def test_qqkiller_is_killed_returns_correctly(state, exit, expected):
    killer = QQKiller.__new__(QQKiller)
    killer._state = state
    killer._informer = MagicMock()
    killer._informer.info.job_exit_code = exit
    assert killer._isKilled() is expected


@pytest.mark.parametrize(
    "state,expected",
    [
        (RealState.FINISHED, True),
        (RealState.FAILED, True),
        (RealState.RUNNING, False),
    ],
)
def test_qqkiller_is_completed_returns_correctly(state, expected):
    killer = QQKiller.__new__(QQKiller)
    killer._state = state
    assert killer._isCompleted() is expected


@pytest.mark.parametrize(
    "state,expected",
    [
        (RealState.EXITING, True),
        (RealState.RUNNING, False),
    ],
)
def test_qqkiller_is_exiting_returns_correctly(state, expected):
    killer = QQKiller.__new__(QQKiller)
    killer._state = state
    assert killer._isExiting() is expected


@pytest.mark.parametrize(
    "state,expected",
    [
        (RealState.UNKNOWN, True),
        (RealState.IN_AN_INCONSISTENT_STATE, True),
        (RealState.RUNNING, False),
    ],
)
def test_qqkiller_is_unknown_inconsistent_returns_correctly(state, expected):
    killer = QQKiller.__new__(QQKiller)
    killer._state = state
    assert killer._isUnknownInconsistent() is expected


def test_qqkiller_update_info_file_calls_informer_and_locks_file():
    mock_file = Path("/tmp/fake_info_file.txt")
    killer = QQKiller.__new__(QQKiller)
    killer._info_file = mock_file

    mock_informer = MagicMock()
    killer._informer = mock_informer

    with (
        patch.object(killer, "_lockFile") as mock_lock,
        patch("qq_lib.kill.killer.datetime") as mock_datetime,
    ):
        mock_datetime.now.return_value = datetime(2025, 1, 1)

        killer._updateInfoFile()

        mock_informer.setKilled.assert_called_once_with(datetime(2025, 1, 1))
        mock_informer.toFile.assert_called_once_with(mock_file)
        mock_lock.assert_called_once_with(mock_file)


@pytest.mark.parametrize(
    "state,exit,force,expected",
    [
        (RealState.QUEUED, None, False, True),
        (RealState.QUEUED, None, True, True),
        (RealState.HELD, None, False, True),
        (RealState.HELD, None, True, True),
        (RealState.WAITING, None, False, True),
        (RealState.WAITING, None, True, True),
        (RealState.BOOTING, None, False, True),
        (RealState.BOOTING, None, True, True),
        (RealState.SUSPENDED, None, False, True),
        (RealState.SUSPENDED, None, True, True),
        (RealState.RUNNING, None, False, False),
        (RealState.RUNNING, None, True, True),
        (RealState.FINISHED, 0, False, False),
        (RealState.FINISHED, 0, True, False),
        (RealState.FAILED, 1, False, False),
        (RealState.FAILED, 1, True, False),
        (RealState.KILLED, None, False, False),
        (RealState.KILLED, None, True, False),
        (RealState.UNKNOWN, None, False, False),
        (RealState.UNKNOWN, None, True, False),
        (RealState.EXITING, 0, False, False),
        (RealState.EXITING, 0, True, True),
        (RealState.EXITING, 1, False, False),
        (RealState.EXITING, 1, True, True),
        (RealState.EXITING, None, False, False),
        (RealState.EXITING, None, True, False),
        (RealState.IN_AN_INCONSISTENT_STATE, None, False, False),
        (RealState.IN_AN_INCONSISTENT_STATE, None, True, False),
    ],
)
def test_qqkiller_should_update_info_file_all_combinations_manual(
    state, exit, force, expected
):
    killer = QQKiller.__new__(QQKiller)
    killer._state = state
    killer._informer = MagicMock()
    killer._informer.info.job_exit_code = exit
    assert killer._shouldUpdateInfoFile(force) is expected


def test_qqkiller_terminate_normal_updates_info_file():
    killer = QQKiller.__new__(QQKiller)
    killer._shouldUpdateInfoFile = MagicMock(return_value=True)
    killer._updateInfoFile = MagicMock()
    killer._batch_system = MagicMock()
    killer._informer = MagicMock()
    killer._informer.info.job_id = "1234"

    job_id = killer.terminate(force=False)

    assert job_id == "1234"
    killer._shouldUpdateInfoFile.assert_called_once_with(False)
    killer._batch_system.jobKill.assert_called_once_with("1234")
    killer._batch_system.jobKillForce.assert_not_called()
    killer._updateInfoFile.assert_called_once()


def test_qqkiller_terminate_force_updates_info_file():
    killer = QQKiller.__new__(QQKiller)
    killer._shouldUpdateInfoFile = MagicMock(return_value=True)
    killer._updateInfoFile = MagicMock()
    killer._batch_system = MagicMock()
    killer._informer = MagicMock()
    killer._informer.info.job_id = "5678"

    job_id = killer.terminate(force=True)

    assert job_id == "5678"
    killer._shouldUpdateInfoFile.assert_called_once_with(True)
    killer._batch_system.jobKillForce.assert_called_once_with("5678")
    killer._batch_system.jobKill.assert_not_called()
    killer._updateInfoFile.assert_called_once()


def test_qqkiller_terminate_does_not_update_info_file():
    killer = QQKiller.__new__(QQKiller)
    killer._shouldUpdateInfoFile = MagicMock(return_value=False)
    killer._updateInfoFile = MagicMock()
    killer._batch_system = MagicMock()
    killer._informer = MagicMock()
    killer._informer.info.job_id = "91011"

    job_id = killer.terminate(force=False)

    assert job_id == "91011"
    killer._shouldUpdateInfoFile.assert_called_once_with(False)
    killer._batch_system.jobKill.assert_called_once_with("91011")
    killer._updateInfoFile.assert_not_called()


@pytest.mark.parametrize(
    "state,exit,expected_message",
    [
        (RealState.FINISHED, 0, "Job cannot be terminated. Job is already completed."),
        (RealState.FAILED, 1, "Job cannot be terminated. Job is already completed."),
        (
            RealState.KILLED,
            None,
            "Job cannot be terminated. Job has already been killed.",
        ),
        (
            RealState.EXITING,
            None,
            "Job cannot be terminated. Job has already been killed.",
        ),
        (RealState.EXITING, 0, "Job cannot be terminated. Job is in an exiting state."),
        (RealState.EXITING, 1, "Job cannot be terminated. Job is in an exiting state."),
    ],
)
def test_qqkiller_ensure_suitable_raises(state, exit, expected_message):
    killer = QQKiller.__new__(QQKiller)
    killer._state = state
    killer._informer = MagicMock()
    killer._informer.info.job_exit_code = exit
    with pytest.raises(QQNotSuitableError, match=expected_message):
        killer.ensureSuitable()


@pytest.mark.parametrize(
    "state",
    [
        RealState.RUNNING,
        RealState.SUSPENDED,
        RealState.QUEUED,
        RealState.WAITING,
        RealState.BOOTING,
        RealState.HELD,
        RealState.UNKNOWN,
        RealState.IN_AN_INCONSISTENT_STATE,
    ],
)
def test_qqkiller_ensure_suitable_passes(state):
    killer = QQKiller.__new__(QQKiller)
    killer._state = state
    killer.ensureSuitable()
