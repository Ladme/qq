# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import stat
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.batch.pbs.qqpbs import QQPBS
from qq_lib.core.error import QQNotSuitableError
from qq_lib.kill.killer import QQKiller
from qq_lib.properties.states import RealState


def test_qqkiller_init():
    mock_path = Path("/fake/path/info.txt")

    with patch("qq_lib.kill.killer.QQInformer.fromFile") as mock_from_file:
        mock_informer_instance = MagicMock()
        mock_informer_instance.batch_system = QQPBS
        mock_informer_instance.getRealState.return_value = RealState.RUNNING
        mock_from_file.return_value = mock_informer_instance

        killer = QQKiller(mock_path)

        assert killer._info_file == mock_path
        assert killer._informer == mock_informer_instance
        assert killer._batch_system == QQPBS
        assert killer._state == RealState.RUNNING
        mock_from_file.assert_called_once_with(mock_path)


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
    "state,expected",
    [
        (RealState.KILLED, True),
        (RealState.FAILED, False),
    ],
)
def test_qqkiller_is_killed_returns_correctly(state, expected):
    killer = QQKiller.__new__(QQKiller)
    killer._state = state
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
    "state,force,expected",
    [
        (RealState.QUEUED, False, True),
        (RealState.QUEUED, True, True),
        (RealState.HELD, False, True),
        (RealState.HELD, True, True),
        (RealState.WAITING, False, True),
        (RealState.WAITING, True, True),
        (RealState.BOOTING, False, True),
        (RealState.BOOTING, True, True),
        (RealState.SUSPENDED, False, True),
        (RealState.SUSPENDED, True, True),
        (RealState.RUNNING, False, False),
        (RealState.RUNNING, True, True),
        (RealState.FINISHED, False, False),
        (RealState.FINISHED, True, False),
        (RealState.FAILED, False, False),
        (RealState.FAILED, True, False),
        (RealState.KILLED, False, False),
        (RealState.KILLED, True, False),
        (RealState.UNKNOWN, False, False),
        (RealState.UNKNOWN, True, False),
        (RealState.EXITING, False, False),
        (RealState.EXITING, True, True),
        (RealState.IN_AN_INCONSISTENT_STATE, False, False),
        (RealState.IN_AN_INCONSISTENT_STATE, True, False),
    ],
)
def test_qqkiller_should_update_info_file_all_combinations_manual(
    state, force, expected
):
    killer = QQKiller.__new__(QQKiller)
    killer._state = state
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


def test_qqkiller_matches_job_returns_true():
    killer = QQKiller.__new__(QQKiller)
    killer._informer = MagicMock()
    killer._informer.matchesJob.return_value = True

    assert killer.matchesJob("12345") is True
    killer._informer.matchesJob.assert_called_once_with("12345")


def test_qqkiller_matches_job_returns_false():
    killer = QQKiller.__new__(QQKiller)
    killer._informer = MagicMock()
    killer._informer.matchesJob.return_value = False

    assert killer.matchesJob("99999") is False
    killer._informer.matchesJob.assert_called_once_with("99999")


@pytest.mark.parametrize(
    "state,expected_message",
    [
        (RealState.FINISHED, "Job cannot be terminated. Job is already completed."),
        (RealState.FAILED, "Job cannot be terminated. Job is already completed."),
        (RealState.KILLED, "Job cannot be terminated. Job has already been killed."),
        (RealState.EXITING, "Job cannot be terminated. Job is in an exiting state."),
    ],
)
def test_qqkiller_ensure_suitable_raises(state, expected_message):
    killer = QQKiller.__new__(QQKiller)
    killer._state = state
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


def test_qqkiller_print_info_prints_panel():
    killer = QQKiller.__new__(QQKiller)
    killer._informer = MagicMock()
    mock_console = MagicMock()
    mock_panel = MagicMock()

    with patch("qq_lib.kill.killer.QQPresenter") as mock_presenter_cls:
        mock_presenter_instance = MagicMock()
        mock_presenter_cls.return_value = mock_presenter_instance
        mock_presenter_instance.createJobStatusPanel.return_value = mock_panel

        killer.printInfo(mock_console)

        mock_presenter_cls.assert_called_once_with(killer._informer)
        mock_presenter_instance.createJobStatusPanel.assert_called_once_with(
            mock_console
        )
        mock_console.print.assert_called_once_with(mock_panel)
