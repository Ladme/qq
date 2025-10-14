# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import socket
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.batch.pbs.qqpbs import QQPBS
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.go.goer import QQGoer
from qq_lib.properties.states import RealState


def test_qqgoer_init_sets_info_file_and_calls_update():
    mock_path = Path("/fake/path/info.txt")

    with patch("qq_lib.go.goer.QQGoer._update") as mock_update:
        goer = QQGoer(mock_path)

        assert goer._info_file == mock_path
        mock_update.assert_called_once()


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
    goer = QQGoer.__new__(QQGoer)
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
    goer = QQGoer.__new__(QQGoer)
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
    goer = QQGoer.__new__(QQGoer)
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
    goer = QQGoer.__new__(QQGoer)
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
    goer = QQGoer.__new__(QQGoer)
    goer._state = state
    assert goer._isUnknownInconsistent() is expected


def test_qqgoer_set_destination_exists():
    goer = QQGoer.__new__(QQGoer)
    mock_informer = MagicMock()
    mock_informer.getDestination.return_value = ("host1", "/path/to/dir")
    goer._informer = mock_informer

    goer._setDestination()

    assert goer._host == "host1"
    assert goer._directory == "/path/to/dir"


def test_qqgoer_set_destination_none():
    goer = QQGoer.__new__(QQGoer)
    mock_informer = MagicMock()
    mock_informer.getDestination.return_value = None
    goer._informer = mock_informer

    goer._setDestination()

    assert goer._host is None
    assert goer._directory is None


def test_qqgoer_wait_queued():
    goer = QQGoer.__new__(QQGoer)
    goer._isQueued = MagicMock(side_effect=[True, True, False])
    goer._update = MagicMock()
    goer.ensureSuitable = MagicMock()

    with patch("qq_lib.go.goer.sleep") as mock_sleep:
        goer._waitQueued()

        # everything called twice (once per True in side_effect except last)
        assert mock_sleep.call_count == 2
        assert goer._update.call_count == 2
        assert goer.ensureSuitable.call_count == 2


def test_qqgoer_wait_queued_raises_not_suitable_error():
    goer = QQGoer.__new__(QQGoer)
    goer._isQueued = MagicMock(return_value=True)
    goer._update = MagicMock()
    goer.ensureSuitable = MagicMock(side_effect=QQNotSuitableError("not suitable"))

    with patch("qq_lib.go.goer.sleep"):
        with pytest.raises(QQNotSuitableError):
            goer._waitQueued()

        # ensure that _update is called at least once before exception
        goer._update.assert_called_once()


def test_qqgoer_is_in_work_dir_in_input_dir():
    goer = QQGoer.__new__(QQGoer)
    goer._directory = Path.cwd()
    goer._informer = MagicMock()
    goer._informer.useScratch.return_value = False
    goer._host = "irrelevant"

    assert goer._isInWorkDir() is True


def test_qqgoer_is_in_work_dir_shared_not_in_input_dir(tmp_path):
    goer = QQGoer.__new__(QQGoer)
    goer._directory = tmp_path
    goer._informer = MagicMock()
    goer._informer.useScratch.return_value = False
    goer._host = "irrelevant"

    assert goer._isInWorkDir() is False


def test_qqgoer_is_in_work_dir_directory_none():
    goer = QQGoer.__new__(QQGoer)
    goer._directory = None
    goer._informer = MagicMock()
    goer._host = socket.gethostname()

    assert goer._isInWorkDir() is False


def test_qqgoer_is_in_work_dir_scratch_host_mismatch():
    goer = QQGoer.__new__(QQGoer)
    goer._directory = Path.cwd()  # directory matches cwd
    goer._informer = MagicMock()
    goer._informer.useScratch.return_value = True
    goer._host = "otherhost"

    with patch("socket.gethostname", return_value="currenthost"):
        assert goer._isInWorkDir() is False


def test_qqgoer_is_in_work_dir_scratch_host_match():
    goer = QQGoer.__new__(QQGoer)
    goer._directory = Path.cwd()
    goer._informer = MagicMock()
    goer._informer.useScratch.return_value = True
    goer._host = socket.gethostname()

    assert goer._isInWorkDir() is True


def test_qqgoer_update():
    goer = QQGoer.__new__(QQGoer)
    mock_info_file = Path("/fake/info.txt")
    goer._info_file = mock_info_file
    goer._setDestination = MagicMock()

    mock_informer_instance = MagicMock()
    mock_informer_instance.info.batch_system = QQPBS
    mock_informer_instance.getRealState.return_value = RealState.RUNNING

    with patch(
        "qq_lib.go.goer.QQInformer.fromFile", return_value=mock_informer_instance
    ):
        goer._update()

        assert goer._informer == mock_informer_instance
        assert goer._batch_system == QQPBS
        assert goer._state == RealState.RUNNING
        goer._setDestination.assert_called_once()


def test_qqgoer_matches_job_returns_true():
    goer = QQGoer.__new__(QQGoer)
    goer._informer = MagicMock()
    goer._informer.isJob.return_value = True

    assert goer.matchesJob("12345") is True
    goer._informer.isJob.assert_called_once_with("12345")


def test_qqgoer_matches_job_returns_false():
    goer = QQGoer.__new__(QQGoer)
    goer._informer = MagicMock()
    goer._informer.isJob.return_value = False

    assert goer.matchesJob("99999") is False
    goer._informer.isJob.assert_called_once_with("99999")


def test_qqgoer_ensure_suitable_raises_finished():
    goer = QQGoer.__new__(QQGoer)
    goer._state = RealState.FINISHED

    with pytest.raises(
        QQNotSuitableError,
        match="Job has finished and was synchronized: working directory does not exist.",
    ):
        goer.ensureSuitable()


@pytest.mark.parametrize("destination", [(None, "host"), (Path("some/path"), None)])
def test_qqgoer_ensure_suitable_raises_killed_without_destination(destination):
    goer = QQGoer.__new__(QQGoer)
    goer._state = RealState.KILLED
    goer._directory, goer._host = destination

    with pytest.raises(
        QQNotSuitableError,
        match="Job has been killed and no working directory is available.",
    ):
        goer.ensureSuitable()


def test_qqgoer_ensure_suitable_passes_running():
    goer = QQGoer.__new__(QQGoer)
    goer._state = RealState.RUNNING

    goer.ensureSuitable()  # should not raise


def test_qqgoer_ensure_suitable_passes_killed_with_destination():
    goer = QQGoer.__new__(QQGoer)
    goer._state = RealState.KILLED
    goer._directory = Path("/some/path")
    goer._host = "host"

    goer.ensureSuitable()  # should not raise


def test_qqgoer_has_destination_returns_true_when_both_set():
    goer = QQGoer.__new__(QQGoer)
    goer._directory = Path("/some/dir")
    goer._host = "host1"

    assert goer.hasDestination() is True


def test_qqgoer_has_destination_returns_false_when_directory_none():
    goer = QQGoer.__new__(QQGoer)
    goer._directory = None
    goer._host = "host1"

    assert goer.hasDestination() is False


def test_qqgoer_has_destination_returns_false_when_host_none():
    goer = QQGoer.__new__(QQGoer)
    goer._directory = Path("/some/dir")
    goer._host = None

    assert goer.hasDestination() is False


def test_qqgoer_has_destination_returns_false_when_both_none():
    goer = QQGoer.__new__(QQGoer)
    goer._directory = None
    goer._host = None

    assert goer.hasDestination() is False


def test_qqgoer_print_info_prints_panel():
    killer = QQGoer.__new__(QQGoer)
    killer._informer = MagicMock()
    mock_console = MagicMock()
    mock_panel = MagicMock()

    with patch("qq_lib.go.goer.QQPresenter") as mock_presenter_cls:
        mock_presenter_instance = MagicMock()
        mock_presenter_cls.return_value = mock_presenter_instance
        mock_presenter_instance.createJobStatusPanel.return_value = mock_panel

        killer.printInfo(mock_console)

        mock_presenter_cls.assert_called_once_with(killer._informer)
        mock_presenter_instance.createJobStatusPanel.assert_called_once_with(
            mock_console
        )
        mock_console.print.assert_called_once_with(mock_panel)


def test_qqgoer_go_already_in_work_dir_logs_info_and_returns():
    goer = QQGoer.__new__(QQGoer)
    goer._batch_system = MagicMock()
    goer._isInWorkDir = MagicMock(return_value=True)

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        mock_logger.info.assert_called_once_with(
            "You are already in the working directory."
        )
        goer._batch_system.navigateToDestination.assert_not_called()


def test_qqgoer_go_killed_state_logs_warning_and_navigates():
    goer = QQGoer.__new__(QQGoer)
    goer._state = RealState.KILLED
    goer._directory = Path("/dir")
    goer._host = "host"
    goer._batch_system = MagicMock()
    goer._isInWorkDir = MagicMock(return_value=False)

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        mock_logger.warning.assert_called_once_with(
            "Job has been killed: working directory may no longer exist."
        )
        goer._batch_system.navigateToDestination.assert_called_once_with(
            goer._host, goer._directory
        )


def test_qqgoer_go_failed_state_logs_warning_and_navigates():
    goer = QQGoer.__new__(QQGoer)
    goer._state = RealState.FAILED
    goer._directory = Path("/dir")
    goer._host = "host"
    goer._batch_system = MagicMock()
    goer._isInWorkDir = MagicMock(return_value=False)

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        mock_logger.warning.assert_called_once_with(
            "Job has completed with an error code: working directory may no longer exist."
        )
        goer._batch_system.navigateToDestination.assert_called_once_with(
            goer._host, goer._directory
        )


@pytest.mark.parametrize(
    "state", [RealState.UNKNOWN, RealState.IN_AN_INCONSISTENT_STATE]
)
def test_qqgoer_go_unknown_inconsistent_logs_warning_and_navigates(state):
    goer = QQGoer.__new__(QQGoer)
    goer._state = state
    goer._directory = Path("/dir")
    goer._host = "host"
    goer._batch_system = MagicMock()
    goer._isInWorkDir = MagicMock(return_value=False)

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        mock_logger.warning.assert_called_once_with(
            "Job is in an unknown, unrecognized, or inconsistent state."
        )
        goer._batch_system.navigateToDestination.assert_called_once_with(
            goer._host, goer._directory
        )


@pytest.mark.parametrize(
    "state", [RealState.QUEUED, RealState.BOOTING, RealState.HELD, RealState.WAITING]
)
def test_qqgoer_go_queued_state_in_work_dir_calls_waitqueued_and_logs_info(state):
    goer = QQGoer.__new__(QQGoer)
    goer._state = state
    goer._directory = Path("/dir")
    goer._host = "host"
    goer._batch_system = MagicMock()
    goer._isInWorkDir = MagicMock(side_effect=[False, True])
    goer._waitQueued = MagicMock()

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        goer._waitQueued.assert_called_once()
        mock_logger.info.assert_called_with("You are already in the working directory.")
        goer._batch_system.navigateToDestination.assert_not_called()


@pytest.mark.parametrize(
    "state", [RealState.QUEUED, RealState.BOOTING, RealState.HELD, RealState.WAITING]
)
def test_qqgoer_go_queued_state_not_in_work_dir_navigates(state):
    goer = QQGoer.__new__(QQGoer)
    goer._state = state
    goer._directory = Path("/dir")
    goer._host = "host"
    goer._batch_system = MagicMock()
    goer._isInWorkDir = MagicMock(side_effect=[False, False])
    goer._waitQueued = MagicMock()

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        goer._waitQueued.assert_called_once()
        mock_logger.info.assert_called_with(
            f"Navigating to '{str(goer._directory)}' on '{goer._host}'."
        )
        goer._batch_system.navigateToDestination.assert_called_once_with(
            goer._host, goer._directory
        )


def test_qqgoer_go_no_destination_raises_error():
    goer = QQGoer.__new__(QQGoer)
    goer._state = RealState.RUNNING
    goer._directory = None
    goer._host = None
    goer._batch_system = MagicMock()
    goer._isInWorkDir = MagicMock(return_value=False)

    with (
        patch("qq_lib.go.goer.logger"),
        pytest.raises(
            QQError, match="Nowhere to go. Working directory or main node are not set."
        ),
    ):
        goer.go()


def test_qqgoer_go_navigates_when_suitable():
    goer = QQGoer.__new__(QQGoer)
    goer._state = RealState.RUNNING
    goer._directory = Path("/dir")
    goer._host = "host"
    goer._batch_system = MagicMock()
    goer._isInWorkDir = MagicMock(return_value=False)

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        mock_logger.info.assert_called_with(
            f"Navigating to '{str(goer._directory)}' on '{goer._host}'."
        )
        goer._batch_system.navigateToDestination.assert_called_once_with(
            goer._host, goer._directory
        )
