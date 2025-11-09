# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.go.goer import Goer
from qq_lib.properties.states import RealState


def test_goer_wait_queued():
    goer = Goer.__new__(Goer)
    goer._isQueued = MagicMock(side_effect=[True, True, False])
    goer.update = MagicMock()
    goer.ensureSuitable = MagicMock()

    with patch("qq_lib.go.goer.sleep") as mock_sleep:
        goer._waitQueued()

        # everything called twice (once per True in side_effect except last)
        assert mock_sleep.call_count == 2
        assert goer.update.call_count == 2
        assert goer.ensureSuitable.call_count == 2


def test_goer_wait_queued_raises_not_suitable_error():
    goer = Goer.__new__(Goer)
    goer._isQueued = MagicMock(return_value=True)
    goer.update = MagicMock()
    goer.ensureSuitable = MagicMock(side_effect=QQNotSuitableError("not suitable"))

    with patch("qq_lib.go.goer.sleep"):
        with pytest.raises(QQNotSuitableError):
            goer._waitQueued()

        # ensure that update is called at least once before exception
        goer.update.assert_called_once()


def test_goer_ensure_suitable_raises_finished():
    goer = Goer.__new__(Goer)
    goer._state = RealState.FINISHED

    with pytest.raises(
        QQNotSuitableError,
        match="Job has finished and was synchronized: working directory no longer exists.",
    ):
        goer.ensureSuitable()


def test_goer_ensure_suitable_raises_exiting_successfully():
    goer = Goer.__new__(Goer)
    goer._state = RealState.EXITING
    goer._informer = MagicMock()
    goer._informer.info.job_exit_code = 0

    with pytest.raises(
        QQNotSuitableError,
        match="Job is finishing successfully: working directory no longer exists.",
    ):
        goer.ensureSuitable()


@pytest.mark.parametrize(
    "destination", [(None, "host"), (Path("some/path"), None), (None, None)]
)
def test_goer_ensure_suitable_raises_killed_without_destination(destination):
    goer = Goer.__new__(Goer)
    goer._state = RealState.KILLED
    goer._work_dir, goer._main_node = destination

    with pytest.raises(
        QQNotSuitableError,
        match="Job has been killed and no working directory has been created.",
    ):
        goer.ensureSuitable()


def test_goer_ensure_suitable_passes_running():
    goer = Goer.__new__(Goer)
    goer._state = RealState.RUNNING

    goer.ensureSuitable()  # should not raise


def test_goer_ensure_suitable_passes_killed_with_destination():
    goer = Goer.__new__(Goer)
    goer._state = RealState.KILLED
    goer._work_dir = Path("/some/path")
    goer._main_node = "host"

    goer.ensureSuitable()  # should not raise


def test_goer_go_already_in_work_dir_logs_info_and_returns():
    goer = Goer.__new__(Goer)
    goer._batch_system = MagicMock()
    goer._isInWorkDir = MagicMock(return_value=True)

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        mock_logger.info.assert_called_once_with(
            "You are already in the working directory."
        )
        goer._batch_system.navigateToDestination.assert_not_called()


def test_goer_go_killed_state_logs_warning_and_navigates():
    goer = Goer.__new__(Goer)
    goer._state = RealState.KILLED
    goer._work_dir = Path("/dir")
    goer._main_node = "host"
    goer._batch_system = MagicMock()
    goer._isInWorkDir = MagicMock(return_value=False)

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        mock_logger.warning.assert_called_once_with(
            "Job has been killed: working directory may no longer exist."
        )
        goer._batch_system.navigateToDestination.assert_called_once_with(
            goer._main_node, goer._work_dir
        )


def test_goer_go_failed_state_logs_warning_and_navigates():
    goer = Goer.__new__(Goer)
    goer._state = RealState.FAILED
    goer._work_dir = Path("/dir")
    goer._main_node = "host"
    goer._batch_system = MagicMock()
    goer._isInWorkDir = MagicMock(return_value=False)

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        mock_logger.warning.assert_called_once_with(
            "Job has completed with an error code: working directory may no longer exist."
        )
        goer._batch_system.navigateToDestination.assert_called_once_with(
            goer._main_node, goer._work_dir
        )


@pytest.mark.parametrize(
    "state", [RealState.UNKNOWN, RealState.IN_AN_INCONSISTENT_STATE]
)
def test_goer_go_unknown_inconsistent_logs_warning_and_navigates(state):
    goer = Goer.__new__(Goer)
    goer._state = state
    goer._work_dir = Path("/dir")
    goer._main_node = "host"
    goer._batch_system = MagicMock()
    goer._isInWorkDir = MagicMock(return_value=False)

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        mock_logger.warning.assert_called_once_with(
            "Job is in an unknown, unrecognized, or inconsistent state."
        )
        goer._batch_system.navigateToDestination.assert_called_once_with(
            goer._main_node, goer._work_dir
        )


@pytest.mark.parametrize(
    "state", [RealState.QUEUED, RealState.BOOTING, RealState.HELD, RealState.WAITING]
)
def test_goer_go_queued_state_in_work_dir_calls_waitqueued_and_logs_info(state):
    goer = Goer.__new__(Goer)
    goer._state = state
    goer._work_dir = Path("/dir")
    goer._main_node = "host"
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
def test_goer_go_queued_state_not_in_work_dir_navigates(state):
    goer = Goer.__new__(Goer)
    goer._state = state
    goer._work_dir = Path("/dir")
    goer._main_node = "host"
    goer._batch_system = MagicMock()
    goer._isInWorkDir = MagicMock(side_effect=[False, False])
    goer._waitQueued = MagicMock()

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        goer._waitQueued.assert_called_once()
        mock_logger.info.assert_called_with(
            f"Navigating to '{str(goer._work_dir)}' on '{goer._main_node}'."
        )
        goer._batch_system.navigateToDestination.assert_called_once_with(
            goer._main_node, goer._work_dir
        )


def test_goer_go_no_destination_raises_error():
    goer = Goer.__new__(Goer)
    goer._state = RealState.RUNNING
    goer._work_dir = None
    goer._main_node = None
    goer._batch_system = MagicMock()
    goer._isInWorkDir = MagicMock(return_value=False)

    with (
        patch("qq_lib.go.goer.logger"),
        pytest.raises(
            QQError,
            match="Host \\('main_node'\\) or working directory \\('work_dir'\\) are not defined.",
        ),
    ):
        goer.go()


def test_goer_go_navigates_when_suitable():
    goer = Goer.__new__(Goer)
    goer._state = RealState.RUNNING
    goer._work_dir = Path("/dir")
    goer._main_node = "host"
    goer._batch_system = MagicMock()
    goer._isInWorkDir = MagicMock(return_value=False)

    with patch("qq_lib.go.goer.logger") as mock_logger:
        goer.go()
        mock_logger.info.assert_called_with(
            f"Navigating to '{str(goer._work_dir)}' on '{goer._main_node}'."
        )
        goer._batch_system.navigateToDestination.assert_called_once_with(
            goer._main_node, goer._work_dir
        )
