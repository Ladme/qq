# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.properties.states import RealState
from qq_lib.wipe.wiper import Wiper


@pytest.mark.parametrize(
    "state, job_exit_code, expected_message",
    [
        (
            RealState.QUEUED,
            None,
            "Job is queued and does not have a working directory yet",
        ),
        (
            RealState.BOOTING,
            None,
            "Job is booting and does not have a working directory yet",
        ),
        (
            RealState.HELD,
            None,
            "Job is held and does not have a working directory yet",
        ),
        (
            RealState.WAITING,
            None,
            "Job is waiting and does not have a working directory yet",
        ),
        (
            RealState.RUNNING,
            None,
            "Job is running. It is not safe to delete the working directory",
        ),
        (
            RealState.SUSPENDED,
            None,
            "Job is suspended. It is not safe to delete the working directory",
        ),
        (
            RealState.FINISHED,
            None,
            "Job has finished and was synchronized: working directory no longer exists",
        ),
        (
            RealState.EXITING,
            0,
            "Job is finishing successfully: working directory no longer exists",
        ),
    ],
)
def test_wiper_ensure_suitable_raises_for_disallowed_states(
    state, job_exit_code, expected_message
):
    wiper = Wiper.__new__(Wiper)
    wiper._state = state
    wiper._informer = MagicMock()
    wiper._informer.info.job_exit_code = job_exit_code
    wiper._informer.getRealState.return_value = state

    wiper._work_dir = Path("/workdir")
    wiper._main_node = "node"
    wiper._input_machine = "input"
    wiper._informer.info.input_dir = Path("/different_input")
    wiper._informer.usesScratch.return_value = False
    wiper._batch_system = MagicMock()

    with pytest.raises(QQNotSuitableError, match=expected_message):
        wiper.ensureSuitable()


def test_wiper_ensure_suitable_raises_when_destination_missing():
    wiper = Wiper.__new__(Wiper)
    wiper._state = RealState.FAILED
    wiper._informer = MagicMock()
    wiper._informer.info.job_exit_code = 1
    wiper._informer.getRealState.return_value = RealState.FAILED

    wiper._work_dir = None
    wiper._main_node = None
    wiper._input_machine = "input"
    wiper._informer.info.input_dir = Path("/input")
    wiper._informer.usesScratch.return_value = False
    wiper._batch_system = MagicMock()

    with pytest.raises(
        QQNotSuitableError, match="Job does not have a working directory."
    ):
        wiper.ensureSuitable()


def test_wiper_ensure_suitable_raises_when_workdir_is_inputdir():
    wiper = Wiper.__new__(Wiper)
    wiper._state = RealState.FAILED
    wiper._informer = MagicMock()
    wiper._informer.info.job_exit_code = 1
    wiper._informer.getRealState.return_value = RealState.FAILED
    same_path = Path("/shared/path")

    wiper._work_dir = same_path
    wiper._main_node = "main"
    wiper._input_machine = "main"
    wiper._informer.info.input_dir = same_path
    wiper._informer.usesScratch.return_value = False
    wiper._batch_system = MagicMock()

    with pytest.raises(
        QQNotSuitableError, match="Working directory of the job is the input directory"
    ):
        wiper.ensureSuitable()


@pytest.mark.parametrize(
    "state",
    [
        RealState.FAILED,
        RealState.KILLED,
        RealState.UNKNOWN,
        RealState.IN_AN_INCONSISTENT_STATE,
    ],
)
def test_wiper_ensure_suitable_passes_for_allowed_states(state):
    wiper = Wiper.__new__(Wiper)
    wiper._state = state
    wiper._informer = MagicMock()
    wiper._informer.info.job_exit_code = 1
    wiper._informer.getRealState.return_value = state
    wiper._work_dir = Path("/valid/workdir")
    wiper._main_node = "main"
    wiper._input_machine = "input"
    wiper._informer.info.input_dir = Path("/different")
    wiper._informer.usesScratch.return_value = True
    wiper._batch_system = MagicMock()

    wiper.ensureSuitable()


@pytest.mark.parametrize(
    "has_destination, workdir_is_inputdir, expected_exception, expected_message",
    [
        (
            False,
            False,
            QQError,
            "not defined",
        ),
        (
            True,
            True,
            QQError,
            "Working directory of the job is the input directory of the job",
        ),
    ],
)
def test_wiper_wipe_raises_for_invalid_conditions(
    has_destination, workdir_is_inputdir, expected_exception, expected_message
):
    wiper = Wiper.__new__(Wiper)
    wiper.hasDestination = MagicMock(return_value=has_destination)
    wiper._workDirIsInputDir = MagicMock(return_value=workdir_is_inputdir)
    wiper._batch_system = MagicMock()
    wiper._informer = MagicMock()
    wiper._main_node = "node"
    wiper._work_dir = Path("/workdir")

    with pytest.raises(expected_exception, match=expected_message):
        wiper.wipe()


@patch("qq_lib.wipe.wiper.logger.info")
def test_wiper_delete_success_calls_logger_and_deletes(mock_logger_info):
    # Successful deletion should call logger and deleteRemoteDir
    wiper = Wiper.__new__(Wiper)
    wiper.hasDestination = MagicMock(return_value=True)
    wiper._workDirIsInputDir = MagicMock(return_value=False)
    wiper._batch_system = MagicMock()
    wiper._informer = MagicMock()
    wiper._informer.info.job_id = "job123"
    wiper._main_node = "main_node"
    wiper._work_dir = Path("/some/workdir")

    result = wiper.wipe()

    assert result == "job123"
    mock_logger_info.assert_called_once()
    wiper._batch_system.deleteRemoteDir.assert_called_once_with(
        "main_node", Path("/some/workdir")
    )


@pytest.mark.parametrize(
    "work_dir, input_dir, uses_scratch, main_node, input_machine, expected",
    [
        # work dir is None
        (None, Path("/input"), False, "main", "input", False),
        # different paths
        (Path("/workdir"), Path("/input"), False, "main", "input", False),
        # same path, shared storage, different nodes
        (Path("/shared/path"), Path("/shared/path"), False, "nodeA", "nodeB", True),
        #  same path, scratch storage, same node
        (Path("/shared/path"), Path("/shared/path"), True, "nodeA", "nodeA", True),
        # # same path, scratch storage, different node
        (Path("/shared/path"), Path("/shared/path"), True, "nodeA", "nodeB", False),
        # same path, shared storage, same node
        (Path("/same/path"), Path("/same/path"), False, "main", "main", True),
    ],
)
def test_wiper_workdir_is_inputdir_various_conditions(
    work_dir, input_dir, uses_scratch, main_node, input_machine, expected
):
    wiper = Wiper.__new__(Wiper)
    wiper._work_dir = work_dir
    wiper._main_node = main_node
    wiper._input_machine = input_machine

    wiper._informer = MagicMock()
    wiper._informer.info.input_dir = input_dir
    wiper._informer.usesScratch.return_value = uses_scratch

    result = wiper._workDirIsInputDir()
    assert result is expected
