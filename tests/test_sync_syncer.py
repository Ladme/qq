# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.batch.pbs.qqpbs import QQPBS
from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.properties.states import RealState
from qq_lib.sync.syncer import QQSyncer


def test_qqsyncer_init():
    mock_path = Path("/fake/job.qqinfo")

    with (
        patch("qq_lib.sync.syncer.QQInformer.fromFile") as mock_from_file,
        patch.object(QQSyncer, "_setDestination") as mock_set_destination,
    ):
        mock_informer_instance = MagicMock()
        mock_informer_instance.batch_system = QQPBS
        mock_informer_instance.getRealState.return_value = RealState.RUNNING
        mock_from_file.return_value = mock_informer_instance

        syncer = QQSyncer(mock_path)

        assert syncer._info_file == mock_path
        assert syncer._informer == mock_informer_instance
        assert syncer._batch_system == QQPBS
        assert syncer._state == RealState.RUNNING
        mock_from_file.assert_called_once_with(mock_path)
        mock_set_destination.assert_called_once()


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
def test_qqsyncer_is_queued(state, expected):
    goer = QQSyncer.__new__(QQSyncer)
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
def test_qqsyncer_is_killed(state, job_exit_code, expected):
    goer = QQSyncer.__new__(QQSyncer)
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
def test_qqsyncer_is_finished(state, expected):
    goer = QQSyncer.__new__(QQSyncer)
    goer._state = state
    assert goer._isFinished() is expected


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
def test_qqsyncer_is_exiting_successfully(state, job_exit_code, expected):
    goer = QQSyncer.__new__(QQSyncer)
    goer._state = state
    goer._informer = MagicMock()
    goer._informer.info.job_exit_code = job_exit_code
    assert goer._isExitingSuccessfully() is expected


def test_qqsyncer_set_destination_exists():
    goer = QQSyncer.__new__(QQSyncer)
    mock_informer = MagicMock()
    mock_informer.getDestination.return_value = ("host1", "/path/to/dir")
    goer._informer = mock_informer

    goer._setDestination()

    assert goer._host == "host1"
    assert goer._directory == "/path/to/dir"


def test_qqsyncer_set_destination_none():
    goer = QQSyncer.__new__(QQSyncer)
    mock_informer = MagicMock()
    mock_informer.getDestination.return_value = None
    goer._informer = mock_informer

    goer._setDestination()

    assert goer._host is None
    assert goer._directory is None


def test_qqsyncer_has_destination_returns_true_when_both_set():
    goer = QQSyncer.__new__(QQSyncer)
    goer._directory = Path("/some/dir")
    goer._host = "host1"

    assert goer.hasDestination() is True


def test_qqsyncer_has_destination_returns_false_when_directory_none():
    goer = QQSyncer.__new__(QQSyncer)
    goer._directory = None
    goer._host = "host1"

    assert goer.hasDestination() is False


def test_qqsyncer_has_destination_returns_false_when_host_none():
    goer = QQSyncer.__new__(QQSyncer)
    goer._directory = Path("/some/dir")
    goer._host = None

    assert goer.hasDestination() is False


def test_qqsyncer_has_destination_returns_false_when_both_none():
    goer = QQSyncer.__new__(QQSyncer)
    goer._directory = None
    goer._host = None

    assert goer.hasDestination() is False


def test_qqsyncer_matches_job_returns_true():
    goer = QQSyncer.__new__(QQSyncer)
    goer._informer = MagicMock()
    goer._informer.isJob.return_value = True

    assert goer.matchesJob("12345") is True
    goer._informer.isJob.assert_called_once_with("12345")


def test_qqsyncer_matches_job_returns_false():
    goer = QQSyncer.__new__(QQSyncer)
    goer._informer = MagicMock()
    goer._informer.isJob.return_value = False

    assert goer.matchesJob("99999") is False
    goer._informer.isJob.assert_called_once_with("99999")


def test_qqsyncer_print_info_prints_panel():
    killer = QQSyncer.__new__(QQSyncer)
    killer._informer = MagicMock()
    mock_console = MagicMock()
    mock_panel = MagicMock()

    with patch("qq_lib.sync.syncer.QQPresenter") as mock_presenter_cls:
        mock_presenter_instance = MagicMock()
        mock_presenter_cls.return_value = mock_presenter_instance
        mock_presenter_instance.createJobStatusPanel.return_value = mock_panel

        killer.printInfo(mock_console)

        mock_presenter_cls.assert_called_once_with(killer._informer)
        mock_presenter_instance.createJobStatusPanel.assert_called_once_with(
            mock_console
        )
        mock_console.print.assert_called_once_with(mock_panel)


def test_qqsyncer_ensure_suitable_raises_finished():
    syncer = QQSyncer.__new__(QQSyncer)
    syncer._state = RealState.FINISHED

    with pytest.raises(
        QQNotSuitableError,
        match="Job has finished and was synchronized: nothing to sync.",
    ):
        syncer.ensureSuitable()


def test_qqsyncer_ensure_suitable_raises_exiting_successfully():
    syncer = QQSyncer.__new__(QQSyncer)
    syncer._state = RealState.EXITING
    syncer._informer = MagicMock()
    syncer._informer.info.job_exit_code = 0

    with pytest.raises(
        QQNotSuitableError,
        match="Job is finishing successfully: nothing to sync.",
    ):
        syncer.ensureSuitable()


@pytest.mark.parametrize("destination", [(None, "host"), (Path("some/path"), None)])
def test_qqsyncer_ensure_suitable_raises_killed_without_destination(destination):
    syncer = QQSyncer.__new__(QQSyncer)
    syncer._state = RealState.KILLED
    syncer._directory, syncer._host = destination

    with pytest.raises(
        QQNotSuitableError,
        match="Job has been killed and no working directory is available.",
    ):
        syncer.ensureSuitable()


def test_qqsyncer_ensure_suitable_raises_queued_state():
    syncer = QQSyncer.__new__(QQSyncer)
    syncer._state = RealState.QUEUED

    with pytest.raises(
        QQNotSuitableError,
        match="Job is queued or booting: nothing to sync.",
    ):
        syncer.ensureSuitable()


def test_qqsyncer_ensure_suitable_passes_when_suitable():
    syncer = QQSyncer.__new__(QQSyncer)
    syncer._state = RealState.RUNNING
    syncer._directory = Path("/some/dir")
    syncer._host = "host"

    # should not raise
    syncer.ensureSuitable()


@pytest.mark.parametrize(
    "destination", [(None, "host"), (Path("some/path"), None), (None, None)]
)
def test_qqsyncer_sync_raises_without_destination(destination):
    syncer = QQSyncer.__new__(QQSyncer)
    syncer._directory, syncer._host = destination

    with pytest.raises(
        QQError,
        match=r"Host \('main_node'\) or working directory \('work_dir'\) are not defined\.",
    ):
        syncer.sync()


def test_qqsyncer_sync_calls_sync_selected_with_files():
    syncer = QQSyncer.__new__(QQSyncer)
    syncer._directory = Path("/work")
    syncer._host = "host"
    syncer._batch_system = MagicMock()
    syncer._informer = MagicMock()
    syncer._informer.info.input_dir = Path("/input")
    syncer.hasDestination = MagicMock(return_value=True)

    files = ["a.txt", "b.txt"]

    with patch("qq_lib.sync.syncer.logger") as mock_logger:
        syncer.sync(files=files)
        mock_logger.info.assert_called_once_with(
            "Fetching files 'a.txt b.txt' from job's working directory to input directory."
        )
        syncer._batch_system.syncSelected.assert_called_once_with(
            syncer._directory,
            syncer._informer.info.input_dir,
            syncer._host,
            None,
            [syncer._directory / x for x in files],
        )


def test_qqsyncer_sync_calls_sync_with_exclusions_without_files():
    syncer = QQSyncer.__new__(QQSyncer)
    syncer._directory = Path("/work")
    syncer._host = "host"
    syncer._batch_system = MagicMock()
    syncer._informer = MagicMock()
    syncer._informer.info.input_dir = Path("/input")
    syncer.hasDestination = MagicMock(return_value=True)

    with patch("qq_lib.sync.syncer.logger") as mock_logger:
        syncer.sync()
        mock_logger.info.assert_called_once_with(
            "Fetching all files from job's working directory to input directory."
        )
        syncer._batch_system.syncWithExclusions.assert_called_once_with(
            syncer._directory, syncer._informer.info.input_dir, syncer._host, None
        )
