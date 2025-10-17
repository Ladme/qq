# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.core.error import QQError, QQNotSuitableError
from qq_lib.properties.states import RealState
from qq_lib.sync.syncer import QQSyncer


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
    syncer._work_dir, syncer._main_node = destination

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
    syncer._work_dir = Path("/some/dir")
    syncer._main_node = "host"

    # should not raise
    syncer.ensureSuitable()


@pytest.mark.parametrize(
    "destination", [(None, "host"), (Path("some/path"), None), (None, None)]
)
def test_qqsyncer_sync_raises_without_destination(destination):
    syncer = QQSyncer.__new__(QQSyncer)
    syncer._work_dir, syncer._main_node = destination

    with pytest.raises(
        QQError,
        match=r"Host \('main_node'\) or working directory \('work_dir'\) are not defined\.",
    ):
        syncer.sync()


def test_qqsyncer_sync_calls_sync_selected_with_files():
    syncer = QQSyncer.__new__(QQSyncer)
    syncer._work_dir = Path("/work")
    syncer._main_node = "host"
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
            syncer._work_dir,
            syncer._informer.info.input_dir,
            syncer._main_node,
            None,
            [syncer._work_dir / x for x in files],
        )


def test_qqsyncer_sync_calls_sync_with_exclusions_without_files():
    syncer = QQSyncer.__new__(QQSyncer)
    syncer._work_dir = Path("/work")
    syncer._main_node = "host"
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
            syncer._work_dir, syncer._informer.info.input_dir, syncer._main_node, None
        )
