# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.core.config import CFG
from qq_lib.core.error import QQError, QQJobMismatchError
from qq_lib.info.informer import Informer
from qq_lib.properties.info import Info
from qq_lib.properties.states import BatchState, NaiveState, RealState


def test_informer_init_sets_info_and_batch_info():
    info_mock = MagicMock(spec=Info)
    informer = Informer(info_mock)
    assert informer.info == info_mock
    assert informer._batch_info is None


@pytest.mark.parametrize(
    "input_id,expected",
    [
        ("12345.fake.server.com", True),
        ("12345.other.domain.net", True),
        ("12345", True),
        ("12345.", True),
        ("12345.fake.server.com.subdomain", True),
        ("99999.fake.server.com", False),
        ("54321", False),
        ("abcd.fake.server.com", False),
        ("", False),
        (".fake.server.com", False),
        ("12345.fake", True),
        (" 12345.fake.server.com ", True),
        ("12345.FAKE.SERVER.COM", True),
        ("123456.fake.server.com", False),
        ("12345.....fake.server.com", True),
        ("1234.fake.server.com", False),
    ],
)
def test_is_job_matches_and_mismatches(input_id, expected):
    informer = Informer.__new__(Informer)
    informer.info = MagicMock(job_id="12345.fake.server.com")
    input_id = input_id.strip()
    assert informer.matchesJob(input_id) == expected


def test_informer_batch_system():
    info_mock = MagicMock(spec=Info)
    batch_system_mock = MagicMock()
    info_mock.batch_system = batch_system_mock

    informer = Informer(info_mock)
    assert informer.batch_system == batch_system_mock


def test_informer_from_file_with_host(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy content")

    qqinfo_mock = MagicMock(spec=Info)
    with patch(
        "qq_lib.info.informer.Info.fromFile", return_value=qqinfo_mock
    ) as mock_from_file:
        informer = Informer.fromFile(dummy_file, host="remote_host")

    mock_from_file.assert_called_once_with(dummy_file, "remote_host")
    assert isinstance(informer, Informer)
    assert informer.info == qqinfo_mock


def test_informer_from_file_no_host(tmp_path):
    dummy_file = tmp_path / "job.qqinfo"
    dummy_file.write_text("dummy content")

    qqinfo_mock = MagicMock(spec=Info)
    with patch(
        "qq_lib.info.informer.Info.fromFile", return_value=qqinfo_mock
    ) as mock_from_file:
        informer = Informer.fromFile(dummy_file, host=None)

    mock_from_file.assert_called_once_with(dummy_file, None)
    assert isinstance(informer, Informer)
    assert informer.info == qqinfo_mock


def test_informer_to_file_with_host(tmp_path):
    info_mock = MagicMock(spec=Info)
    informer = Informer(info_mock)

    dummy_file = tmp_path / "output.qqinfo"
    informer.toFile(dummy_file, host="remote_host")

    info_mock.toFile.assert_called_once_with(dummy_file, "remote_host")


def test_informer_to_file_no_host(tmp_path):
    info_mock = MagicMock(spec=Info)
    informer = Informer(info_mock)

    dummy_file = tmp_path / "output.qqinfo"
    informer.toFile(dummy_file, host=None)

    info_mock.toFile.assert_called_once_with(dummy_file, None)


def test_informer_set_running():
    info_mock = MagicMock(spec=Info)
    informer = Informer(info_mock)

    time_val = datetime(2025, 1, 1, 12, 0)
    main_node = "node01"
    all_nodes = ["node01", "node02"]
    work_dir = Path("/tmp/jobdir")

    informer.setRunning(time_val, main_node, all_nodes, work_dir)

    assert info_mock.job_state == NaiveState.RUNNING
    assert info_mock.start_time == time_val
    assert info_mock.main_node == main_node
    assert info_mock.all_nodes == all_nodes
    assert info_mock.work_dir == work_dir


def test_informer_set_finished():
    info_mock = MagicMock(spec=Info)
    informer = Informer(info_mock)

    time_val = datetime(2025, 1, 1, 15, 0)
    informer.setFinished(time_val)

    assert info_mock.job_state == NaiveState.FINISHED
    assert info_mock.completion_time == time_val
    assert info_mock.job_exit_code == 0


def test_informer_set_failed():
    info_mock = MagicMock(spec=Info)
    informer = Informer(info_mock)

    time_val = datetime(2025, 1, 1, 15, 0)
    exit_code = 42
    informer.setFailed(time_val, exit_code)

    assert info_mock.job_state == NaiveState.FAILED
    assert info_mock.completion_time == time_val
    assert info_mock.job_exit_code == exit_code


def test_informer_set_killed():
    info_mock = MagicMock(spec=Info)
    informer = Informer(info_mock)

    time_val = datetime(2025, 1, 1, 15, 0)
    informer.setKilled(time_val)

    assert info_mock.job_state == NaiveState.KILLED
    assert info_mock.completion_time == time_val


def test_informer_uses_scratch_returns_value():
    resources_mock = MagicMock()
    resources_mock.usesScratch.return_value = True

    info_mock = MagicMock(spec=Info)
    info_mock.resources = resources_mock

    informer = Informer(info_mock)
    assert informer.usesScratch() is True

    resources_mock.usesScratch.return_value = False
    assert informer.usesScratch() is False


def test_informer_get_destination_returns_tuple_when_set():
    info_mock = MagicMock(spec=Info)
    info_mock.main_node = "node01"
    info_mock.work_dir = Path("/tmp/jobdir")

    informer = Informer(info_mock)
    dest = informer.getDestination()

    assert dest == ("node01", Path("/tmp/jobdir"))


def test_informer_get_destination_returns_none_when_missing():
    info_mock = MagicMock(spec=Info)
    info_mock.main_node = None
    info_mock.work_dir = Path("/tmp/jobdir")

    informer = Informer(info_mock)
    assert informer.getDestination() is None

    info_mock.main_node = "node01"
    info_mock.work_dir = None
    assert informer.getDestination() is None

    info_mock.main_node = None
    info_mock.work_dir = None
    assert informer.getDestination() is None


def test_informer_get_batch_state_no_cache():
    batch_job_info_mock = MagicMock()
    batch_job_info_mock.getState.return_value = BatchState.RUNNING

    batch_system_mock = MagicMock()
    batch_system_mock.getBatchJob.return_value = batch_job_info_mock

    info_mock = MagicMock()
    info_mock.job_id = "12345"
    info_mock.batch_system = batch_system_mock

    informer = Informer(info_mock)

    state = informer.getBatchState()

    batch_system_mock.getBatchJob.assert_called_once_with("12345")
    batch_job_info_mock.getState.assert_called_once()
    assert state == BatchState.RUNNING


def test_informer_get_batch_state_with_cache():
    batch_job_info_mock = MagicMock()
    batch_job_info_mock.getState.return_value = BatchState.FINISHED

    batch_system_mock = MagicMock()
    batch_system_mock.getBatchJob.return_value = batch_job_info_mock

    info_mock = MagicMock()
    info_mock.job_id = "12345"
    info_mock.batch_system = batch_system_mock

    informer = Informer(info_mock)
    # populate the cache
    informer._batch_info = batch_job_info_mock

    state = informer.getBatchState()

    batch_system_mock.getBatchJob.assert_not_called()
    batch_job_info_mock.getState.assert_called_once()
    assert state == BatchState.FINISHED


def test_informer_get_real_state():
    info_mock = MagicMock()
    info_mock.job_state = NaiveState.QUEUED
    info_mock.job_id = "12345"
    info_mock.batch_system = MagicMock()

    informer = Informer(info_mock)

    with (
        patch.object(
            informer, "getBatchState", return_value=BatchState.RUNNING
        ) as batch_state_patch,
        patch(
            "qq_lib.info.informer.RealState.fromStates", return_value=RealState.BOOTING
        ) as mock_from_states,
    ):
        state = informer.getRealState()

    batch_state_patch.assert_called_once()
    mock_from_states.assert_called_once_with(NaiveState.QUEUED, BatchState.RUNNING)
    assert state == RealState.BOOTING


def test_informer_get_comment_no_cache():
    batch_job_info_mock = MagicMock()
    batch_job_info_mock.getComment.return_value = "Job comment"

    batch_system_mock = MagicMock()
    batch_system_mock.getBatchJob.return_value = batch_job_info_mock

    info_mock = MagicMock()
    info_mock.job_id = "12345"
    info_mock.batch_system = batch_system_mock

    informer = Informer(info_mock)

    comment = informer.getComment()

    batch_system_mock.getBatchJob.assert_called_once_with("12345")
    batch_job_info_mock.getComment.assert_called_once()
    assert comment == "Job comment"


def test_informer_get_comment_with_cache():
    batch_job_info_mock = MagicMock()
    batch_job_info_mock.getComment.return_value = "Cached comment"

    info_mock = MagicMock()
    info_mock.job_id = "12345"
    info_mock.batch_system = MagicMock()

    informer = Informer(info_mock)
    # populate the cache
    informer._batch_info = batch_job_info_mock

    comment = informer.getComment()

    batch_job_info_mock.getComment.assert_called_once()
    assert comment == "Cached comment"


def test_informer_get_estimated_no_cache():
    estimated_mock = (MagicMock(), "node1")
    batch_job_info_mock = MagicMock()
    batch_job_info_mock.getEstimated.return_value = estimated_mock

    batch_system_mock = MagicMock()
    batch_system_mock.getBatchJob.return_value = batch_job_info_mock

    info_mock = MagicMock()
    info_mock.job_id = "12345"
    info_mock.batch_system = batch_system_mock

    informer = Informer(info_mock)

    result = informer.getEstimated()

    batch_system_mock.getBatchJob.assert_called_once_with("12345")
    batch_job_info_mock.getEstimated.assert_called_once()
    assert result == estimated_mock


def test_informer_get_estimated_with_cache():
    estimated_mock = (MagicMock(), "node1")
    batch_job_info_mock = MagicMock()
    batch_job_info_mock.getEstimated.return_value = estimated_mock

    info_mock = MagicMock()
    info_mock.job_id = "12345"
    info_mock.batch_system = MagicMock()

    informer = Informer(info_mock)
    # populate the cache
    informer._batch_info = batch_job_info_mock

    result = informer.getEstimated()

    batch_job_info_mock.getEstimated.assert_called_once()
    assert result == estimated_mock


def test_informer_get_main_node_no_cache():
    batch_job_info_mock = MagicMock()
    batch_job_info_mock.getMainNode.return_value = "node1"

    batch_system_mock = MagicMock()
    batch_system_mock.getBatchJob.return_value = batch_job_info_mock

    info_mock = MagicMock()
    info_mock.job_id = "12345"
    info_mock.batch_system = batch_system_mock

    informer = Informer(info_mock)

    result = informer.getMainNode()

    batch_system_mock.getBatchJob.assert_called_once_with("12345")
    batch_job_info_mock.getMainNode.assert_called_once()
    assert result == "node1"


def test_informer_get_main_node_with_cache():
    batch_job_info_mock = MagicMock()
    batch_job_info_mock.getMainNode.return_value = "node1"

    info_mock = MagicMock()
    info_mock.job_id = "12345"
    info_mock.batch_system = MagicMock()

    informer = Informer(info_mock)
    # populate the cache
    informer._batch_info = batch_job_info_mock

    result = informer.getMainNode()

    batch_job_info_mock.getMainNode.assert_called_once()
    assert result == "node1"


def test_informer_get_nodes_no_cache():
    batch_job_info_mock = MagicMock()
    batch_job_info_mock.getNodes.return_value = ["node1", "node2"]

    batch_system_mock = MagicMock()
    batch_system_mock.getBatchJob.return_value = batch_job_info_mock

    info_mock = MagicMock()
    info_mock.job_id = "12345"
    info_mock.batch_system = batch_system_mock

    informer = Informer(info_mock)

    result = informer.getNodes()

    batch_system_mock.getBatchJob.assert_called_once_with("12345")
    batch_job_info_mock.getNodes.assert_called_once()
    assert result == ["node1", "node2"]


def test_informer_get_nodes_with_cache():
    batch_job_info_mock = MagicMock()
    batch_job_info_mock.getNodes.return_value = ["node1", "node2"]

    info_mock = MagicMock()
    info_mock.job_id = "12345"
    info_mock.batch_system = MagicMock()

    informer = Informer(info_mock)
    # populate the cache
    informer._batch_info = batch_job_info_mock

    result = informer.getNodes()

    batch_job_info_mock.getNodes.assert_called_once()
    assert result == ["node1", "node2"]


def test_informer_get_batch_info_fetches_when_not_cached():
    batch_job = MagicMock()
    batch_system = MagicMock()
    batch_system.getBatchJob.return_value = batch_job

    info = MagicMock()
    info.job_id = "123"
    info.batch_system = batch_system

    informer = Informer(info)

    result = informer.getBatchInfo()

    assert result is batch_job
    batch_system.getBatchJob.assert_called_once_with("123")
    assert informer._batch_info is batch_job


def test_informer_get_batch_info_returns_cached_when_available():
    batch_job = MagicMock()

    info = MagicMock()
    info.job_id = "123"

    informer = Informer(info)
    informer._batch_info = batch_job

    result = informer.getBatchInfo()

    assert result is batch_job


def test_informer_get_info_file_returns_expected_path():
    info = Info.__new__(Info)
    info.input_dir = Path("/tmp/jobs")
    info.job_name = "job"

    informer = Informer.__new__(Informer)
    informer.info = info

    expected = (
        (info.input_dir / info.job_name).with_suffix(CFG.suffixes.qq_info).resolve()
    )

    assert informer.getInfoFile() == expected


def test_informer_from_job_id_raises_when_empty():
    batch_system = MagicMock()
    batch_job = MagicMock()
    batch_job.isEmpty.return_value = True
    batch_system.getBatchJob.return_value = batch_job

    with (
        patch(
            "qq_lib.info.informer.BatchMeta.fromEnvVarOrGuess",
            return_value=batch_system,
        ),
        pytest.raises(QQError, match="Job '123' does not exist."),
    ):
        Informer.fromJobId("123")


def test_informer_from_job_id_returns_informer_when_valid():
    batch_system = MagicMock()
    batch_job = MagicMock()
    batch_job.isEmpty.return_value = False
    batch_system.getBatchJob.return_value = batch_job

    informer_instance = MagicMock()

    with (
        patch(
            "qq_lib.info.informer.BatchMeta.fromEnvVarOrGuess",
            return_value=batch_system,
        ),
        patch(
            "qq_lib.info.informer.Informer.fromBatchJob", return_value=informer_instance
        ),
    ):
        result = Informer.fromJobId("123")

    assert result is informer_instance


def test_informer_from_batch_job_raises_when_no_info_file():
    batch_job = MagicMock()
    batch_job.getInfoFile.return_value = None
    batch_job.getId.return_value = "123"

    with pytest.raises(QQError, match="Job '123' is not a valid qq job."):
        Informer.fromBatchJob(batch_job)


def test_informer_from_batch_job_raises_on_mismatch():
    batch_job = MagicMock()
    batch_job.getInfoFile.return_value = "info_path"
    batch_job.getId.return_value = "123"

    informer_mock = MagicMock()
    informer_mock.matchesJob.return_value = False

    with (
        patch("qq_lib.info.informer.Informer.fromFile", return_value=informer_mock),
        pytest.raises(
            QQJobMismatchError,
            match="Info file for job '123' does not exist or is not reachable.",
        ),
    ):
        Informer.fromBatchJob(batch_job)


def test_informer_from_batch_job_returns_informer_on_success():
    batch_job = MagicMock()
    batch_job.getInfoFile.return_value = "info_path"
    batch_job.getId.return_value = "123"

    informer_mock = MagicMock()
    informer_mock.matchesJob.return_value = True

    with patch("qq_lib.info.informer.Informer.fromFile", return_value=informer_mock):
        result = Informer.fromBatchJob(batch_job)

    assert result is informer_mock
    assert result._batch_info is batch_job
