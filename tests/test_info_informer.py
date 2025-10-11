# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from qq_lib.batch.interface import QQBatchMeta
from qq_lib.batch.pbs import QQPBS, PBSJobInfo
from qq_lib.core.constants import DATE_FORMAT
from qq_lib.info.informer import QQInformer
from qq_lib.properties.info import QQInfo
from qq_lib.properties.job_type import QQJobType
from qq_lib.properties.resources import QQResources
from qq_lib.properties.states import BatchState, NaiveState, RealState


@pytest.fixture(autouse=True)
def register():
    QQBatchMeta.register(QQPBS)


@pytest.fixture
def sample_resources():
    return QQResources(ncpus=8, work_dir="scratch_local")


@pytest.fixture
def sample_info(sample_resources):
    return QQInfo(
        batch_system=QQPBS,
        qq_version="0.1.0",
        username="fake_user",
        job_id="12345.fake.server.com",
        job_name="script.sh+025",
        queue="default",
        script_name="script.sh",
        job_type=QQJobType.STANDARD,
        input_machine="fake.machine.com",
        input_dir=Path("/shared/storage/"),
        job_state=NaiveState.RUNNING,
        submission_time=datetime.strptime("2025-09-21 12:00:00", DATE_FORMAT),
        stdout_file="stdout.log",
        stderr_file="stderr.log",
        resources=sample_resources,
        excluded_files=[Path("ignore.txt")],
        command_line=["-q", "default", "script.sh"],
        work_dir=Path("/scratch/job_12345.fake.server.com"),
    )


def test_load_informer_from_file(tmp_path, sample_info):
    file_path = tmp_path / "qqinfo.yaml"

    sample_info.toFile(file_path)

    loaded_informer = QQInformer.fromFile(file_path)

    assert loaded_informer.info.input_machine == sample_info.input_machine
    assert loaded_informer.info.work_dir == sample_info.work_dir
    assert loaded_informer.info.resources.work_dir == sample_info.resources.work_dir


def test_export_informer_to_file_contains_yaml(sample_info, tmp_path):
    file_path = tmp_path / "qqinfo.yaml"

    informer = QQInformer(sample_info)
    informer.toFile(file_path)

    content = file_path.read_text()

    assert content.startswith("# qq job info file")

    data: dict[str, str] = yaml.safe_load(content)

    assert data["job_id"] == informer.info.job_id
    assert data["job_name"] == informer.info.job_name
    assert data["batch_system"] == str(informer.batch_system)
    assert data["job_state"] == str(informer.info.job_state)

    resources_dict = informer.info.resources.toDict()
    assert data["resources"] == resources_dict

    assert informer.info.excluded_files is not None
    assert data["excluded_files"] == [str(p) for p in informer.info.excluded_files]

    assert "start_time" not in data
    assert "completion_time" not in data
    assert "main_node" not in data
    assert "job_exit_code" not in data


def test_set_running(sample_info):
    informer = QQInformer(sample_info)
    start_time = datetime(2025, 9, 22, 14, 30, 0)
    informer.setRunning(
        start_time,
        "main.node",
        ["main.node", "node02", "node03"],
        Path("/scratch/new_dir"),
    )

    assert informer.info.job_state == NaiveState.RUNNING
    assert informer.info.start_time == start_time
    assert informer.info.main_node == "main.node"
    assert informer.info.all_nodes == ["main.node", "node02", "node03"]
    assert informer.info.work_dir == Path("/scratch/new_dir")


def test_set_finished(sample_info):
    informer = QQInformer(sample_info)
    finish_time = datetime(2025, 9, 22, 16, 0, 0)
    informer.setFinished(finish_time)

    assert informer.info.job_state == NaiveState.FINISHED
    assert informer.info.completion_time == finish_time
    assert informer.info.job_exit_code == 0


def test_set_failed(sample_info):
    informer = QQInformer(sample_info)
    fail_time = datetime(2025, 9, 22, 16, 15, 0)
    informer.setFailed(fail_time, exit_code=42)

    assert informer.info.job_state == NaiveState.FAILED
    assert informer.info.completion_time == fail_time
    assert informer.info.job_exit_code == 42


def test_set_killed(sample_info):
    informer = QQInformer(sample_info)
    killed_time = datetime(2025, 9, 22, 16, 30, 0)
    informer.setKilled(killed_time)

    assert informer.info.job_state == NaiveState.KILLED
    assert informer.info.completion_time == killed_time
    # no exit_code set by setKilled
    assert informer.info.job_exit_code is None


def test_use_scratch_true(sample_info):
    informer = QQInformer(sample_info)
    assert informer.useScratch()


def test_use_scratch_false(sample_info):
    informer = QQInformer(sample_info)
    informer.info.resources.work_dir = "input_dir"
    assert not informer.useScratch()


def test_get_destination_exists(sample_info):
    informer = QQInformer(sample_info)
    informer.info.main_node = "random.node.org"

    assert informer.getDestination() == (
        "random.node.org",
        Path("/scratch/job_12345.fake.server.com"),
    )


def test_get_destination_no_workdir(sample_info):
    informer = QQInformer(sample_info)
    informer.info.main_node = "random.node.org"
    informer.info.work_dir = None

    assert informer.getDestination() is None


def test_get_destination_no_node(sample_info):
    informer = QQInformer(sample_info)

    assert informer.getDestination() is None


def test_get_destination_no_node_no_workdir(sample_info):
    informer = QQInformer(sample_info)
    informer.info.work_dir = None

    assert informer.getDestination() is None


@pytest.mark.parametrize("naive_state", list(NaiveState))
@pytest.mark.parametrize("batch_state", list(BatchState))
def test_get_real_state(sample_info, naive_state, batch_state):
    informer = QQInformer(sample_info)
    informer.info.job_state = naive_state

    with patch.object(QQInformer, "getBatchState", return_value=batch_state):
        assert informer.getRealState() == RealState.fromStates(naive_state, batch_state)


def _make_pbsjobinfo_with_info(info: dict[str, str]) -> PBSJobInfo:
    job = PBSJobInfo.__new__(PBSJobInfo)
    job._job_id = "1234"
    job._info = info
    return job


def _make_informer_with_batch_info(batch_info: PBSJobInfo) -> QQInformer:
    informer = QQInformer.__new__(QQInformer)
    informer.info = None  # not used
    informer._batch_info = batch_info
    return informer


def test_get_batch_state_running():
    batch_info = _make_pbsjobinfo_with_info({"job_state": "R"})
    informer = _make_informer_with_batch_info(batch_info)
    assert informer.getBatchState() == BatchState.RUNNING


def test_get_batch_state_failed_when_exit_code_nonzero():
    batch_info = _make_pbsjobinfo_with_info({"job_state": "F", "Exit_status": "1"})
    informer = _make_informer_with_batch_info(batch_info)
    assert informer.getBatchState() == BatchState.FAILED


def test_get_batch_state_unknown_if_missing():
    batch_info = _make_pbsjobinfo_with_info({})
    informer = _make_informer_with_batch_info(batch_info)
    assert informer.getBatchState() == BatchState.UNKNOWN


def test_get_comment_present():
    batch_info = _make_pbsjobinfo_with_info({"comment": "Job in queue"})
    informer = _make_informer_with_batch_info(batch_info)
    assert informer.getComment() == "Job in queue"


def test_get_comment_none_when_missing():
    batch_info = _make_pbsjobinfo_with_info({})
    informer = _make_informer_with_batch_info(batch_info)
    assert informer.getComment() is None


def test_get_estimated_success():
    raw_time = "Fri Oct  4 15:30:00 2124"
    vnode = "(node01:cpu=4)"
    batch_info = _make_pbsjobinfo_with_info(
        {
            "estimated.start_time": raw_time,
            "estimated.exec_vnode": vnode,
        }
    )
    informer = _make_informer_with_batch_info(batch_info)

    result = informer.getEstimated()
    assert isinstance(result, tuple)
    est_time, est_node = result
    assert est_time == datetime(2124, 10, 4, 15, 30, 0)
    assert est_node == "node01"


def test_get_estimated_none_if_missing_time():
    batch_info = _make_pbsjobinfo_with_info({"estimated.exec_vnode": "(node01:cpu=4)"})
    informer = _make_informer_with_batch_info(batch_info)
    assert informer.getEstimated() is None


def test_get_estimated_none_if_missing_vnode():
    raw_time = "Fri Oct  4 15:30:00 2124"
    batch_info = _make_pbsjobinfo_with_info({"estimated.start_time": raw_time})
    informer = _make_informer_with_batch_info(batch_info)
    assert informer.getEstimated() is None


def test_get_main_node_present():
    batch_info = _make_pbsjobinfo_with_info({"exec_host2": "node01.fake.server.org"})
    informer = _make_informer_with_batch_info(batch_info)
    assert informer.getMainNode() == "node01.fake.server.org"


def test_get_main_node_with_parentheses():
    batch_info = _make_pbsjobinfo_with_info({"exec_host2": "(node03:cpu=4)"})
    informer = _make_informer_with_batch_info(batch_info)
    assert informer.getMainNode() == "node03"


def test_get_main_node_complex():
    batch_info = _make_pbsjobinfo_with_info(
        {"exec_host2": "node04.fake.server.org:15002/3*8"}
    )
    informer = _make_informer_with_batch_info(batch_info)
    assert informer.getMainNode() == "node04.fake.server.org"


def test_get_main_node_none_when_missing():
    batch_info = _make_pbsjobinfo_with_info({})
    informer = _make_informer_with_batch_info(batch_info)
    assert informer.getMainNode() is None


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
def test_is_job_matches_and_mismatches(sample_info, input_id, expected):
    informer = QQInformer(sample_info)
    input_id = input_id.strip()
    assert informer.isJob(input_id) == expected
