# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch
from rich.panel import Panel
from rich.table import Table
from rich.console import Group, Console

import pytest
import yaml

from qq_lib.constants import DATE_FORMAT
from qq_lib.error import QQError
from qq_lib.info import QQInfo, QQInformer
from qq_lib.pbs import QQPBS
from qq_lib.resources import QQResources
from qq_lib.states import BatchState, NaiveState, RealState


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
        script_name="script.sh",
        job_type="standard",
        input_machine="fake.machine.com",
        job_dir=Path("/shared/storage/"),
        job_state=NaiveState.RUNNING,
        submission_time=datetime.strptime("2025-09-21 12:00:00", DATE_FORMAT),
        stdout_file="stdout.log",
        stderr_file="stderr.log",
        resources=sample_resources,
        excluded_files=[Path("ignore.txt")],
        work_dir=Path("/scratch/job_12345.fake.server.com"),
    )


def test_to_dict_skips_none(sample_info):
    result = sample_info._toDict()
    assert "start_time" not in result
    assert "completion_time" not in result
    assert "job_exit_code" not in result

    assert result["job_id"] == "12345.fake.server.com"
    assert result["resources"]["ncpus"] == 8
    assert result["work_dir"] == "/scratch/job_12345.fake.server.com"
    assert result["job_dir"] == "/shared/storage"
    assert result["submission_time"] == "2025-09-21 12:00:00"


def test_to_dict_contains_all_non_none_fields(sample_info):
    result = sample_info._toDict()
    expected_fields = {
        "batch_system",
        "qq_version",
        "username",
        "job_id",
        "job_name",
        "script_name",
        "job_type",
        "input_machine",
        "job_dir",
        "job_state",
        "submission_time",
        "stdout_file",
        "stderr_file",
        "resources",
        "excluded_files",
    }
    assert expected_fields.issubset(result.keys())


def test_to_yaml_returns_string(sample_info):
    yaml_str = sample_info._toYaml()
    assert isinstance(yaml_str, str)


def test_to_yaml_contains_fields(sample_info):
    yaml_str = sample_info._toYaml()
    data: dict[str, Any] = yaml.safe_load(yaml_str)

    assert data["batch_system"] == "PBS"
    assert data["job_id"] == "12345.fake.server.com"
    assert data["job_name"] == "script.sh+025"
    assert data["resources"]["ncpus"] == 8


def test_to_yaml_skips_none_fields(sample_info):
    yaml_str = sample_info._toYaml()
    data: dict[str, Any] = yaml.safe_load(yaml_str)

    assert "start_time" not in data
    assert "completion_time" not in data
    assert "job_exit_code" not in data


def test_export_to_file_creates_file(sample_info, tmp_path):
    file_path = tmp_path / "qqinfo.yaml"
    sample_info.toFile(file_path)

    assert file_path.exists()
    assert file_path.is_file()


def test_export_to_file_contains_yaml(sample_info, tmp_path):
    file_path = tmp_path / "qqinfo.yaml"
    sample_info.toFile(file_path)

    content = file_path.read_text()

    assert content.startswith("# qq job info file")

    data: dict[str, str] = yaml.safe_load(content)

    assert data["job_id"] == sample_info.job_id
    assert data["job_name"] == sample_info.job_name
    assert data["batch_system"] == str(sample_info.batch_system)
    assert data["job_state"] == str(sample_info.job_state)

    resources_dict = sample_info.resources.toDict()
    assert data["resources"] == resources_dict

    assert data["excluded_files"] == [str(p) for p in sample_info.excluded_files]


def test_export_to_file_skips_none_fields(sample_info, tmp_path):
    file_path = tmp_path / "qqinfo.yaml"
    sample_info.toFile(file_path)

    content = file_path.read_text()
    data = yaml.safe_load(content)

    assert "start_time" not in data
    assert "completion_time" not in data
    assert "main_node" not in data
    assert "job_exit_code" not in data


def test_export_to_file_invalid_path(sample_info):
    invalid_file = Path("/this/path/does/not/exist/qqinfo.yaml")

    with pytest.raises(QQError, match="Cannot create or write to file"):
        sample_info.toFile(invalid_file)


def test_from_dict_roundtrip(sample_info):
    # convert to dict and back
    data = sample_info._toDict()
    reconstructed = QQInfo._fromDict(data)

    # basic fields
    for field_name in [
        "batch_system",
        "qq_version",
        "username",
        "job_id",
        "job_name",
        "script_name",
        "job_type",
        "input_machine",
        "job_dir",
        "job_state",
        "submission_time",
        "stdout_file",
        "stderr_file",
    ]:
        assert getattr(reconstructed, field_name) == getattr(sample_info, field_name)
        assert type(getattr(reconstructed, field_name)) == type(getattr(sample_info, field_name))

    # resources
    assert isinstance(reconstructed.resources, QQResources)
    assert reconstructed.resources.ncpus == sample_info.resources.ncpus
    assert reconstructed.resources.work_dir == sample_info.resources.work_dir

    # optional fields
    for optional_field in [
        "start_time",
        "main_node",
        "completion_time",
        "job_exit_code",
    ]:
        value = object.__getattribute__(reconstructed, optional_field)
        assert value is None

    assert getattr(reconstructed, "work_dir") == getattr(sample_info, "work_dir")

    # excluded files
    assert reconstructed.excluded_files == [Path(p) for p in sample_info.excluded_files]


def test_from_dict_multiple_excluded_files(sample_info):
    sample_info.excluded_files.append(Path("excluded2.txt"))
    sample_info.excluded_files.append(Path("excluded3.txt"))

    # convert to dict and back
    data = sample_info._toDict()
    reconstructed = QQInfo._fromDict(data)

    assert reconstructed.excluded_files == [Path(p) for p in sample_info.excluded_files]


def test_from_dict_with_empty_resources(sample_info):
    data = sample_info._toDict()
    data["resources"] = {}

    reconstructed = QQInfo._fromDict(data)
    assert isinstance(reconstructed.resources, QQResources)


def test_from_dict_empty_excluded(sample_info):
    data = sample_info._toDict()
    data["excluded_files"] = []

    reconstructed = QQInfo._fromDict(data)
    assert len(reconstructed.excluded_files) == 0


def test_load_from_file(tmp_path, sample_info):
    file_path = tmp_path / "qqinfo.yaml"

    sample_info.toFile(file_path)

    loaded_info = QQInfo.fromFile(file_path)

    assert loaded_info.job_id == sample_info.job_id
    assert loaded_info.job_name == sample_info.job_name
    assert loaded_info.resources.ncpus == sample_info.resources.ncpus


def test_load_from_file_missing(tmp_path):
    missing_file = tmp_path / "nonexistent.yaml"
    with pytest.raises(QQError, match="does not exist"):
        QQInfo.fromFile(missing_file)


def test_from_file_invalid_yaml(tmp_path):
    file = tmp_path / "bad.yaml"
    file.write_text("key: : value")

    with pytest.raises(QQError, match=r"Could not parse the qq info file"):
        QQInfo.fromFile(file)


def test_from_file_missing_required_field(tmp_path):
    file = tmp_path / "missing_field.yaml"
    data = {
        "batch_system": "PBS",
        "qq_version": "0.1.0",
        # "job_id" is missing
        "job_name": "script.sh+025",
        "script_name": "script.sh",
        "job_type": "standard",
        "input_machine": "fake.machine.com",
        "job_dir": "/shared/storage/",
        "job_state": "running",
        "submission_time": "2025-09-21 12:00:00",
        "stdout_file": "stdout.log",
        "stderr_file": "stderr.log",
        "resources": {"ncpus": 8, "work_dir": "scratch_local"},
        "start_time": "2025-02-21 12:30:00",
    }
    file.write_text(yaml.dump(data))

    with pytest.raises(QQError, match=r"Mandatory information missing"):
        QQInfo.fromFile(file)

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

    assert data["excluded_files"] == [str(p) for p in informer.info.excluded_files]

    assert "start_time" not in data
    assert "completion_time" not in data
    assert "main_node" not in data
    assert "job_exit_code" not in data
    
def test_set_running(sample_info):
    informer = QQInformer(sample_info)
    start_time = datetime(2025, 9, 22, 14, 30, 0)
    informer.setRunning(start_time, "main.node", Path("/scratch/new_dir"))

    assert informer.info.job_state == NaiveState.RUNNING
    assert informer.info.start_time == start_time
    assert informer.info.main_node == "main.node"
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
    informer.info.resources.work_dir = None
    assert not informer.useScratch()

def test_get_destination_exists(sample_info):
    informer = QQInformer(sample_info)
    informer.info.main_node = "random.node.org"

    assert informer.getDestination() == ("random.node.org", Path("/scratch/job_12345.fake.server.com"))

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


@pytest.mark.parametrize(
    "state,expected_first_keyword,expected_second_keyword",
    [
        (RealState.QUEUED, "queued", "queue"),
        (RealState.HELD, "held", "queue"),
        (RealState.SUSPENDED, "suspended", ""),
        (RealState.WAITING, "waiting", "queue"),
        (RealState.RUNNING, "running", "running"),
        (RealState.BOOTING, "booting", "preparing"),
        (RealState.KILLED, "killed", "killed"),
        (RealState.FAILED, "failed", "failed"),
        (RealState.FINISHED, "finished", "completed"),
        (RealState.IN_AN_INCONSISTENT_STATE, "inconsistent", "disagree"),
        (RealState.UNKNOWN, "unknown", "does not recognize"),
    ],
)
def test_informer_state_messages(sample_info, state, expected_first_keyword, expected_second_keyword):
    informer = QQInformer(sample_info)
    
    # Set required fields for running/finished/failed states
    if state == RealState.RUNNING:
        sample_info.main_node = "node1"

    if state == RealState.FAILED:
        sample_info.job_exit_code = 1
    
    start_time = datetime.now()
    end_time = start_time + timedelta(hours=1)

    first_msg, second_msg = informer._getStateMessages(state, start_time, end_time)

    assert expected_first_keyword.lower() in first_msg.lower()
    assert expected_second_keyword.lower() in second_msg.lower()

def test_create_job_status_panel(sample_info):
    informer = QQInformer(sample_info)

    panel_group: Group = informer.createJobStatusPanel()

    # group
    assert isinstance(panel_group, Group)
    assert len(panel_group.renderables) == 3

    # panel
    panel: Panel = panel_group.renderables[1]
    assert isinstance(panel, Panel)
    assert informer.info.job_id in panel.title.plain

    # table
    table: Table = panel.renderable
    assert isinstance(table, Table)
    assert len(table.columns) == 2

    # printed content
    console = Console(record=True)
    console.print(table)
    output = console.export_text()

    assert "Job state:" in output
    assert str(informer.getRealState()).lower() in output.lower()

@pytest.mark.parametrize("naive_state", list(NaiveState))
@pytest.mark.parametrize("batch_state", list(BatchState))
def test_get_real_state(sample_info, naive_state, batch_state):
    informer = QQInformer(sample_info)
    informer.info.job_state = naive_state

    with patch.object(QQInformer, "getBatchState", return_value=batch_state):
        assert informer.getRealState() == RealState.fromStates(naive_state, batch_state)