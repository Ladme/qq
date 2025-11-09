# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
import yaml

from qq_lib.batch.interface import BatchMeta
from qq_lib.batch.pbs import PBS
from qq_lib.core.error import QQError
from qq_lib.properties.info import CFG, QQInfo
from qq_lib.properties.job_type import QQJobType
from qq_lib.properties.resources import QQResources
from qq_lib.properties.states import NaiveState


@pytest.fixture(autouse=True)
def register():
    BatchMeta.register(PBS)


@pytest.fixture
def sample_resources():
    return QQResources(ncpus=8, work_dir="scratch_local")


@pytest.fixture
def sample_info(sample_resources):
    return QQInfo(
        batch_system=PBS,
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
        submission_time=datetime.strptime(
            "2025-09-21 12:00:00", CFG.date_formats.standard
        ),
        stdout_file="stdout.log",
        stderr_file="stderr.log",
        resources=sample_resources,
        excluded_files=[Path("ignore.txt")],
        command_line=["-q", "default", "script.sh"],
        work_dir=Path("/scratch/job_12345.fake.server.com"),
        account="fake-account",
    )


def test_to_dict_skips_none(sample_info):
    result = sample_info._toDict()
    assert "start_time" not in result
    assert "completion_time" not in result
    assert "job_exit_code" not in result

    assert result["job_id"] == "12345.fake.server.com"
    assert result["resources"]["ncpus"] == 8
    assert result["work_dir"] == "/scratch/job_12345.fake.server.com"
    assert result["input_dir"] == "/shared/storage"
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
        "input_dir",
        "job_state",
        "submission_time",
        "stdout_file",
        "stderr_file",
        "resources",
        "excluded_files",
        "account",
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
    assert data["account"] == "fake-account"


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
        "input_dir",
        "job_state",
        "submission_time",
        "stdout_file",
        "stderr_file",
    ]:
        assert getattr(reconstructed, field_name) == getattr(sample_info, field_name)
        assert type(getattr(reconstructed, field_name)) is type(
            getattr(sample_info, field_name)
        )

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
        "input_dir": "/shared/storage/",
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
