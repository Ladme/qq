# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Any
from unittest.mock import patch

import pytest
import yaml
from click.testing import CliRunner

from qq_lib.batch import QQBatchMeta
from qq_lib.constants import DATE_FORMAT
from qq_lib.error import QQError
from qq_lib.info import QQInfo, QQInformer, info
from qq_lib.pbs import QQPBS
from qq_lib.resources import QQResources
from qq_lib.states import BatchState, NaiveState, RealState
from qq_lib.submit import QQSubmitter, submit
from qq_lib.vbs import QQVBS


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

    assert informer.info.excluded_files is not None
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
    informer.info.resources.work_dir = "job_dir"
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


def test_info_no_jobs_integration(tmp_path):
    QQVBS._batch_system.clearJobs()
    runner = CliRunner()

    with runner.isolated_filesystem(temp_dir=tmp_path):
        os.chdir(tmp_path)

        result_info = runner.invoke(info)
        assert result_info.exit_code == 91
        assert "No qq job info file found" in result_info.stderr


def test_info_basic_integration(tmp_path):
    QQVBS._batch_system.clearJobs()
    runner = CliRunner()
    script_file = tmp_path / "test_script.sh"
    script_file.write_text("#!/bin/bash\necho Hello\n")
    script_file.chmod(0o755)

    with runner.isolated_filesystem(temp_dir=tmp_path):
        os.chdir(tmp_path)

        # submit the job using VBS
        with patch.object(QQSubmitter, "_hasValidShebang", return_value=True):
            result_submit = runner.invoke(
                submit,
                ["-q", "default", str(script_file), "--batch-system", "VBS"],
            )
        assert result_submit.exit_code == 0

        result_info = runner.invoke(info)
        assert result_info.exit_code == 0
        assert "queued" in result_info.stdout

        # run the job (frozen)
        info_file = tmp_path / "test_script.qqinfo"
        informer = QQInformer.fromFile(info_file)
        job_id = informer.info.job_id
        QQVBS._batch_system.runJob(job_id, freeze=True)

        result_info = runner.invoke(info)
        assert result_info.exit_code == 0
        assert "booting" in result_info.stdout

        # set the info file state to running
        informer.setRunning(datetime.now(), "fake.node.org", "/fake/path/to/work_dir")
        informer.toFile(info_file)

        result_info = runner.invoke(info)
        assert result_info.exit_code == 0
        assert "running" in result_info.stdout

        # unfreeze the job
        QQVBS._batch_system.releaseFrozenJob(job_id)

        sleep(0.3)

        # set the info file to finished
        informer.setFinished(datetime.now())
        informer.toFile(info_file)

        result_info = runner.invoke(info)
        assert result_info.exit_code == 0
        assert "finished" in result_info.stdout

        # set the info file to failed
        informer.setFailed(datetime.now(), 1)
        informer.toFile(info_file)

        result_info = runner.invoke(info)
        assert result_info.exit_code == 0
        assert "failed" in result_info.stdout

        # set the info file to killed
        informer.setKilled(datetime.now())
        informer.toFile(info_file)

        result_info = runner.invoke(info)
        assert result_info.exit_code == 0
        assert "killed" in result_info.stdout


def test_info_multiple_jobs_integration(tmp_path):
    QQVBS._batch_system.clearJobs()
    runner = CliRunner()

    with runner.isolated_filesystem(temp_dir=tmp_path):
        os.chdir(tmp_path)

        # create scripts for jobs
        script_files = []
        for i in range(3):
            script_file = tmp_path / f"test_script{i}.sh"
            script_file.write_text("#!/bin/bash\necho Hello\n")
            script_file.chmod(0o755)
            script_files.append(script_file)

        # submit 3 jobs using VBS
        job_ids = []
        info_files = []
        with (
            patch.object(QQSubmitter, "_hasValidShebang", return_value=True),
            patch.object(QQSubmitter, "guardOrClear"),
        ):
            for i in range(3):
                result_submit = runner.invoke(
                    submit,
                    [
                        "--queue",
                        "default",
                        str(script_files[i]),
                        "--batch-system",
                        "VBS",
                    ],
                )
                assert result_submit.exit_code == 0
                info_file = tmp_path / f"test_script{i}.qqinfo"
                info_files.append(info_file)
                informer = QQInformer.fromFile(info_file)
                job_ids.append(informer.info.job_id)

        # set job 2 as running
        QQVBS._batch_system.runJob(job_ids[1], freeze=True)
        informer2 = QQInformer.fromFile(info_files[1])
        informer2.setRunning(datetime.now(), "fake.node.org", "/fake/path/to/work_dir")
        informer2.toFile(info_files[1])

        # set job 3 as finished
        QQVBS._batch_system.runJob(job_ids[2])
        sleep(0.2)
        informer3 = QQInformer.fromFile(info_files[2])
        informer3.setFinished(datetime.now())
        informer3.toFile(info_files[2])

        # check info command shows all three states
        result_info = runner.invoke(info)
        assert result_info.exit_code == 0
        stdout = result_info.stdout
        assert "queued" in stdout
        assert "running" in stdout
        assert "finished" in stdout
