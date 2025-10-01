# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

# ruff: noqa: W291

import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.batch import QQBatchInterface
from qq_lib.constants import QQ_OUT_SUFFIX, SHARED_SUBMIT, SSH_TIMEOUT
from qq_lib.error import QQError
from qq_lib.pbs import QQPBS, PBSJobInfo
from qq_lib.resources import QQResources
from qq_lib.states import BatchState


@pytest.fixture
def sample_dump_file():
    return """
Job Id: 123456.fake-cluster.example.com
    Job_Name = example_job
    Job_Owner = user@EXAMPLE
    resources_used.cpupercent = 100
    resources_used.cput = 01:23:45
    resources_used.diag_messages = '{}'
    resources_used.mem = 102400kb
    resources_used.ncpus = 8
    resources_used.vmem = 102400kb
    resources_used.walltime = 02:00:00
    job_state = R
    queue = gpu
    server = fake-cluster.example.com
    ctime = Sun Sep 21 00:00:00 2025
    depend = afterany:123455.fake-cluster.example.com@fake-cluster.example.com
    Error_Path = /fake/path/job_123456.log
    exec_host = node1/8*8
    exec_host2 = node1.example.com:15002/8*8
    exec_vnode = (node1:ncpus=8:ngpus=1:mem=8192mb:scratch_local=8192mb)
    group_list = examplegroup
    Hold_Types = n
    Join_Path = oe
    Mail_Points = n
    mtime = Sun Sep 21 02:00:00 2025
    Output_Path = /fake/path/job_123456.log
    qtime = Sun Sep 21 00:00:00 2025
    Rerunable = False
    Resource_List.mem = 8gb
    Resource_List.mpiprocs = 8
    Resource_List.ncpus = 8
    Resource_List.ngpus = 1
    Resource_List.nodect = 1
    Resource_List.place = free
    Resource_List.scratch_local = 8gb
    Resource_List.select = 1:ncpus=8:ngpus=1:mpiprocs=8:mem=8gb:scratch_local=8gb:cl_two=true:ompthreads=1:node_owner=everybody
    Resource_List.walltime = 24:00:00
    stime = Sun Sep 21 00:00:00 2025
    session_id = 123456
    jobdir = /fake/home/user
    substate = 42
    Variable_List = QQ_DEBUG=true,QQ_ENV_SET=true,AMS_SITE_SUPPORT=linuxsupport@example.com,PBS_O_LOGNAME=user,PBS_O_QUEUE=gpu,PBS_O_HOST=host.example.com,SCRATCHDIR=/scratch/user/job_123456,SCRATCH=/scratch/user/job_123456,SINGULARITY_TMPDIR=/scratch/user/job_123456,SINGULARITY_CACHEDIR=/scratch/user/job_123456
    etime = Sun Sep 21 00:00:00 2025
    umask = 77
    run_count = 1
    eligible_time = 00:00:00
    project = _pbs_project_default
    Submit_Host = host.example.com
    credential_id = user@EXAMPLE
    credential_validity = Mon Sep 22 06:38:19 2025
"""


def test_parse_pbs_dump_empty_string():
    text = ""
    result = PBSJobInfo._parsePBSDumpToDictionary(text)
    assert result == {}


def test_parse_pbs_dump_real_file(sample_dump_file):
    result = PBSJobInfo._parsePBSDumpToDictionary(sample_dump_file)

    assert isinstance(result, dict)
    assert result["Job_Name"].strip() == "example_job"
    assert result["job_state"].strip() == "R"
    assert (
        result["Resource_List.select"].strip()
        == "1:ncpus=8:ngpus=1:mpiprocs=8:mem=8gb:scratch_local=8gb:cl_two=true:ompthreads=1:node_owner=everybody"
    )
    assert result["jobdir"].strip() == "/fake/home/user"
    assert result["resources_used.cpupercent"].strip() == "100"
    assert "QQ_DEBUG=true,QQ_ENV_SET=true," in result["Variable_List"]
    assert "SINGULARITY_CACHEDIR=/scratch/user/job_123456" in result["Variable_List"]


def test_parse_pbs_dump_nonsense_input():
    text = """
This is not a key-value
Just some random text
= = =
Another line without equal
KEY = 
=VALUE
NORMAL = OK
NOTCONTINUATION
"""
    result = PBSJobInfo._parsePBSDumpToDictionary(text)

    assert result.get("NORMAL") == "OK"

    assert "This is not a key-value" not in result
    assert "KEY" not in result


def test_get_job_state(sample_dump_file):
    pbs_job_info = object.__new__(PBSJobInfo)
    pbs_job_info._info = PBSJobInfo._parsePBSDumpToDictionary(sample_dump_file)

    assert pbs_job_info.getJobState() == BatchState.RUNNING

    pbs_job_info._info["job_state"] = "Q"
    assert pbs_job_info.getJobState() == BatchState.QUEUED

    pbs_job_info._info["job_state"] = "F"
    assert pbs_job_info.getJobState() == BatchState.FINISHED

    pbs_job_info._info["job_state"] = "F"
    pbs_job_info._info["Exit_status"] = " 0 "
    assert pbs_job_info.getJobState() == BatchState.FINISHED

    pbs_job_info._info["job_state"] = "F"
    pbs_job_info._info["Exit_status"] = " 3"
    assert pbs_job_info.getJobState() == BatchState.FAILED

    pbs_job_info._info["job_state"] = "z"
    assert pbs_job_info.getJobState() == BatchState.UNKNOWN


@pytest.fixture
def resources():
    return QQResources(
        nnodes=1, mem_per_cpu="1gb", ncpus=4, work_dir="scratch_local", work_size="16gb"
    )


def test_translate_kill_force():
    job_id = "123"
    cmd = QQPBS._translateKillForce(job_id)
    assert cmd == f"qdel -W force {job_id}"


def test_translate_kill():
    job_id = "123"
    cmd = QQPBS._translateKill(job_id)
    assert cmd == f"qdel {job_id}"


def test_navigate_success(tmp_path):
    directory = tmp_path

    with patch("subprocess.run") as mock_run:
        QQPBS.navigateToDestination("fake.host.org", directory)
        # check that subprocess was called properly
        mock_run.assert_called_once_with(
            [
                "ssh",
                "-o PasswordAuthentication=no",
                f"-o ConnectTimeout={SSH_TIMEOUT}",
                "fake.host.org",
                "-t",
                f"cd {directory} || exit {QQBatchInterface.CD_FAIL} && exec bash -l",
            ]
        )

        # should not raise


def test_is_shared_returns_false_for_local(monkeypatch, tmp_path):
    def fake_run(cmd, **kwargs):
        _ = cmd
        _ = kwargs

        class Result:
            returncode = 0

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)
    assert QQPBS._isShared(tmp_path) is False


def test_is_shared_returns_true_for_shared(monkeypatch, tmp_path):
    def fake_run(cmd, **kwargs):
        _ = cmd
        _ = kwargs

        class Result:
            returncode = 1

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert QQPBS._isShared(tmp_path) is True


def test_is_shared_passes_correct_command(monkeypatch, tmp_path):
    captured = {}

    def fake_run(cmd, **kwargs):
        _ = kwargs
        captured["cmd"] = cmd

        class Result:
            returncode = 0

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)

    QQPBS._isShared(tmp_path)

    assert captured["cmd"][0:2] == ["df", "-l"]
    assert Path(captured["cmd"][2]) == tmp_path


def test_shared_guard_sets_env_var():
    os.environ.pop(SHARED_SUBMIT, None)

    # patch _isShared to return True
    with patch.object(QQPBS, "_isShared", return_value=True):
        QQPBS._sharedGuard(QQResources(work_dir="scratch_local"))
        assert os.environ.get(SHARED_SUBMIT) == "true"

    # clean up
    os.environ.pop(SHARED_SUBMIT, None)


def test_shared_guard_does_not_set_env_var():
    os.environ.pop(SHARED_SUBMIT, None)

    # patch _isShared to return False
    with patch.object(QQPBS, "_isShared", return_value=False):
        QQPBS._sharedGuard(QQResources(work_dir="scratch_local"))
        assert SHARED_SUBMIT not in os.environ


def test_shared_guard_jobdir_does_not_raise():
    os.environ.pop(SHARED_SUBMIT, None)

    # patch _isShared to return True
    with patch.object(QQPBS, "_isShared", return_value=True):
        QQPBS._sharedGuard(QQResources(work_dir="job_dir"))
        assert os.environ.get(SHARED_SUBMIT) == "true"

    # clean up
    os.environ.pop(SHARED_SUBMIT, None)


def test_shared_guard_jobdir_raises():
    os.environ.pop(SHARED_SUBMIT, None)

    # patch _isShared to return False
    with (
        patch.object(QQPBS, "_isShared", return_value=False),
        pytest.raises(
            QQError,
            match="Job was requested to run directly in the submission directory",
        ),
    ):
        QQPBS._sharedGuard(QQResources(work_dir="job_dir"))
        assert SHARED_SUBMIT not in os.environ


def test_sync_with_exclusions_shared_storage_sets_local(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = [Path("file1"), Path("file2")]

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    with patch.object(QQBatchInterface, "syncWithExclusions") as mock_sync:
        QQPBS.syncWithExclusions(src_dir, dest_dir, "host1", "host2", exclude_files)
        mock_sync.assert_called_once_with(src_dir, dest_dir, None, None, exclude_files)

    monkeypatch.delenv(SHARED_SUBMIT)


def test_sync_with_exclusions_local_src(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = [Path("file1")]
    local_host = "myhost"

    monkeypatch.setenv(SHARED_SUBMIT, "")

    with (
        patch.object(QQBatchInterface, "syncWithExclusions") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        # source is local, destination is remote
        QQPBS.syncWithExclusions(
            src_dir, dest_dir, local_host, "remotehost", exclude_files
        )
        mock_sync.assert_called_once_with(
            src_dir, dest_dir, None, "remotehost", exclude_files
        )


def test_sync_with_exclusions_local_dest(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = []
    local_host = "myhost"

    monkeypatch.setenv(SHARED_SUBMIT, "")

    with (
        patch.object(QQBatchInterface, "syncWithExclusions") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        # destination is local, source is remote
        QQPBS.syncWithExclusions(
            src_dir, dest_dir, "remotehost", local_host, exclude_files
        )
        mock_sync.assert_called_once_with(
            src_dir, dest_dir, "remotehost", None, exclude_files
        )


def test_sync_with_exclusions_one_remote(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = None
    local_host = "myhost"

    monkeypatch.setenv(SHARED_SUBMIT, "")

    with (
        patch.object(QQBatchInterface, "syncWithExclusions") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        # source local, destination local -> uses None
        QQPBS.syncWithExclusions(src_dir, dest_dir, None, local_host, exclude_files)
        mock_sync.assert_called_once_with(src_dir, dest_dir, None, None, exclude_files)


def test_sync_with_exclusions_both_remote_raises(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = None

    monkeypatch.setenv(SHARED_SUBMIT, "")

    with (
        patch("socket.gethostname", return_value="localhost"),
        pytest.raises(QQError, match="cannot be both remote"),
    ):
        # both source and destination are remote and job directory is not shared
        QQPBS.syncWithExclusions(src_dir, dest_dir, "remote1", "remote2", exclude_files)


def test_sync_selected_shared_storage_sets_local(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    include_files = [Path("file1"), Path("file2")]

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    with patch.object(QQBatchInterface, "syncSelected") as mock_sync:
        QQPBS.syncSelected(src_dir, dest_dir, "host1", "host2", include_files)
        mock_sync.assert_called_once_with(src_dir, dest_dir, None, None, include_files)

    monkeypatch.delenv(SHARED_SUBMIT)


def test_sync_selected_local_src(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    include_files = [Path("file1")]
    local_host = "myhost"

    monkeypatch.setenv(SHARED_SUBMIT, "")

    with (
        patch.object(QQBatchInterface, "syncSelected") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        QQPBS.syncSelected(src_dir, dest_dir, local_host, "remotehost", include_files)
        mock_sync.assert_called_once_with(
            src_dir, dest_dir, None, "remotehost", include_files
        )


def test_sync_selected_local_dest(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    include_files = []
    local_host = "myhost"

    monkeypatch.setenv(SHARED_SUBMIT, "")

    with (
        patch.object(QQBatchInterface, "syncSelected") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        QQPBS.syncSelected(src_dir, dest_dir, "remotehost", local_host, include_files)
        mock_sync.assert_called_once_with(
            src_dir, dest_dir, "remotehost", None, include_files
        )


def test_sync_selected_one_remote(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    include_files = None
    local_host = "myhost"

    monkeypatch.setenv(SHARED_SUBMIT, "")

    with (
        patch.object(QQBatchInterface, "syncSelected") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        QQPBS.syncSelected(src_dir, dest_dir, None, local_host, include_files)
        mock_sync.assert_called_once_with(src_dir, dest_dir, None, None, include_files)


def test_sync_selected_both_remote_raises(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    include_files = None

    monkeypatch.setenv(SHARED_SUBMIT, "")

    with (
        patch("socket.gethostname", return_value="localhost"),
        pytest.raises(QQError, match="cannot be both remote"),
    ):
        QQPBS.syncSelected(src_dir, dest_dir, "remote1", "remote2", include_files)


def test_read_remote_file_shared_storage(tmp_path, monkeypatch):
    file_path = tmp_path / "testfile.txt"
    content = "Hello, QQ!"
    file_path.write_text(content)

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    result = QQPBS.readRemoteFile("remotehost", file_path)
    assert result == content

    monkeypatch.delenv(SHARED_SUBMIT)


def test_read_remote_file_shared_storage_file_missing(tmp_path, monkeypatch):
    file_path = tmp_path / "nonexistent.txt"

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    with pytest.raises(QQError, match="Could not read file"):
        QQPBS.readRemoteFile("remotehost", file_path)

    monkeypatch.delenv(SHARED_SUBMIT)


def test_read_remote_file_remote():
    file_path = Path("/remote/file.txt")
    with patch.object(
        QQBatchInterface, "readRemoteFile", return_value="data"
    ) as mock_read:
        result = QQPBS.readRemoteFile("remotehost", file_path)
        mock_read.assert_called_once_with("remotehost", file_path)
        assert result == "data"


def test_write_remote_file_shared_storage(tmp_path, monkeypatch):
    file_path = tmp_path / "output.txt"
    content = "Test content"

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    QQPBS.writeRemoteFile("remotehost", file_path, content)
    assert file_path.read_text() == content

    monkeypatch.delenv(SHARED_SUBMIT)


def test_write_remote_file_shared_storage_exception(tmp_path, monkeypatch):
    # using a directory instead of a file to cause write_text to fail
    dir_path = tmp_path / "dir"
    dir_path.mkdir()

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    with pytest.raises(QQError, match="Could not write file"):
        QQPBS.writeRemoteFile("remotehost", dir_path, "content")

    monkeypatch.delenv(SHARED_SUBMIT)


def test_write_remote_file_remote():
    file_path = Path("/remote/output.txt")
    content = "data"

    with patch.object(QQBatchInterface, "writeRemoteFile") as mock_write:
        QQPBS.writeRemoteFile("remotehost", file_path, content)
        mock_write.assert_called_once_with("remotehost", file_path, content)


def test_translate_work_dir_job_dir_returns_none():
    res = QQResources(nnodes=1, work_dir="job_dir")
    assert QQPBS._translateWorkDir(res) is None


def test_translate_work_dir_scratch_shm_returns_true_string():
    res = QQResources(nnodes=3, work_dir="scratch_shm")
    assert QQPBS._translateWorkDir(res) == "scratch_shm=true"


def test_translate_work_dir_work_size_divided_by_nnodes():
    res = QQResources(nnodes=2, work_dir="scratch_local", work_size="7mb")
    result = QQPBS._translateWorkDir(res)
    assert result == "scratch_local=4mb"


def test_translate_work_dir_work_size_per_cpu_and_ncpus():
    res = QQResources(
        nnodes=4, ncpus=5, work_dir="scratch_local", work_size_per_cpu="3mb"
    )
    result = QQPBS._translateWorkDir(res)
    # 3mb * 5 = 15mb, divided by 4 nodes = 4mb
    assert result == "scratch_local=4mb"


def test_translate_work_dir_missing_work_size_raises():
    res = QQResources(nnodes=2, ncpus=4, work_dir="scratch_local")
    with pytest.raises(QQError, match="work-size"):
        QQPBS._translateWorkDir(res)


def test_translate_work_dir_missing_ncpus_with_work_size_per_cpu_raises():
    res = QQResources(nnodes=2, work_dir="scratch_local", work_size_per_cpu="3mb")
    with pytest.raises(QQError, match="work-size"):
        QQPBS._translateWorkDir(res)


def test_translate_per_chunk_resources_nnones_missing_raises():
    res = QQResources(nnodes=None, ncpus=2, mem="4mb")
    with pytest.raises(QQError, match="nnodes"):
        QQPBS._translatePerChunkResources(res)


def test_translate_per_chunk_resources_nnones_zero_raises():
    res = QQResources(nnodes=0, ncpus=2, mem="4mb")
    with pytest.raises(QQError, match="nnodes"):
        QQPBS._translatePerChunkResources(res)


def test_translate_per_chunk_resources_ncpus_not_divisible_raises():
    res = QQResources(nnodes=3, ncpus=4, mem="4mb")
    with pytest.raises(QQError, match="ncpus"):
        QQPBS._translatePerChunkResources(res)


def test_translate_per_chunk_resources_ngpus_not_divisible_raises():
    res = QQResources(nnodes=2, ncpus=2, ngpus=3, mem="4mb")
    with pytest.raises(QQError, match="ngpus"):
        QQPBS._translatePerChunkResources(res)


def test_translate_per_chunk_resources_mem_division():
    res = QQResources(nnodes=2, ncpus=4, mem="7mb", work_dir="job_dir")
    result = QQPBS._translatePerChunkResources(res)
    assert "ncpus=2" in result
    assert "mem=4mb" in result


def test_translate_per_chunk_resources_mem_per_cpu_used():
    res = QQResources(nnodes=2, ncpus=4, mem_per_cpu="2mb", work_dir="job_dir")
    result = QQPBS._translatePerChunkResources(res)
    # 2mb * 4 / 2 = 4mb
    assert "mem=4mb" in result


def test_translate_per_chunk_resources_ngpus_included():
    res = QQResources(nnodes=3, ncpus=9, mem="8mb", ngpus=6, work_dir="job_dir")
    result = QQPBS._translatePerChunkResources(res)
    assert "ngpus=2" in result


def test_translate_per_chunk_resources_work_dir_translated():
    res = QQResources(
        nnodes=2, ncpus=4, mem="8mb", work_dir="scratch_local", work_size="1mb"
    )
    result = QQPBS._translatePerChunkResources(res)
    assert "scratch_local=512kb" in result


def test_translate_per_chunk_resources_missing_memory_raises():
    res = QQResources(nnodes=2, ncpus=4)
    with pytest.raises(QQError, match="mem"):
        QQPBS._translatePerChunkResources(res)


def test_parse_queue_info_empty_text_returns_empty_dict():
    text = ""
    result = QQPBS._parseQueueInfoToDictionary(text)
    assert result == {}


def test_parse_queue_info_only_non_default_lines_ignored():
    text = """
queue_type = Execution
Priority = 75
total_jobs = 308
"""
    result = QQPBS._parseQueueInfoToDictionary(text)
    assert result == {}


def test_parse_queue_info_extracts_default_resources():
    text = """
resources_max.ngpus = 99
resources_max.walltime = 24:00:00
resources_min.mem = 50mb
resources_default.ngpus = 1
resources_default.walltime = 12:00:00
resources_default.mem = 5gb
"""
    result = QQPBS._parseQueueInfoToDictionary(text)
    expected = {
        "ngpus": "1",
        "walltime": "12:00:00",
        "mem": "5gb",
    }
    assert result == expected


def test_parse_queue_info_ignores_extra_spaces():
    text = """
resources_default.ngpus =    2
resources_default.mem   = 10gb
"""
    result = QQPBS._parseQueueInfoToDictionary(text)
    expected = {
        "ngpus": "2",
        "mem": "10gb",
    }
    assert result == expected


def test_parse_queue_info_multiple_default_resources():
    text = """
resources_default.mem = 8gb
resources_default.ncpus = 16
resources_default.ngpus = 4
"""
    result = QQPBS._parseQueueInfoToDictionary(text)
    expected = {
        "mem": "8gb",
        "ncpus": "16",
        "ngpus": "4",
    }
    assert result == expected


def test_parse_queue_info_ignores_non_resource_default_lines():
    text = """
comment = Example queue
resources_default.mem = 2gb
enabled = True
"""
    result = QQPBS._parseQueueInfoToDictionary(text)
    expected = {"mem": "2gb"}
    assert result == expected


@pytest.mark.parametrize("queue_name", ["gpu", "cpu"])
def test_get_default_queue_resources_success(queue_name):
    mock_output = """
resources_default.mem = 4gb
resources_default.ncpus = 16
resources_default.ngpus = 2
resources_default.walltime = 12:00:00
resources_default.unknown_field = ignored
"""
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = mock_output

    with patch("qq_lib.pbs.subprocess.run", return_value=mock_result) as mock_run:
        res = QQPBS._getDefaultQueueResources(queue_name)

    mock_run.assert_called_once()
    assert isinstance(res, QQResources)
    assert str(res.mem) == "4gb"
    assert res.ncpus == 16
    assert res.ngpus == 2
    assert res.walltime == "12:00:00"
    assert not hasattr(res, "unknown_field")


def test_get_default_queue_resources_failure_returns_empty():
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stdout = ""

    with patch("qq_lib.pbs.subprocess.run", return_value=mock_result) as mock_run:
        res = QQPBS._getDefaultQueueResources("nonexistent_queue")

    mock_run.assert_called_once()

    assert isinstance(res, QQResources)
    for f in res.__dataclass_fields__:
        assert getattr(res, f) is None


def test_get_default_queue_resources_calls_parse_queue_info():
    mock_output = "resources_default.ncpus = 8\nresources_default.mem = 2gb\n"
    mock_result = MagicMock(returncode=0, stdout=mock_output)

    with (
        patch("qq_lib.pbs.subprocess.run", return_value=mock_result),
        patch.object(
            QQPBS,
            "_parseQueueInfoToDictionary",
            wraps=QQPBS._parseQueueInfoToDictionary,
        ) as mock_parse,
    ):
        res = QQPBS._getDefaultQueueResources("gpu")
        mock_parse.assert_called_once_with(mock_output)
        assert res.ncpus == 8
        assert str(res.mem) == "2gb"


def test_translate_submit_minimal_fields():
    res = QQResources(nnodes=1, ncpus=1, mem="1gb", work_dir="job_dir")
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e script{QQ_OUT_SUFFIX} -V -l ncpus=1,mem=1gb script.sh"
    )


def test_translate_submit_multiple_nodes():
    res = QQResources(nnodes=4, ncpus=8, mem="1gb", work_dir="job_dir")
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e script{QQ_OUT_SUFFIX} -V -l select=4:ncpus=2:mem=256mb script.sh"
    )


def test_translate_submit_with_walltime():
    res = QQResources(
        nnodes=1, ncpus=2, mem="2gb", walltime="1d24m121s", work_dir="job_dir"
    )
    assert (
        QQPBS._translateSubmit(res, "queue", "script.sh", "job")
        == f"qsub -N job -q queue -j eo -e script{QQ_OUT_SUFFIX} -V -l ncpus=2,mem=2gb -l walltime=24:26:01 script.sh"
    )


def test_translate_submit_with_walltime2():
    res = QQResources(
        nnodes=1, ncpus=2, mem="2gb", walltime="12:30:15", work_dir="job_dir"
    )
    assert (
        QQPBS._translateSubmit(res, "queue", "script.sh", "job")
        == f"qsub -N job -q queue -j eo -e script{QQ_OUT_SUFFIX} -V -l ncpus=2,mem=2gb -l walltime=12:30:15 script.sh"
    )


def test_translate_submit_work_dir_scratch_shm():
    res = QQResources(nnodes=1, ncpus=1, mem="8gb", work_dir="scratch_shm")
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e script{QQ_OUT_SUFFIX} -V -l ncpus=1,mem=8gb,scratch_shm=true script.sh"
    )


def test_translate_submit_scratch_local_work_size():
    res = QQResources(
        nnodes=2, ncpus=2, mem="4gb", work_dir="scratch_local", work_size="16gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e script{QQ_OUT_SUFFIX} -V -l select=2:ncpus=1:mem=2gb:scratch_local=8gb script.sh"
    )


def test_translate_submit_scratch_ssd_work_size():
    res = QQResources(
        nnodes=2, ncpus=2, mem="4gb", work_dir="scratch_ssd", work_size="16gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e script{QQ_OUT_SUFFIX} -V -l select=2:ncpus=1:mem=2gb:scratch_ssd=8gb script.sh"
    )


def test_translate_submit_scratch_shared_work_size():
    res = QQResources(
        nnodes=2, ncpus=2, mem="4gb", work_dir="scratch_shared", work_size="16gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e script{QQ_OUT_SUFFIX} -V -l select=2:ncpus=1:mem=2gb:scratch_shared=8gb script.sh"
    )


def test_translate_submit_work_size_per_cpu():
    res = QQResources(
        nnodes=1, ncpus=8, mem="4gb", work_dir="scratch_local", work_size_per_cpu="2gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e script{QQ_OUT_SUFFIX} -V -l ncpus=8,mem=4gb,scratch_local=16gb script.sh"
    )


def test_translate_submit_work_size_per_cpu_multiple_nodes():
    res = QQResources(
        nnodes=3, ncpus=3, mem="4gb", work_dir="scratch_local", work_size_per_cpu="2gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e script{QQ_OUT_SUFFIX} -V -l select=3:ncpus=1:mem=2gb:scratch_local=2gb script.sh"
    )


def test_translate_submit_mem_per_cpu():
    res = QQResources(
        nnodes=1, ncpus=4, mem_per_cpu="2gb", work_dir="scratch_local", work_size="10gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e script{QQ_OUT_SUFFIX} -V -l ncpus=4,mem=8gb,scratch_local=10gb script.sh"
    )


def test_translate_submit_mem_per_cpu_multiple_nodes():
    res = QQResources(
        nnodes=2, ncpus=4, mem_per_cpu="2gb", work_dir="scratch_local", work_size="20gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e script{QQ_OUT_SUFFIX} -V -l select=2:ncpus=2:mem=4gb:scratch_local=10gb script.sh"
    )


def test_translate_submit_mem_per_cpu_and_work_size_per_cpu():
    res = QQResources(
        nnodes=1,
        ncpus=4,
        mem_per_cpu="2gb",
        work_dir="scratch_local",
        work_size_per_cpu="5gb",
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e script{QQ_OUT_SUFFIX} -V -l ncpus=4,mem=8gb,scratch_local=20gb script.sh"
    )


def test_translate_submit_mem_per_cpu_and_work_size_per_cpu_multiple_nodes():
    res = QQResources(
        nnodes=2,
        ncpus=4,
        mem_per_cpu="2gb",
        work_dir="scratch_local",
        work_size_per_cpu="5gb",
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", "script.sh", "job")
        == f"qsub -N job -q gpu -j eo -e script{QQ_OUT_SUFFIX} -V -l select=2:ncpus=2:mem=4gb:scratch_local=10gb script.sh"
    )


def test_translate_submit_with_props():
    res = QQResources(
        nnodes=1,
        ncpus=1,
        mem="1gb",
        props={"vnode": "my_node", "infiniband": "true"},
        work_dir="job_dir",
    )
    assert (
        QQPBS._translateSubmit(res, "queue", "script.sh", "job")
        == f"qsub -N job -q queue -j eo -e script{QQ_OUT_SUFFIX} -V -l ncpus=1,mem=1gb,vnode=my_node,infiniband=true script.sh"
    )


def test_translate_submit_complex_case():
    res = QQResources(
        nnodes=3,
        ncpus=6,
        mem="5gb",
        ngpus=3,
        walltime="1h30m",
        work_dir="scratch_local",
        work_size_per_cpu="2gb",
        props={"cl_cluster": "true"},
    )
    assert QQPBS._translateSubmit(res, "gpu", "myscript.sh", "job") == (
        f"qsub -N job -q gpu -j eo -e myscript{QQ_OUT_SUFFIX} -V "
        f"-l select=3:ncpus=2:mem=2gb:ngpus=1:scratch_local=4gb:cl_cluster=true "
        f"-l walltime=1:30:00 myscript.sh"
    )


def test_build_resources_job_dir_warns_and_sets_work_dir():
    provided = QQResources(work_dir="job_dir", work_size="10gb")
    with (
        patch.object(QQPBS, "_getDefaultQueueResources", return_value=QQResources()),
        patch.object(QQPBS, "_getDefaultServerResources", return_value=QQResources()),
        patch.object(QQResources, "mergeResources", return_value=provided),
        patch("qq_lib.pbs.logger.warning") as mock_warning,
    ):
        res = QQPBS.buildResources("gpu", work_dir="job_dir", work_size="10gb")

    assert res.work_dir == "job_dir"

    called_args = mock_warning.call_args[0]
    assert "Setting work-size is not supported" in called_args[0]
    assert "job_dir" in called_args[0]


def test_build_resources_scratch_shm_warns_and_clears_work_size():
    provided = QQResources(work_dir="scratch_shm", work_size="10gb")
    with (
        patch.object(QQPBS, "_getDefaultQueueResources", return_value=QQResources()),
        patch.object(QQPBS, "_getDefaultServerResources", return_value=QQResources()),
        patch.object(QQResources, "mergeResources", return_value=provided),
        patch("qq_lib.pbs.logger.warning") as mock_warning,
    ):
        res = QQPBS.buildResources("gpu", work_dir="scratch_shm", work_size="10gb")

    assert res.work_dir == "scratch_shm"
    assert res.work_size is None

    called_args = mock_warning.call_args[0]
    assert "Setting work-size is not supported" in called_args[0]
    assert "scratch_shm" in called_args[0]


def test_build_resources_supported_scratch():
    for scratch in QQPBS.SUPPORTED_SCRATCHES:
        provided = QQResources(work_dir=scratch, work_size="10gb")
        with (
            patch.object(
                QQPBS, "_getDefaultQueueResources", return_value=QQResources()
            ),
            patch.object(
                QQPBS, "_getDefaultServerResources", return_value=QQResources()
            ),
            patch.object(QQResources, "mergeResources", return_value=provided),
        ):
            res = QQPBS.buildResources("gpu", work_dir=scratch, work_size="10gb")

        assert res.work_dir == scratch


def test_build_resources_supported_scratch_unnormalized():
    for scratch in QQPBS.SUPPORTED_SCRATCHES:
        provided = QQResources(
            work_dir=scratch.upper().replace("_", "-"), work_size="10gb"
        )
        with (
            patch.object(
                QQPBS, "_getDefaultQueueResources", return_value=QQResources()
            ),
            patch.object(
                QQPBS, "_getDefaultServerResources", return_value=QQResources()
            ),
            patch.object(QQResources, "mergeResources", return_value=provided),
        ):
            res = QQPBS.buildResources(
                "gpu", work_dir=scratch.upper().replace("_", "-"), work_size="10gb"
            )

        assert res.work_dir == scratch


def test_build_resources_unknown_work_dir_raises():
    provided = QQResources(work_dir="unknown_scratch")
    with (
        patch.object(QQPBS, "_getDefaultQueueResources", return_value=QQResources()),
        patch.object(QQPBS, "_getDefaultServerResources", return_value=QQResources()),
        patch.object(QQResources, "mergeResources", return_value=provided),
        pytest.raises(QQError, match="Unknown working directory type specified"),
    ):
        QQPBS.buildResources("gpu", work_dir="unknown_scratch")


def test_build_resources_missing_work_dir_raises():
    provided = QQResources(work_dir=None)
    with (
        patch.object(QQPBS, "_getDefaultQueueResources", return_value=QQResources()),
        patch.object(QQPBS, "_getDefaultServerResources", return_value=QQResources()),
        patch.object(QQResources, "mergeResources", return_value=provided),
        pytest.raises(
            QQError, match="Work-dir is not set after filling in default attributes"
        ),
    ):
        QQPBS.buildResources("gpu")
