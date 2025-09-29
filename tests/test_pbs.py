# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

# ruff: noqa: W291

import os
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from qq_lib.batch import QQBatchInterface
from qq_lib.constants import SHARED_SUBMIT, SSH_TIMEOUT
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
    result = PBSJobInfo._parse_pbs_dump_to_dictionary(text)
    assert result == {}


def test_parse_pbs_dump_real_file(sample_dump_file):
    result = PBSJobInfo._parse_pbs_dump_to_dictionary(sample_dump_file)

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
    result = PBSJobInfo._parse_pbs_dump_to_dictionary(text)

    assert result.get("NORMAL") == "OK"

    assert "This is not a key-value" not in result
    assert "KEY" not in result


def test_get_job_state(sample_dump_file):
    pbs_job_info = object.__new__(PBSJobInfo)
    pbs_job_info._info = PBSJobInfo._parse_pbs_dump_to_dictionary(sample_dump_file)

    assert pbs_job_info.getJobState() == BatchState.RUNNING

    pbs_job_info._info["job_state"] = "Q"
    assert pbs_job_info.getJobState() == BatchState.QUEUED

    pbs_job_info._info["job_state"] = "F"
    assert pbs_job_info.getJobState() == BatchState.FINISHED

    pbs_job_info._info["job_state"] = "z"
    assert pbs_job_info.getJobState() == BatchState.UNKNOWN


@pytest.fixture
def resources():
    return QQResources(ncpus=4, work_dir="scratch_local", work_size="16gb")


def test_translate_submit(resources):
    script = "myscript.sh"
    queue = "default"
    cmd = QQPBS._translateSubmit(resources, queue, script)

    assert (
        cmd
        == f"qsub -q {queue} -j eo -e myscript.qqout -V -l ncpus={resources.ncpus},{resources.work_dir}={resources.work_size} myscript.sh"
    )


def test_translate_resources(resources):
    trans = QQPBS._translateResources(resources)
    assert trans == [
        f"ncpus={resources.ncpus}",
        f"{resources.work_dir}={resources.work_size}",
    ]


def test_translate_work_dir(resources):
    assert (
        QQPBS._translateWorkDir(resources)
        == f"{resources.work_dir}={resources.work_size}"
    )
    assert QQPBS._translateWorkDir(QQResources()) is None


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


def test_set_shared_sets_env_var():
    os.environ.pop(SHARED_SUBMIT, None)

    # patch _isShared to return True
    with patch.object(QQPBS, "_isShared", return_value=True):
        QQPBS._setShared()
        assert os.environ.get(SHARED_SUBMIT) == "true"

    # clean up
    os.environ.pop(SHARED_SUBMIT, None)


def test_set_shared_does_not_set_env_var():
    os.environ.pop(SHARED_SUBMIT, None)

    # patch _isShared to return False
    with patch.object(QQPBS, "_isShared", return_value=False):
        QQPBS._setShared()
        assert SHARED_SUBMIT not in os.environ


def test_sync_directories_shared_storage_sets_local(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = [Path("file1"), Path("file2")]

    monkeypatch.setenv(SHARED_SUBMIT, "true")

    with patch.object(QQBatchInterface, "syncDirectories") as mock_sync:
        QQPBS.syncDirectories(src_dir, dest_dir, "host1", "host2", exclude_files)
        mock_sync.assert_called_once_with(src_dir, dest_dir, None, None, exclude_files)

    monkeypatch.delenv(SHARED_SUBMIT)


def test_sync_directories_local_src(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = [Path("file1")]
    local_host = "myhost"

    monkeypatch.setenv(SHARED_SUBMIT, "")

    with (
        patch.object(QQBatchInterface, "syncDirectories") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        # source is local, destination is remote
        QQPBS.syncDirectories(
            src_dir, dest_dir, local_host, "remotehost", exclude_files
        )
        mock_sync.assert_called_once_with(
            src_dir, dest_dir, None, "remotehost", exclude_files
        )


def test_sync_directories_local_dest(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = []
    local_host = "myhost"

    monkeypatch.setenv(SHARED_SUBMIT, "")

    with (
        patch.object(QQBatchInterface, "syncDirectories") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        # destination is local, source is remote
        QQPBS.syncDirectories(
            src_dir, dest_dir, "remotehost", local_host, exclude_files
        )
        mock_sync.assert_called_once_with(
            src_dir, dest_dir, "remotehost", None, exclude_files
        )


def test_sync_directories_one_remote(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = None
    local_host = "myhost"

    monkeypatch.setenv(SHARED_SUBMIT, "")

    with (
        patch.object(QQBatchInterface, "syncDirectories") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        # source local, destination local -> uses None
        QQPBS.syncDirectories(src_dir, dest_dir, None, local_host, exclude_files)
        mock_sync.assert_called_once_with(src_dir, dest_dir, None, None, exclude_files)


def test_sync_directories_both_remote_raises(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = None

    monkeypatch.setenv(SHARED_SUBMIT, "")

    with (
        patch("socket.gethostname", return_value="localhost"),
        pytest.raises(QQError, match="cannot be both remote"),
    ):
        # both source and destination are remote and job directory is not shared
        QQPBS.syncDirectories(src_dir, dest_dir, "remote1", "remote2", exclude_files)


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
