# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import time
from pathlib import Path

import pytest

from qq_lib.batch.vbs import QQVBS, VBSError, VBSJobInfo, VirtualBatchSystem, VirtualJob
from qq_lib.core.error import QQError
from qq_lib.properties.resources import QQResources
from qq_lib.properties.states import BatchState


def test_try_create_scratch_creates_directory(tmp_path):
    job = VirtualJob(
        job_id="1", script=tmp_path / "dummy.sh", use_scratch=True, node=tmp_path
    )
    job.tryCreateScratch()
    assert job.scratch is not None
    assert job.scratch.exists() and job.scratch.is_dir()
    assert job.scratch.name == "1"


def test_try_create_scratch_no_scratch(tmp_path):
    job = VirtualJob(
        job_id="1", script=tmp_path / "dummy.sh", use_scratch=False, node=tmp_path
    )
    job.tryCreateScratch()
    assert job.scratch is None


def test_submit_job_adds_job(tmp_path):
    vbs = VirtualBatchSystem()
    script = tmp_path / "dummy.sh"
    script.write_text("echo hello")
    job_id = vbs.submitJob(script, use_scratch=True)

    assert job_id in vbs.jobs
    job = vbs.jobs[job_id]
    assert job.script == script
    assert job.state == BatchState.QUEUED
    assert job.use_scratch is True


def test_run_job_starts_job(tmp_path):
    vbs = VirtualBatchSystem()
    script = tmp_path / "dummy.sh"
    script.write_text("#!/bin/bash\necho hello\n")
    script.chmod(script.stat().st_mode | 0o111)

    job_id = vbs.submitJob(script, use_scratch=True)
    vbs.runJob(job_id)

    time.sleep(0.3)

    job = vbs.jobs[job_id]
    assert job.state == BatchState.FINISHED
    assert "hello" in job.output
    assert job.node is not None
    assert job.node.exists()


def test_run_job_frozen_starts_job(tmp_path):
    vbs = VirtualBatchSystem()
    script = tmp_path / "dummy.sh"
    script.write_text("#!/bin/bash\necho hello\n")
    script.chmod(script.stat().st_mode | 0o111)

    job_id = vbs.submitJob(script, use_scratch=True)
    vbs.runJob(job_id, freeze=True)

    time.sleep(0.3)

    job = vbs.jobs[job_id]
    assert job.state == BatchState.RUNNING
    assert "" in job.output
    assert job.node is not None
    assert job.node.exists()

    vbs.releaseFrozenJob(job_id)

    time.sleep(0.3)

    job = vbs.jobs[job_id]
    assert job.state == BatchState.FINISHED
    assert "hello" in job.output
    assert job.node is not None
    assert job.node.exists()


def test_kill_job(tmp_path):
    vbs = VirtualBatchSystem()
    script = tmp_path / "sleep.sh"
    script.write_text("#!/bin/bash\nsleep 1\n")
    script.chmod(script.stat().st_mode | 0o111)

    job_id = vbs.submitJob(script, use_scratch=True)
    vbs.runJob(job_id)

    time.sleep(0.2)

    job = vbs.jobs[job_id]
    vbs.killJob(job_id, hard=False)
    assert job.state == BatchState.FAILED


def test_kill_job_hard(tmp_path):
    vbs = VirtualBatchSystem()
    script = tmp_path / "sleep.sh"
    script.write_text("#!/bin/bash\nsleep 1\n")
    script.chmod(script.stat().st_mode | 0o111)

    job_id = vbs.submitJob(script, use_scratch=True)
    vbs.runJob(job_id)

    time.sleep(0.2)

    job = vbs.jobs[job_id]
    vbs.killJob(job_id, hard=True)
    assert job.state == BatchState.FAILED


@pytest.mark.parametrize("state", [BatchState.FINISHED, BatchState.FAILED])
def test_kill_completed_job(tmp_path, state):
    vbs = VirtualBatchSystem()
    script = tmp_path / "sleep.sh"
    script.write_text(
        f"#!/bin/bash\necho hello; {'exit 0' if state == BatchState.FINISHED else 'exit 1'}\n"
    )
    script.chmod(script.stat().st_mode | 0o111)

    job_id = vbs.submitJob(script, use_scratch=True)
    vbs.runJob(job_id)

    time.sleep(0.3)
    assert vbs.jobs[job_id].state == state

    with pytest.raises(VBSError, match="is completed"):
        vbs.killJob(job_id, hard=False)


@pytest.fixture
def sample_resources():
    return QQResources(ncpus=1, work_dir="scratch_local")


def test_qqvbs_job_submit_and_get_job_info(tmp_path, sample_resources):
    # we need to always clear the jobs from the batch system, because batch system is global
    QQVBS._batch_system.clearJobs()
    script = tmp_path / "dummy.sh"
    script.write_text("#!/bin/bash\necho hello\n")
    script.chmod(script.stat().st_mode | 0o111)

    QQVBS.jobSubmit(sample_resources, "", script, "job")

    job = QQVBS._batch_system.jobs["0"]
    assert job.script == script
    assert job.state == BatchState.QUEUED

    info = QQVBS.getJobInfo("0")
    assert isinstance(info, VBSJobInfo)
    assert info.getState() == BatchState.QUEUED

    empty_info = QQVBS.getJobInfo("999")
    assert isinstance(empty_info, VBSJobInfo)
    assert empty_info.getState() == BatchState.UNKNOWN


def test_qqvbs_get_scratch_dir_success(tmp_path, sample_resources):
    QQVBS._batch_system.clearJobs()
    script = tmp_path / "dummy.sh"
    script.write_text("#!/bin/bash\necho hi")
    script.chmod(script.stat().st_mode | 0o111)

    QQVBS.jobSubmit(sample_resources, "", script, "job")
    job_id = "0"
    QQVBS._batch_system.runJob(job_id)

    time.sleep(0.2)
    scratch = QQVBS.getScratchDir(job_id)

    assert Path(scratch).exists()


def test_qqvbs_get_scratch_dir_job_not_exist():
    QQVBS._batch_system.clearJobs()
    with pytest.raises(QQError, match="does not exist"):
        QQVBS.getScratchDir("999")


def test_qqvbs_get_scratch_dir_no_scratch(tmp_path):
    QQVBS._batch_system.clearJobs()
    script = tmp_path / "dummy.sh"
    script.write_text("#!/bin/bash\necho hi")
    script.chmod(script.stat().st_mode | 0o111)

    # submit without scratch
    QQVBS._batch_system.submitJob(script, use_scratch=False)
    with pytest.raises(QQError, match="does not have a scratch"):
        QQVBS.getScratchDir("0")


def test_qqvbs_job_kill_and_job_kill_force(tmp_path, sample_resources):
    QQVBS._batch_system.clearJobs()
    script = tmp_path / "dummy.sh"
    script.write_text("#!/bin/bash\nsleep 2")
    script.chmod(script.stat().st_mode | 0o111)

    # normal kill
    QQVBS.jobSubmit(sample_resources, "", script, "job")
    job_id = "0"
    QQVBS._batch_system.runJob(job_id)
    time.sleep(0.3)

    QQVBS.jobKill(job_id)
    job = QQVBS._batch_system.jobs["0"]
    assert job.state == BatchState.FAILED
    assert job.process is None

    # forced kill
    QQVBS._batch_system.submitJob(script, sample_resources.useScratch())
    job_id2 = "1"
    QQVBS._batch_system.runJob(job_id2)
    time.sleep(0.3)

    QQVBS.jobKillForce(job_id2)
    job = QQVBS._batch_system.jobs["1"]
    assert job.state == BatchState.FAILED
    assert job.process is None


def test_job_kill_fails_if_finished(tmp_path, sample_resources):
    QQVBS._batch_system.clearJobs()
    script = tmp_path / "dummy.sh"
    script.write_text("#!/bin/bash\necho hello\n")
    script.chmod(script.stat().st_mode | 0o111)

    QQVBS.jobSubmit(sample_resources, "", script, "job")
    job_id = "0"
    QQVBS._batch_system.runJob(job_id)

    time.sleep(0.5)
    job = QQVBS._batch_system.jobs[job_id]
    assert job.state == BatchState.FINISHED

    with pytest.raises(QQError, match="is completed"):
        QQVBS.jobKill(job_id)


def test_job_kill_force_fails_if_finished(tmp_path, sample_resources):
    QQVBS._batch_system.clearJobs()
    script = tmp_path / "dummy.sh"
    script.write_text("#!/bin/bash\necho hello\n")
    script.chmod(script.stat().st_mode | 0o111)

    QQVBS.jobSubmit(sample_resources, "", script, "job")
    job_id = "0"
    QQVBS._batch_system.runJob(job_id)

    time.sleep(0.5)
    job = QQVBS._batch_system.jobs[job_id]
    assert job.state == BatchState.FINISHED

    with pytest.raises(QQError, match="is completed"):
        QQVBS.jobKillForce(job_id)


def test_qqvbs_navigate_to_destination(tmp_path):
    target = tmp_path / "workdir"
    target.mkdir()

    QQVBS.navigateToDestination(str(tmp_path), Path("workdir"))
    assert Path.cwd() == target


def test_qqvbs_navigate_to_destination_failure(tmp_path):
    with pytest.raises(QQError, match=r"Could not reach.*Could not change directory"):
        QQVBS.navigateToDestination(str(tmp_path), Path("does_not_exist"))


def test_vbs_job_info_get_job_state_returns_state(tmp_path, sample_resources):
    QQVBS._batch_system.clearJobs()
    script = tmp_path / "dummy.sh"
    script.write_text("#!/bin/bash\necho hello")
    script.chmod(script.stat().st_mode | 0o111)

    QQVBS._batch_system.submitJob(script, sample_resources.useScratch())
    job = QQVBS._batch_system.jobs["0"]

    info = VBSJobInfo(job)
    assert info.getState() == BatchState.QUEUED

    job.state = BatchState.RUNNING
    assert info.getState() == BatchState.RUNNING


def test_vbs_job_info_get_job_state_job_none():
    QQVBS._batch_system.clearJobs()

    info = VBSJobInfo(None)
    assert info.getState() == BatchState.UNKNOWN


def test_qqvbs_read_remote_file_and_write_remote_file(tmp_path):
    file_path = tmp_path / "testfile.txt"
    content = "Hello QQVBS"

    QQVBS.writeRemoteFile("dummy_host", file_path, content)
    assert file_path.exists()
    assert file_path.read_text() == content

    read_content = QQVBS.readRemoteFile("dummy_host", file_path)
    assert read_content == content


def test_qqvbs_read_remote_file_error(tmp_path):
    non_existent_file = tmp_path / "no_such_file.txt"
    with pytest.raises(QQError):
        QQVBS.readRemoteFile("dummy_host", non_existent_file)


def test_qqvbs_write_remote_file_error(tmp_path):
    file_path = tmp_path / "readonly.txt"

    file_path.mkdir()
    with pytest.raises(QQError):
        QQVBS.writeRemoteFile("dummy_host", file_path, "content")


def test_qqvbs_make_remote_dir(tmp_path):
    dir_path = tmp_path / "subdir"
    QQVBS.makeRemoteDir("dummy_host", dir_path)
    assert dir_path.exists() and dir_path.is_dir()


def test_qqvbs_make_remote_dir_existing(tmp_path):
    dir_path = tmp_path / "existing"
    dir_path.mkdir()
    # should not raise an error
    QQVBS.makeRemoteDir("dummy_host", dir_path)


def test_qqvbs_make_remote_dir_error(tmp_path):
    # try creating a directory inside a file
    file_path = tmp_path / "file.txt"
    file_path.write_text("dummy")
    with pytest.raises(QQError):
        QQVBS.makeRemoteDir("dummy_host", file_path)


def test_qqvbs_list_remote_dir(tmp_path):
    files = [tmp_path / f"file{i}.txt" for i in range(3)]
    dirs = [tmp_path / f"dir{i}" for i in range(2)]
    for f in files:
        f.write_text("data")
    for d in dirs:
        d.mkdir()

    result = QQVBS.listRemoteDir("dummy_host", tmp_path)
    result_set = {p.name for p in result}
    expected_set = {f.name for f in files + dirs}
    assert result_set == expected_set


def test_qqvbs_list_remote_dir_error(tmp_path):
    file_path = tmp_path / "file.txt"
    file_path.write_text("dummy")
    with pytest.raises(QQError):
        QQVBS.listRemoteDir("dummy_host", file_path)


def test_qqvbs_move_remote_files(tmp_path):
    src_files = [tmp_path / f"src{i}.txt" for i in range(3)]
    dst_files = [tmp_path / f"dst{i}.txt" for i in range(3)]
    for f in src_files:
        f.write_text("data")

    QQVBS.moveRemoteFiles("dummy_host", src_files, dst_files)
    # check that src_files no longer exist and dst_files exist
    for f in src_files:
        assert not f.exists()
    for f in dst_files:
        assert f.exists() and f.read_text() == "data"


def test_qqvbs_move_remote_files_length_mismatch(tmp_path):
    src_files = [tmp_path / "file1.txt"]
    dst_files = [tmp_path / "file2.txt", tmp_path / "file3.txt"]
    with pytest.raises(QQError):
        QQVBS.moveRemoteFiles("dummy_host", src_files, dst_files)


def test_qqvbs_sync_with_exclusions(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    (src / "a.txt").write_text("content of a")
    (src / "b.txt").write_text("content of b")
    (src / "c.txt").write_text("content of c")

    exclude_paths = [src / "b.txt"]

    QQVBS.syncWithExclusions(
        src, dest, "fake_src_host", "fake_dest_host", exclude_files=exclude_paths
    )

    expected_files = {"a.txt", "c.txt"}
    actual_files = {p.name for p in dest.iterdir()}
    assert actual_files == expected_files

    assert not (dest / "b.txt").exists()


def test_qqvbs_sync_selected(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    (src / "a.txt").write_text("content of a")
    (src / "b.txt").write_text("content of b")
    (src / "c.txt").write_text("content of c")

    include_paths = [src / "a.txt", src / "c.txt"]

    QQVBS.syncSelected(
        src, dest, "fake_src_host", "fake_dest_host", include_files=include_paths
    )

    actual_files = {p.name for p in dest.iterdir()}
    assert actual_files == {"a.txt", "c.txt"}

    assert not (dest / "b.txt").exists()
