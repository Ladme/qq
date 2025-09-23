# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
from pathlib import Path
import pytest
from qq_lib.resources import QQResources
from qq_lib.states import BatchState
from qq_lib.vbs import QQVBS, VBSError, VBSJobInfo, VirtualBatchSystem, VirtualJob
import time


def test_try_create_scratch_creates_directory(tmp_path):
    job = VirtualJob(job_id="1", script=tmp_path / "dummy.sh", use_scratch=True, node=tmp_path)
    job.tryCreateScratch()
    assert job.scratch.exists() and job.scratch.is_dir()
    assert job.scratch.name == "1"

def test_try_create_scratch_no_scratch(tmp_path):
    job = VirtualJob(job_id="1", script=tmp_path / "dummy.sh", use_scratch=False, node=tmp_path)
    job.tryCreateScratch()
    assert job.scratch is None

def test_submit_job_adds_job(tmp_path):
    vbs = VirtualBatchSystem()
    script = tmp_path / "dummy.sh"
    script.write_text("echo hello")
    vbs.submitJob(script, use_scratch=True)
    
    assert "0" in vbs.jobs
    job = vbs.jobs["0"]
    assert job.script == script
    assert job.state == BatchState.QUEUED
    assert job.use_scratch is True

def test_run_job_starts_job(tmp_path):
    vbs = VirtualBatchSystem()
    script = tmp_path / "dummy.sh"
    script.write_text("#!/bin/bash\necho hello\n")
    script.chmod(script.stat().st_mode | 0o111)

    vbs.submitJob(script, use_scratch=True)
    vbs.runJob("0")

    time.sleep(0.2)

    job = vbs.jobs["0"]
    assert job.state == BatchState.FINISHED
    assert "hello" in job.output
    assert job.node.exists()

def test_run_job_frozen_starts_job(tmp_path):
    vbs = VirtualBatchSystem()
    script = tmp_path / "dummy.sh"
    script.write_text("#!/bin/bash\necho hello\n")
    script.chmod(script.stat().st_mode | 0o111)

    vbs.submitJob(script, use_scratch=True)
    vbs.runJob("0", freeze = True)

    time.sleep(0.1)

    job = vbs.jobs["0"]
    assert job.state == BatchState.RUNNING
    assert "" in job.output
    assert job.node.exists()

    vbs.releaseFrozenJob("0")

    time.sleep(0.2)

    job = vbs.jobs["0"]
    assert job.state == BatchState.FINISHED
    assert "hello" in job.output
    assert job.node.exists()

def test_kill_job(tmp_path):
    vbs = VirtualBatchSystem()
    script = tmp_path / "sleep.sh"
    script.write_text("#!/bin/bash\nsleep 1\n")
    script.chmod(script.stat().st_mode | 0o111)
    
    vbs.submitJob(script, use_scratch=True)
    vbs.runJob("0")

    time.sleep(0.1)

    job = vbs.jobs["0"]
    vbs.killJob("0", hard=False)
    assert job.state == BatchState.FINISHED

def test_kill_job_hard(tmp_path):
    vbs = VirtualBatchSystem()
    script = tmp_path / "sleep.sh"
    script.write_text("#!/bin/bash\nsleep 1\n")
    script.chmod(script.stat().st_mode | 0o111)
    
    vbs.submitJob(script, use_scratch=True)
    vbs.runJob("0")

    time.sleep(0.1)

    job = vbs.jobs["0"]
    vbs.killJob("0", hard=True)
    assert job.state == BatchState.FINISHED

def test_kill_finished_job(tmp_path):
    vbs = VirtualBatchSystem()
    script = tmp_path / "sleep.sh"
    script.write_text("#!/bin/bash\necho hello\n")
    script.chmod(script.stat().st_mode | 0o111)
    
    vbs.submitJob(script, use_scratch=True)
    vbs.runJob("0")

    time.sleep(0.2)
    assert vbs.jobs["0"].state == BatchState.FINISHED

    with pytest.raises(VBSError, match="is finished"):
        vbs.killJob("0", hard=False)

@pytest.fixture
def sample_resources():
    return QQResources(ncpus=1, work_dir="scratch_local")

def test_qqvbs_job_submit_and_get_job_info(tmp_path, sample_resources):
    # we need to always clear the jobs from the batch system, because batch system is global
    QQVBS._batch_system.jobs.clear()
    script = tmp_path / "dummy.sh"
    script.write_text("#!/bin/bash\necho hello\n")
    script.chmod(script.stat().st_mode | 0o111)

    result = QQVBS.jobSubmit(sample_resources, "", script)
    assert result.exit_code == 0

    job = QQVBS._batch_system.jobs["0"]
    assert job.script == script
    assert job.state == BatchState.QUEUED

    info = QQVBS.getJobInfo("0")
    assert isinstance(info, VBSJobInfo)
    assert info.getJobState() == BatchState.QUEUED

    empty_info = QQVBS.getJobInfo("999")
    assert isinstance(empty_info, VBSJobInfo)
    assert empty_info.getJobState() == BatchState.UNKNOWN


def test_qqvbs_get_scratch_dir_success(tmp_path, sample_resources):
    QQVBS._batch_system.jobs.clear()
    script = tmp_path / "dummy.sh"
    script.write_text("#!/bin/bash\necho hi")
    script.chmod(script.stat().st_mode | 0o111)

    QQVBS.jobSubmit(sample_resources, "", script)
    job_id = "0"
    QQVBS._batch_system.runJob(job_id)

    time.sleep(0.1)
    result = QQVBS.getScratchDir(job_id)

    assert result.exit_code == 0
    assert Path(result.success_message).exists()

def test_qqvbs_get_scratch_dir_job_not_exist():
    QQVBS._batch_system.jobs.clear()
    result = QQVBS.getScratchDir("999")
    assert result.exit_code != 0
    assert "does not exist" in result.error_message

def test_qqvbs_get_scratch_dir_no_scratch(tmp_path):
    QQVBS._batch_system.jobs.clear()
    script = tmp_path / "dummy.sh"
    script.write_text("#!/bin/bash\necho hi")
    script.chmod(script.stat().st_mode | 0o111)

    # submit without scratch
    QQVBS._batch_system.submitJob(script, use_scratch=False)
    result = QQVBS.getScratchDir("0")

    assert result.exit_code != 0
    assert "does not have a scratch" in result.error_message


def test_qqvbs_job_kill_and_job_kill_force(tmp_path, sample_resources):
    QQVBS._batch_system.jobs.clear()
    script = tmp_path / "dummy.sh"
    script.write_text("#!/bin/bash\nsleep 2")
    script.chmod(script.stat().st_mode | 0o111)

    # normal kill
    QQVBS.jobSubmit(sample_resources, "", script)
    job_id = "0"
    QQVBS._batch_system.runJob(job_id)
    time.sleep(0.1)

    result = QQVBS.jobKill(job_id)
    assert result.exit_code == 0
    job = QQVBS._batch_system.jobs["0"]
    assert job.state == BatchState.FINISHED
    assert job.process is None

    # forced kill
    QQVBS._batch_system.submitJob(script, sample_resources.useScratch())
    job_id2 = "1"
    QQVBS._batch_system.runJob(job_id2)
    time.sleep(0.1)

    result2 = QQVBS.jobKillForce(job_id2)
    assert result2.exit_code == 0
    job = QQVBS._batch_system.jobs["1"]
    assert job.state == BatchState.FINISHED
    assert job.process is None

def test_job_kill_fails_if_finished(tmp_path, sample_resources):
    QQVBS._batch_system.jobs.clear()
    script = tmp_path / "dummy.sh"
    script.write_text("#!/bin/bash\necho hello\n")
    script.chmod(script.stat().st_mode | 0o111)

    QQVBS.jobSubmit(sample_resources, "", script)
    job_id = "0"
    QQVBS._batch_system.runJob(job_id)

    time.sleep(0.3)
    job = QQVBS._batch_system.jobs[job_id]
    assert job.state == BatchState.FINISHED

    result = QQVBS.jobKill(job_id)
    assert result.exit_code == 1
    assert "is finished" in result.error_message

def test_job_kill_force_fails_if_finished(tmp_path, sample_resources):
    QQVBS._batch_system.jobs.clear()
    script = tmp_path / "dummy.sh"
    script.write_text("#!/bin/bash\necho hello\n")
    script.chmod(script.stat().st_mode | 0o111)

    QQVBS.jobSubmit(sample_resources, "", script)
    job_id = "0"
    QQVBS._batch_system.runJob(job_id)

    time.sleep(0.3)
    job = QQVBS._batch_system.jobs[job_id]
    assert job.state == BatchState.FINISHED

    result = QQVBS.jobKillForce(job_id)
    assert result.exit_code == 1
    assert "is finished" in result.error_message

def test_qqvbs_navigate_to_destination(tmp_path):
    target = tmp_path / "workdir"
    target.mkdir()

    result = QQVBS.navigateToDestination(str(tmp_path), Path("workdir"))
    assert result.exit_code == 0
    assert os.getcwd() == str(target)

def test_qqvbs_navigate_to_destination_failure(tmp_path):
    result = QQVBS.navigateToDestination(str(tmp_path), Path("does_not_exist"))
    assert result.exit_code == 1
    assert "No such file" in result.error_message or "does not exist" in result.error_message

def test_vbs_job_info_get_job_state_returns_state(tmp_path, sample_resources):
    QQVBS._batch_system.jobs.clear()
    script = tmp_path / "dummy.sh"
    script.write_text("#!/bin/bash\necho hello")
    script.chmod(script.stat().st_mode | 0o111)

    QQVBS._batch_system.submitJob(script, sample_resources.useScratch())
    job = QQVBS._batch_system.jobs["0"]

    info = VBSJobInfo(job)
    assert info.getJobState() == BatchState.QUEUED

    job.state = BatchState.RUNNING
    assert info.getJobState() == BatchState.RUNNING

def test_VBSJobInfo_getJobState_job_none():
    QQVBS._batch_system.jobs.clear()

    info = VBSJobInfo(None)
    assert info.getJobState() == BatchState.UNKNOWN