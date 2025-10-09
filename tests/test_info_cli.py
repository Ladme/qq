# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import os
from datetime import datetime
from pathlib import Path
from time import sleep
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from qq_lib.batch.pbs import QQPBS, PBSJobInfo
from qq_lib.batch.vbs import QQVBS
from qq_lib.core.constants import INFO_FILE
from qq_lib.core.error import QQError
from qq_lib.info import info
from qq_lib.info.cli import _get_info_file_from_job_id
from qq_lib.info.informer import QQInformer
from qq_lib.properties.states import BatchState
from qq_lib.submit import submit
from qq_lib.submit.submitter import QQSubmitter


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
        print(result_info.stderr)
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
        informer.setRunning(
            datetime.now(), "fake.node.org", ["fake.node.org"], "/fake/path/to/work_dir"
        )
        informer.toFile(info_file)

        result_info = runner.invoke(info)
        assert result_info.exit_code == 0
        assert "running" in result_info.stdout

        # set the info file to finished
        informer.setFinished(datetime.now())
        informer.toFile(info_file)

        result_info = runner.invoke(info)
        assert result_info.exit_code == 0
        assert "exiting" in result_info.stdout

        # unfreeze the job
        QQVBS._batch_system.releaseFrozenJob(job_id)

        # wait for the job to finish
        sleep(0.3)

        result_info = runner.invoke(info)
        assert result_info.exit_code == 0
        assert "finished" in result_info.stdout

        # set the info file to failed
        informer.setFailed(datetime.now(), 1)
        informer.toFile(info_file)

        # set the job's batch state to failed
        QQVBS._batch_system.jobs[job_id].state = BatchState.FAILED

        result_info = runner.invoke(info)
        assert result_info.exit_code == 0
        assert "failed" in result_info.stdout

        # set the info file to killed
        informer.setKilled(datetime.now())
        informer.toFile(info_file)

        # we do not care about batch state for killed jobs

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
        informer2.setRunning(
            datetime.now(), "fake.node.org", ["fake.node.org"], "/fake/path/to/work_dir"
        )
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


def _make_jobinfo_with_info(info: dict[str, str]) -> PBSJobInfo:
    job = PBSJobInfo.__new__(PBSJobInfo)
    job._job_id = "1234"
    job._info = info
    return job


def test_get_info_file_from_job_id_success():
    with patch.object(
        QQPBS,
        "getJobInfo",
        return_value=_make_jobinfo_with_info(
            {
                "Variable_List": f"{INFO_FILE}=/path/to/info_file.qqinfo,SINGLE_PROPERTY,PBS_O_HOST=host.example.com,SCRATCH=/scratch/user/job_123456"
            }
        ),
    ):
        assert _get_info_file_from_job_id(QQPBS, "12345") == Path(
            "/path/to/info_file.qqinfo"
        )


def test_get_info_file_from_job_id_no_info():
    with (
        patch.object(
            QQPBS,
            "getJobInfo",
            return_value=_make_jobinfo_with_info(
                {
                    "Variable_List": "SINGLE_PROPERTY,PBS_O_HOST=host.example.com,SCRATCH=/scratch/user/job_123456"
                }
            ),
        ),
        pytest.raises(QQError, match="is not a qq job"),
    ):
        _get_info_file_from_job_id(QQPBS, "12345")


def test_get_info_file_from_job_id_nonexistent_job():
    with (
        patch.object(
            QQPBS,
            "getJobInfo",
            return_value=_make_jobinfo_with_info({}),
        ),
        pytest.raises(QQError, match="does not exist"),
    ):
        _get_info_file_from_job_id(QQPBS, "12345")
