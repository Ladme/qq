# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from unittest.mock import patch

import pytest
from click.testing import CliRunner

from qq_lib.batch.interface import QQBatchMeta
from qq_lib.batch.pbs import QQPBS, PBSJobInfo
from qq_lib.cd.cli import cd
from qq_lib.core.constants import INPUT_DIR


@pytest.fixture(autouse=True)
def register():
    QQBatchMeta._registry.clear()
    QQBatchMeta.register(QQPBS)


def _make_jobinfo_with_info(info: dict[str, str]) -> PBSJobInfo:
    job = PBSJobInfo.__new__(PBSJobInfo)
    job._job_id = "1234"
    job._info = info
    return job


def test_cd_command_success_pbs_o_workdir():
    runner = CliRunner()
    env_vars = "PBS_O_WORKDIR=/pbs/job/dir,OTHER_VAR=123"
    job_info = _make_jobinfo_with_info({"Variable_List": env_vars})

    with patch.object(QQPBS, "getJobInfo", return_value=job_info):
        result = runner.invoke(cd, ["1234"])
        print(result.stderr)
        assert result.exit_code == 0
        assert result.output.strip() == "/pbs/job/dir"


def test_cd_command_success_input_dir():
    runner = CliRunner()
    env_vars = f"{INPUT_DIR}=/qq/input/dir,OTHER_VAR=123"
    job_info = _make_jobinfo_with_info({"Variable_List": env_vars})

    with patch.object(QQPBS, "getJobInfo", return_value=job_info):
        result = runner.invoke(cd, ["1234"])
        print(result.stderr)
        assert result.exit_code == 0
        assert result.output.strip() == "/qq/input/dir"


def test_cd_command_job_does_not_exist():
    runner = CliRunner()
    job_info_empty = _make_jobinfo_with_info({})

    with patch.object(QQPBS, "getJobInfo", return_value=job_info_empty):
        result = runner.invoke(cd, ["1234"])
        assert result.exit_code == 91
