# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from unittest.mock import patch

import pytest

from qq_lib.batch.interface.interface import CFG
from qq_lib.batch.pbs import QQPBS, PBSJobInfo
from qq_lib.cd.cder import QQCder
from qq_lib.core.error import QQError


def _make_jobinfo_with_info(info: dict[str, str]) -> PBSJobInfo:
    job = PBSJobInfo.__new__(PBSJobInfo)
    job._job_id = "1234"
    job._info = info
    return job


def test_cder_cd_success_pbs_o_workdir():
    env_vars = "PBS_O_WORKDIR=/pbs/job/dir,OTHER_VAR=123"
    job_info = _make_jobinfo_with_info({"Variable_List": env_vars})

    with patch.object(QQPBS, "getBatchJob", return_value=job_info):
        cder = QQCder(QQPBS, "1234")
        assert cder.cd() == "/pbs/job/dir"


def test_cder_cd_success_input_dir():
    env_vars = f"{CFG.env_vars.input_dir}=/qq/input/dir,OTHER_VAR=123"
    job_info = _make_jobinfo_with_info({"Variable_List": env_vars})

    with patch.object(QQPBS, "getBatchJob", return_value=job_info):
        cder = QQCder(QQPBS, "1234")
        assert cder.cd() == "/qq/input/dir"


def test_cder_cd_success_inf_input_dir():
    env_vars = "INF_INPUT_DIR=/infinity/input/dir,OTHER_VAR=123"
    job_info = _make_jobinfo_with_info({"Variable_List": env_vars})

    with patch.object(QQPBS, "getBatchJob", return_value=job_info):
        cder = QQCder(QQPBS, "1234")
        assert cder.cd() == "/infinity/input/dir"


def test_cder_cd_does_not_exist():
    job_info_empty = _make_jobinfo_with_info({})

    with (
        patch.object(QQPBS, "getBatchJob", return_value=job_info_empty),
        pytest.raises(QQError, match="does not exist"),
    ):
        cder = QQCder(QQPBS, "1234")
        cder.cd()
