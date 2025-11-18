# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import getpass
import os
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from qq_lib.batch.slurmlumi.slurm import SlurmLumi
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError


@pytest.mark.parametrize("which_return,expected", [(None, False), ("some/path", True)])
def test_slurmlumi_is_available_depends_on_shutil(which_return, expected):
    with patch("qq_lib.batch.slurmlumi.slurm.shutil.which", return_value=which_return):
        assert SlurmLumi.isAvailable() is expected


@pytest.mark.parametrize("uses_scratch,expect_env_var", [(True, True), (False, False)])
def test_slurmlumi_job_submit_sets_env_var_conditionally(uses_scratch, expect_env_var):
    res = Mock()
    res.usesScratch.return_value = uses_scratch
    res.work_dir = "flash"
    env_vars = {}

    with patch(
        "qq_lib.batch.slurmlumi.slurm.SlurmIT4I.jobSubmit", return_value="JOB123"
    ) as mock_super:
        SlurmLumi.jobSubmit(
            res, "default", Path("script.sh"), "job", [], env_vars, None
        )

    if expect_env_var:
        assert env_vars[CFG.env_vars.lumi_scratch_type] == "flash"
    else:
        assert not env_vars
    mock_super.assert_called_once()


def test_slurmlumi_get_scratch_dir_raises_when_no_account(monkeypatch):
    monkeypatch.setattr(os, "environ", {})
    with pytest.raises(QQError, match="No account is defined for job '111'"):
        SlurmLumi.getScratchDir("111")


def test_slurmlumi_get_scratch_dir_raises_when_no_storage_type(monkeypatch):
    monkeypatch.setattr(os, "environ", {CFG.env_vars.slurm_job_account: "account"})
    with pytest.raises(
        QQError,
        match=f"Environment variable '{CFG.env_vars.lumi_scratch_type}' is not defined",
    ):
        SlurmLumi.getScratchDir("222")


def test_slurmlumi_get_scratch_dir_creates_directory(monkeypatch):
    os.environ[CFG.env_vars.slurm_job_account] = "account"
    os.environ[CFG.env_vars.lumi_scratch_type] = "scratch"
    monkeypatch.setattr(getpass, "getuser", lambda: "user")

    with patch.object(Path, "mkdir") as mock_mkdir:
        result = SlurmLumi.getScratchDir("333")

    assert isinstance(result, Path)
    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
    assert Path("/scratch/account/user/qq-jobs/job_333") == result


def test_slurmlumi_get_scratch_dir_raises_on_creation_error(monkeypatch):
    os.environ[CFG.env_vars.slurm_job_account] = "account"
    os.environ[CFG.env_vars.lumi_scratch_type] = "flash"
    monkeypatch.setattr(getpass, "getuser", lambda: "user")

    with (
        patch.object(Path, "mkdir", side_effect=Exception("fail")) as mock_mkdir,
        pytest.raises(
            QQError, match="Could not create a scratch directory for job '444'"
        ),
    ):
        SlurmLumi.getScratchDir("444")

    assert mock_mkdir.call_count == CFG.lumi_scratch_dir_attempts


@patch("qq_lib.batch.slurmlumi.slurm.getpass.getuser", return_value="userX")
@patch.dict(
    os.environ,
    {CFG.env_vars.slurm_job_account: "ACCT", CFG.env_vars.lumi_scratch_type: "scratch"},
    clear=True,
)
def test_slurmlumi_get_scratch_dir_third_attempt_succeeds(mock_user):
    mkdir_mock = MagicMock()
    mkdir_mock.side_effect = [
        OSError("fail 1"),
        OSError("fail 2"),
        None,  # third attempt succeeds
    ]

    with patch("qq_lib.batch.slurmlumi.slurm.Path.mkdir", mkdir_mock):
        result = SlurmLumi.getScratchDir("999")

    expected_path = "/scratch/acct/userX3/qq-jobs/job_999"
    assert str(result).endswith(expected_path)

    mock_user.assert_called_once()
    assert mkdir_mock.call_count == 3


def test_slurmlumi_get_supported_work_dir_types_returns_combined_list():
    expected = ["scratch", "flash", "input_dir", "job_dir"]
    assert SlurmLumi.getSupportedWorkDirTypes() == expected
