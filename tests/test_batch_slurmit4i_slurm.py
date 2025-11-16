# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import os
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.batch.slurmit4i.slurm import SlurmIT4I
from qq_lib.core.config import CFG
from qq_lib.core.error import QQError
from qq_lib.properties.resources import Resources
from qq_lib.properties.size import Size


def test_slurmit4i_env_name_returns_expected_value():
    assert SlurmIT4I.envName() == "SlurmIT4I"


@patch("qq_lib.batch.slurmit4i.slurm.shutil.which", return_value="/usr/bin/it4ifree")
def test_slurmit4i_is_available_returns_true(mock_which):
    assert SlurmIT4I.isAvailable() is True
    mock_which.assert_called_once_with("it4ifree")


@patch("qq_lib.batch.slurmit4i.slurm.shutil.which", return_value=None)
def test_slurmit4i_is_available_returns_false(mock_which):
    assert SlurmIT4I.isAvailable() is False
    mock_which.assert_called_once_with("it4ifree")


@patch("qq_lib.batch.slurm.slurm.Resources.mergeResources")
@patch("qq_lib.batch.slurmit4i.slurm.SlurmIT4I._getDefaultResources")
@patch("qq_lib.batch.slurm.slurm.default_resources_from_dict")
@patch("qq_lib.batch.slurm.slurm.parse_slurm_dump_to_dictionary")
@patch("qq_lib.batch.slurm.slurm.subprocess.run")
def test_slurmit4i_get_default_server_resources_merges_parsed_and_defaults(
    mock_run, mock_parse, mock_from_dict, mock_get_defaults, mock_merge
):
    mock_run.return_value = MagicMock(
        returncode=0, stdout="DefaultTime=2-00:00:00\nDefMemPerCPU=4G"
    )
    mock_parse.return_value = {"DefaultTime": "2-00:00:00", "DefMemPerCPU": "4G"}
    server_res = Resources()
    default_res = Resources()
    merged_res = Resources()
    mock_from_dict.return_value = server_res
    mock_get_defaults.return_value = default_res
    mock_merge.return_value = merged_res

    result = SlurmIT4I._getDefaultServerResources()

    mock_run.assert_called_once()
    mock_parse.assert_called_once_with("DefaultTime=2-00:00:00\nDefMemPerCPU=4G", "\n")
    mock_from_dict.assert_called_once_with(
        {"DefaultTime": "2-00:00:00", "DefMemPerCPU": "4G"}
    )
    mock_get_defaults.assert_called_once()
    mock_merge.assert_called_once_with(server_res, default_res)
    assert result is merged_res


@patch("qq_lib.batch.slurm.slurm.Resources.mergeResources")
@patch("qq_lib.batch.slurmit4i.slurm.SlurmIT4I._getDefaultResources")
@patch("qq_lib.batch.slurm.slurm.default_resources_from_dict")
@patch("qq_lib.batch.slurm.slurm.parse_slurm_dump_to_dictionary")
@patch("qq_lib.batch.slurm.slurm.subprocess.run")
def test_slurmit4i_get_default_server_resources_returns_empty_on_failure(
    mock_run, mock_parse, mock_from_dict, mock_get_defaults, mock_merge
):
    mock_run.return_value = MagicMock(returncode=1, stderr="err")

    result = SlurmIT4I._getDefaultServerResources()

    mock_run.assert_called_once()
    mock_parse.assert_not_called()
    mock_from_dict.assert_not_called()
    mock_get_defaults.assert_not_called()
    mock_merge.assert_not_called()
    assert isinstance(result, Resources)
    assert result == Resources()


@patch("qq_lib.batch.slurmit4i.slurm.subprocess.run")
@patch("qq_lib.batch.slurmit4i.slurm.os.chdir")
def test_slurmit4i_resubmit_success(mock_chdir, mock_run):
    mock_run.return_value = MagicMock(returncode=0)
    SlurmIT4I.resubmit(
        input_dir=Path("/home/user/jobdir"), command_line=["-q", "default"]
    )
    mock_chdir.assert_called_once_with(Path("/home/user/jobdir"))
    mock_run.assert_called_once()


@patch("qq_lib.batch.slurmit4i.slurm.os.chdir", side_effect=OSError("failed to cd"))
def test_slurmit4i_resubmit_raises_when_cannot_cd(mock_chdir):
    with pytest.raises(QQError, match="Could not navigate to"):
        SlurmIT4I.resubmit(
            input_dir=Path("/home/user/jobdir"), command_line=["-q", "default"]
        )
    mock_chdir.assert_called_once_with(Path("/home/user/jobdir"))


@patch("qq_lib.batch.slurmit4i.slurm.subprocess.run")
@patch("qq_lib.batch.slurmit4i.slurm.os.chdir")
def test_slurmit4i_resubmit_raises_when_command_fails(mock_chdir, mock_run):
    mock_run.return_value = MagicMock(returncode=1, stderr="execution failed")
    with pytest.raises(QQError):
        SlurmIT4I.resubmit(
            input_dir=Path("/home/user/jobdir"), command_line=["-q", "default"]
        )
    mock_chdir.assert_called_once_with(Path("/home/user/jobdir"))


def test_slurmit4i_is_shared_returns_true():
    assert SlurmIT4I.isShared(Path.cwd())


@patch("qq_lib.batch.slurmit4i.slurm.SlurmQueue")
@patch(
    "qq_lib.batch.slurmit4i.slurm.SlurmIT4I._getDefaultServerResources",
    return_value=Resources(),
)
def test_slurmit4i_transform_resources_valid_work_dir_scratch(
    mock_get_defaults, mock_queue
):
    mock_instance = MagicMock()
    mock_queue.return_value = mock_instance
    mock_instance.getDefaultResources.return_value = Resources()

    provided = Resources(work_dir="scratch")
    result = SlurmIT4I.transformResources("default", provided)

    mock_get_defaults.assert_called_once()
    mock_queue.assert_called_once_with("default")
    mock_instance.getDefaultResources.assert_called_once()
    assert result.work_dir == "scratch"


@patch("qq_lib.batch.slurmit4i.slurm.SlurmQueue")
@patch(
    "qq_lib.batch.slurmit4i.slurm.SlurmIT4I._getDefaultServerResources",
    return_value=Resources(),
)
def test_slurmit4i_transform_resources_raises_when_no_work_dir(
    mock_get_defaults, mock_queue
):
    mock_instance = MagicMock()
    mock_queue.return_value = mock_instance
    mock_instance.getDefaultResources.return_value = Resources()

    provided = Resources()
    with pytest.raises(
        QQError, match="Work-dir is not set after filling in default attributes"
    ):
        SlurmIT4I.transformResources("default", provided)

    mock_get_defaults.assert_called_once()
    mock_queue.assert_called_once_with("default")
    mock_instance.getDefaultResources.assert_called_once()


@patch("qq_lib.batch.slurmit4i.slurm.logger.warning")
@patch("qq_lib.batch.slurmit4i.slurm.SlurmQueue")
@patch(
    "qq_lib.batch.slurmit4i.slurm.SlurmIT4I._getDefaultServerResources",
    return_value=Resources(),
)
def test_slurmit4i_transform_resources_warns_when_work_size_set(
    mock_get_defaults, mock_queue, mock_warn
):
    mock_instance = MagicMock()
    mock_queue.return_value = mock_instance
    mock_instance.getDefaultResources.return_value = Resources()

    provided = Resources(work_dir="scratch", work_size=Size(10, "gb"))
    SlurmIT4I.transformResources("default", provided)

    mock_warn.assert_called_once()
    mock_get_defaults.assert_called_once()
    mock_queue.assert_called_once_with("default")
    mock_instance.getDefaultResources.assert_called_once()


@patch("qq_lib.batch.slurmit4i.slurm.SlurmQueue")
@patch(
    "qq_lib.batch.slurmit4i.slurm.SlurmIT4I._getDefaultServerResources",
    return_value=Resources(),
)
def test_slurmit4i_transform_resources_raises_for_unknown_work_dir(
    mock_get_defaults, mock_queue
):
    mock_instance = MagicMock()
    mock_queue.return_value = mock_instance
    mock_instance.getDefaultResources.return_value = Resources()

    provided = Resources(work_dir="nonsense")
    with pytest.raises(
        QQError, match="Unknown working directory type specified: work-dir"
    ):
        SlurmIT4I.transformResources("default", provided)

    mock_get_defaults.assert_called_once()
    mock_queue.assert_called_once_with("default")
    mock_instance.getDefaultResources.assert_called_once()


@patch("qq_lib.batch.slurmit4i.slurm.BatchInterface.syncWithExclusions")
def test_slurmit4i_sync_with_exclusions_delegates_correctly(mock_sync):
    SlurmIT4I.syncWithExclusions(
        Path("/data/src"),
        Path("/data/dest"),
        "src_host",
        "dest_host",
        [Path("ignore.txt")],
    )
    mock_sync.assert_called_once_with(
        Path("/data/src"), Path("/data/dest"), None, None, [Path("ignore.txt")]
    )


@patch("qq_lib.batch.slurmit4i.slurm.BatchInterface.syncSelected")
def test_slurmit4i_sync_selected_delegates_correctly(mock_sync):
    SlurmIT4I.syncSelected(
        Path("/data/src"),
        Path("/data/dest"),
        "src_host",
        "dest_host",
        [Path("file.txt")],
    )
    mock_sync.assert_called_once_with(
        Path("/data/src"), Path("/data/dest"), None, None, [Path("file.txt")]
    )


@patch("qq_lib.batch.slurmit4i.slurm.shutil.move")
def test_slurmit4i_move_remote_files_moves_each_pair(mock_move):
    files = [Path("/data/a.txt"), Path("/data/b.txt")]
    moved_files = [Path("/data/a_moved.txt"), Path("/data/b_moved.txt")]

    SlurmIT4I.moveRemoteFiles("host", files, moved_files)

    assert mock_move.call_count == 2
    mock_move.assert_any_call(str(files[0]), str(moved_files[0]))
    mock_move.assert_any_call(str(files[1]), str(moved_files[1]))


def test_slurmit4i_move_remote_files_raises_on_length_mismatch():
    files = [Path("/data/a.txt")]
    moved_files = [Path("/data/a_moved.txt"), Path("/data/b_moved.txt")]
    with pytest.raises(
        QQError,
        match="The provided 'files' and 'moved_files' must have the same length.",
    ):
        SlurmIT4I.moveRemoteFiles("host", files, moved_files)


def test_slurmit4i_read_remote_file_reads_successfully(tmp_path):
    file = tmp_path / "file.txt"
    file.write_text("hello world")
    result = SlurmIT4I.readRemoteFile("host", file)
    assert result == "hello world"


def test_slurmit4i_read_remote_file_raises_on_missing_file(tmp_path):
    file = tmp_path / "missing.txt"
    with pytest.raises(QQError, match=f"Could not read file '{file}'"):
        SlurmIT4I.readRemoteFile("host", file)


def test_slurmit4i_write_remote_file_writes_successfully(tmp_path):
    file = tmp_path / "output.txt"
    SlurmIT4I.writeRemoteFile("host", file, "data content")
    assert file.read_text() == "data content"


def test_slurmit4i_write_remote_file_raises_on_readonly_dir(tmp_path):
    readonly_dir = tmp_path / "readonly"
    readonly_dir.mkdir()
    (readonly_dir / "file.txt").touch()
    file = readonly_dir / "file.txt"
    file.chmod(0o400)  # make read-only
    with pytest.raises(QQError, match=f"Could not write file '{file}'"):
        SlurmIT4I.writeRemoteFile("host", file, "cannot write")


def test_slurmit4i_make_remote_dir_creates_successfully(tmp_path):
    directory = tmp_path / "newdir"
    SlurmIT4I.makeRemoteDir("host", directory)
    assert directory.exists() and directory.is_dir()


def test_slurmit4i_make_remote_dir_raises_on_invalid_path(tmp_path):
    bad_parent = tmp_path / "bad"
    bad_parent.mkdir()
    bad_parent.chmod(0o400)
    bad_dir = bad_parent / "nested"

    with pytest.raises(QQError, match=f"Could not create a directory '{bad_dir}'"):
        SlurmIT4I.makeRemoteDir("host", bad_dir)


def test_slurmit4i_list_remote_dir_lists_successfully(tmp_path):
    (tmp_path / "a.txt").write_text("A")
    (tmp_path / "b.txt").write_text("B")
    result = SlurmIT4I.listRemoteDir("host", tmp_path)
    assert set(result) == {tmp_path / "a.txt", tmp_path / "b.txt"}


def test_slurmit4i_list_remote_dir_raises_on_invalid_path(tmp_path):
    bad_dir = tmp_path / "nonexistent"
    with pytest.raises(QQError, match=f"Could not list a directory '{bad_dir}'"):
        SlurmIT4I.listRemoteDir("host", bad_dir)


@patch("qq_lib.batch.slurmit4i.slurm.logger.info")
@patch("qq_lib.batch.slurmit4i.slurm.BatchInterface._navigateSameHost")
def test_slurmit4i_navigate_to_destination_calls_interface(mock_nav, mock_info):
    SlurmIT4I.navigateToDestination("host", Path("/data"))
    mock_info.assert_called_once()
    mock_nav.assert_called_once_with(Path("/data"))


@patch("qq_lib.batch.slurmit4i.slurm.getpass.getuser", return_value="user1")
@patch("qq_lib.batch.slurmit4i.slurm.Path.mkdir")
@patch.dict(os.environ, {"SLURM_JOB_ACCOUNT": "ACCT"}, clear=True)
def test_slurmit4i_get_scratch_dir_creates_and_returns_path(mock_mkdir, mock_user):
    result = SlurmIT4I.getScratchDir("123")
    assert str(result).endswith("/scratch/project/acct/user1/qq-jobs/job_123")
    mock_user.assert_called_once()
    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)


@patch.dict(os.environ, {}, clear=True)
def test_slurmit4i_get_scratch_dir_raises_when_no_account():
    with pytest.raises(QQError, match="No account is defined for job '123'"):
        SlurmIT4I.getScratchDir("123")


@patch("qq_lib.batch.slurmit4i.slurm.getpass.getuser", return_value="user2")
@patch("qq_lib.batch.slurmit4i.slurm.Path.mkdir", side_effect=OSError("disk error"))
@patch.dict(os.environ, {"SLURM_JOB_ACCOUNT": "ACCT2"}, clear=True)
def test_slurmit4i_get_scratch_dir_raises_on_mkdir_failure(mock_mkdir, mock_user):
    with pytest.raises(
        QQError, match="Could not create a scratch directory for job '456'"
    ):
        SlurmIT4I.getScratchDir("456")
    mock_user.assert_called_once()
    assert mock_mkdir.call_count == CFG.it4i_scratch_dir_attempts


@patch("qq_lib.batch.slurmit4i.slurm.getpass.getuser", return_value="userX")
@patch.dict(os.environ, {"SLURM_JOB_ACCOUNT": "ACCT"}, clear=True)
def test_slurmit4i_get_scratch_dir_third_attempt_succeeds(mock_user):
    mkdir_mock = MagicMock()
    mkdir_mock.side_effect = [
        OSError("fail 1"),
        OSError("fail 2"),
        None,  # third attempt succeeds
    ]

    with patch("qq_lib.batch.slurmit4i.slurm.Path.mkdir", mkdir_mock):
        result = SlurmIT4I.getScratchDir("999")

    expected_path = "/scratch/project/acct/userX3/qq-jobs/job_999"
    assert str(result).endswith(expected_path)

    mock_user.assert_called_once()
    assert mkdir_mock.call_count == 3


def test_slurm_delete_remote_dir_deletes_local(tmp_path):
    test_dir = tmp_path / "to_delete"
    test_dir.mkdir()
    (test_dir / "file.txt").write_text("content")

    assert test_dir.exists()

    SlurmIT4I.deleteRemoteDir("some_host", test_dir)

    # Ensure directory was removed
    assert not test_dir.exists()


def test_slurm_delete_remote_dir_raises_error_on_local_failure(tmp_path, monkeypatch):
    test_dir = tmp_path / "to_delete_fail"
    test_dir.mkdir()

    def mock_rmtree(_):
        raise PermissionError("access denied")

    monkeypatch.setattr(shutil, "rmtree", mock_rmtree)

    with pytest.raises(
        QQError, match=f"Could not delete directory '{test_dir}': access denied."
    ):
        SlurmIT4I.deleteRemoteDir("some_host", test_dir)
