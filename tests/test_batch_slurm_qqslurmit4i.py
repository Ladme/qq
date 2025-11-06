# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.batch.slurmit4i.qqslurm import QQSlurmIT4I
from qq_lib.core.error import QQError
from qq_lib.properties.resources import QQResources
from qq_lib.properties.size import Size


def test_qqslurmit4i_env_name_returns_expected_value():
    assert QQSlurmIT4I.envName() == "SlurmIT4I"


@patch("qq_lib.batch.slurmit4i.qqslurm.shutil.which", return_value="/usr/bin/it4ifree")
def test_qqslurmit4i_is_available_returns_true(mock_which):
    assert QQSlurmIT4I.isAvailable() is True
    mock_which.assert_called_once_with("it4ifree")


@patch("qq_lib.batch.slurmit4i.qqslurm.shutil.which", return_value=None)
def test_qqslurmit4i_is_available_returns_false(mock_which):
    assert QQSlurmIT4I.isAvailable() is False
    mock_which.assert_called_once_with("it4ifree")


@patch("qq_lib.batch.slurmit4i.qqslurm.QQResources.mergeResources")
@patch("qq_lib.batch.slurmit4i.qqslurm.QQSlurmIT4I._getDefaultResources")
@patch("qq_lib.batch.slurmit4i.qqslurm.default_resources_from_dict")
@patch("qq_lib.batch.slurmit4i.qqslurm.parse_slurm_dump_to_dictionary")
@patch("qq_lib.batch.slurmit4i.qqslurm.subprocess.run")
def test_qqslurmit4i_get_default_server_resources_merges_parsed_and_defaults(
    mock_run, mock_parse, mock_from_dict, mock_get_defaults, mock_merge
):
    mock_run.return_value = MagicMock(
        returncode=0, stdout="DefaultTime=2-00:00:00\nDefMemPerCPU=4G"
    )
    mock_parse.return_value = {"DefaultTime": "2-00:00:00", "DefMemPerCPU": "4G"}
    server_res = QQResources()
    default_res = QQResources()
    merged_res = QQResources()
    mock_from_dict.return_value = server_res
    mock_get_defaults.return_value = default_res
    mock_merge.return_value = merged_res

    result = QQSlurmIT4I._getDefaultServerResources()

    mock_run.assert_called_once()
    mock_parse.assert_called_once_with("DefaultTime=2-00:00:00\nDefMemPerCPU=4G", "\n")
    mock_from_dict.assert_called_once_with(
        {"DefaultTime": "2-00:00:00", "DefMemPerCPU": "4G"}
    )
    mock_get_defaults.assert_called_once()
    mock_merge.assert_called_once_with(server_res, default_res)
    assert result is merged_res


@patch("qq_lib.batch.slurmit4i.qqslurm.QQResources.mergeResources")
@patch("qq_lib.batch.slurmit4i.qqslurm.QQSlurmIT4I._getDefaultResources")
@patch("qq_lib.batch.slurmit4i.qqslurm.default_resources_from_dict")
@patch("qq_lib.batch.slurmit4i.qqslurm.parse_slurm_dump_to_dictionary")
@patch("qq_lib.batch.slurmit4i.qqslurm.subprocess.run")
def test_qqslurmit4i_get_default_server_resources_returns_empty_on_failure(
    mock_run, mock_parse, mock_from_dict, mock_get_defaults, mock_merge
):
    mock_run.return_value = MagicMock(returncode=1, stderr="err")

    result = QQSlurmIT4I._getDefaultServerResources()

    mock_run.assert_called_once()
    mock_parse.assert_not_called()
    mock_from_dict.assert_not_called()
    mock_get_defaults.assert_not_called()
    mock_merge.assert_not_called()
    assert isinstance(result, QQResources)
    assert result == QQResources()


@patch("qq_lib.batch.slurmit4i.qqslurm.subprocess.run")
@patch("qq_lib.batch.slurmit4i.qqslurm.os.chdir")
@patch(
    "qq_lib.batch.slurmit4i.qqslurm.Path.cwd", return_value=Path("/home/user/current")
)
def test_qqslurmit4i_resubmit_success(mock_cwd, mock_chdir, mock_run):
    mock_run.return_value = MagicMock(returncode=0)
    QQSlurmIT4I.resubmit(
        input_dir=Path("/home/user/jobdir"), command_line=["-q", "default"]
    )
    mock_cwd.assert_called_once()
    mock_chdir.assert_any_call(Path("/home/user/jobdir"))
    mock_chdir.assert_any_call(Path("/home/user/current"))
    mock_run.assert_called_once()


@patch("qq_lib.batch.slurmit4i.qqslurm.os.chdir", side_effect=OSError("failed to cd"))
@patch(
    "qq_lib.batch.slurmit4i.qqslurm.Path.cwd", return_value=Path("/home/user/current")
)
def test_qqslurmit4i_resubmit_raises_when_cannot_cd(mock_cwd, mock_chdir):
    with pytest.raises(QQError, match="Could not navigate to"):
        QQSlurmIT4I.resubmit(
            input_dir=Path("/home/user/jobdir"), command_line=["-q", "default"]
        )
    mock_cwd.assert_called_once()
    mock_chdir.assert_called_once_with(Path("/home/user/jobdir"))


@patch("qq_lib.batch.slurmit4i.qqslurm.subprocess.run")
@patch("qq_lib.batch.slurmit4i.qqslurm.os.chdir")
@patch(
    "qq_lib.batch.slurmit4i.qqslurm.Path.cwd", return_value=Path("/home/user/current")
)
def test_qqslurmit4i_resubmit_raises_when_command_fails(mock_cwd, mock_chdir, mock_run):
    mock_run.return_value = MagicMock(returncode=1, stderr="execution failed")
    with pytest.raises(QQError):
        QQSlurmIT4I.resubmit(
            input_dir=Path("/home/user/jobdir"), command_line=["-q", "default"]
        )
    mock_cwd.assert_called_once()
    mock_chdir.assert_any_call(Path("/home/user/jobdir"))
    mock_chdir.assert_any_call(Path("/home/user/current"))


@patch(
    "qq_lib.batch.slurmit4i.qqslurm.subprocess.run",
    return_value=MagicMock(returncode=0),
)
@patch(
    "qq_lib.batch.slurmit4i.qqslurm.os.chdir",
    side_effect=[None, OSError("cannot go back")],
)
@patch(
    "qq_lib.batch.slurmit4i.qqslurm.Path.cwd", return_value=Path("/home/user/current")
)
@patch("qq_lib.batch.slurmit4i.qqslurm.logger.warning")
def test_qqslurmit4i_resubmit_warns_when_cannot_return(
    mock_warn, mock_cwd, mock_chdir, mock_run
):
    QQSlurmIT4I.resubmit(input_dir=Path("/home/user/jobdir"), command_line=["ok.sh"])
    mock_cwd.assert_called_once()
    assert mock_chdir.call_count == 2
    mock_chdir.assert_any_call(Path("/home/user/jobdir"))
    mock_chdir.assert_any_call(Path("/home/user/current"))
    mock_run.assert_called_once()
    mock_warn.assert_called_once()


def test_qqslurmit4i_is_shared_returns_true():
    assert QQSlurmIT4I.isShared(Path.cwd())


@patch(
    "qq_lib.batch.slurmit4i.qqslurm.QQSlurmIT4I._getDefaultServerResources",
    return_value=QQResources(),
)
def test_qqslurmit4i_transform_resources_valid_work_dir_scratch(mock_get_defaults):
    provided = QQResources(work_dir="scratch")
    result = QQSlurmIT4I.transformResources("default", provided)
    mock_get_defaults.assert_called_once()
    assert result.work_dir == "scratch"


@patch(
    "qq_lib.batch.slurmit4i.qqslurm.QQSlurmIT4I._getDefaultServerResources",
    return_value=QQResources(),
)
def test_qqslurmit4i_transform_resources_raises_when_no_work_dir(mock_get_defaults):
    provided = QQResources()
    with pytest.raises(
        QQError, match="Work-dir is not set after filling in default attributes"
    ):
        QQSlurmIT4I.transformResources("default", provided)
    mock_get_defaults.assert_called_once()


@patch("qq_lib.batch.slurmit4i.qqslurm.logger.warning")
@patch(
    "qq_lib.batch.slurmit4i.qqslurm.QQSlurmIT4I._getDefaultServerResources",
    return_value=QQResources(),
)
def test_qqslurmit4i_transform_resources_warns_when_work_size_set(
    mock_get_defaults, mock_warn
):
    provided = QQResources(work_dir="scratch", work_size=Size(10, "gb"))
    QQSlurmIT4I.transformResources("default", provided)
    mock_warn.assert_called_once()
    mock_get_defaults.assert_called_once()


@patch(
    "qq_lib.batch.slurmit4i.qqslurm.QQSlurmIT4I._getDefaultServerResources",
    return_value=QQResources(),
)
def test_qqslurmit4i_transform_resources_raises_for_unknown_work_dir(mock_get_defaults):
    provided = QQResources(work_dir="nonsense")
    with pytest.raises(
        QQError, match="Unknown working directory type specified: work-dir"
    ):
        QQSlurmIT4I.transformResources("default", provided)
    mock_get_defaults.assert_called_once()


@patch("qq_lib.batch.slurmit4i.qqslurm.QQBatchInterface.syncWithExclusions")
def test_qqslurmit4i_sync_with_exclusions_delegates_correctly(mock_sync):
    QQSlurmIT4I.syncWithExclusions(
        Path("/data/src"),
        Path("/data/dest"),
        "src_host",
        "dest_host",
        [Path("ignore.txt")],
    )
    mock_sync.assert_called_once_with(
        Path("/data/src"), Path("/data/dest"), None, None, [Path("ignore.txt")]
    )


@patch("qq_lib.batch.slurmit4i.qqslurm.QQBatchInterface.syncSelected")
def test_qqslurmit4i_sync_selected_delegates_correctly(mock_sync):
    QQSlurmIT4I.syncSelected(
        Path("/data/src"),
        Path("/data/dest"),
        "src_host",
        "dest_host",
        [Path("file.txt")],
    )
    mock_sync.assert_called_once_with(
        Path("/data/src"), Path("/data/dest"), None, None, [Path("file.txt")]
    )


@patch("qq_lib.batch.slurmit4i.qqslurm.shutil.move")
def test_qqslurmit4i_move_remote_files_moves_each_pair(mock_move):
    files = [Path("/data/a.txt"), Path("/data/b.txt")]
    moved_files = [Path("/data/a_moved.txt"), Path("/data/b_moved.txt")]

    QQSlurmIT4I.moveRemoteFiles("host", files, moved_files)

    assert mock_move.call_count == 2
    mock_move.assert_any_call(str(files[0]), str(moved_files[0]))
    mock_move.assert_any_call(str(files[1]), str(moved_files[1]))


def test_qqslurmit4i_move_remote_files_raises_on_length_mismatch():
    files = [Path("/data/a.txt")]
    moved_files = [Path("/data/a_moved.txt"), Path("/data/b_moved.txt")]
    with pytest.raises(
        QQError,
        match="The provided 'files' and 'moved_files' must have the same length.",
    ):
        QQSlurmIT4I.moveRemoteFiles("host", files, moved_files)


def test_qqslurmit4i_read_remote_file_reads_successfully(tmp_path):
    file = tmp_path / "file.txt"
    file.write_text("hello world")
    result = QQSlurmIT4I.readRemoteFile("host", file)
    assert result == "hello world"


def test_qqslurmit4i_read_remote_file_raises_on_missing_file(tmp_path):
    file = tmp_path / "missing.txt"
    with pytest.raises(QQError, match=f"Could not read file '{file}'"):
        QQSlurmIT4I.readRemoteFile("host", file)


def test_qqslurmit4i_write_remote_file_writes_successfully(tmp_path):
    file = tmp_path / "output.txt"
    QQSlurmIT4I.writeRemoteFile("host", file, "data content")
    assert file.read_text() == "data content"


def test_qqslurmit4i_write_remote_file_raises_on_readonly_dir(tmp_path):
    readonly_dir = tmp_path / "readonly"
    readonly_dir.mkdir()
    (readonly_dir / "file.txt").touch()
    file = readonly_dir / "file.txt"
    file.chmod(0o400)  # make read-only
    with pytest.raises(QQError, match=f"Could not write file '{file}'"):
        QQSlurmIT4I.writeRemoteFile("host", file, "cannot write")


def test_qqslurmit4i_make_remote_dir_creates_successfully(tmp_path):
    directory = tmp_path / "newdir"
    QQSlurmIT4I.makeRemoteDir("host", directory)
    assert directory.exists() and directory.is_dir()


def test_qqslurmit4i_make_remote_dir_raises_on_invalid_path(tmp_path):
    bad_parent = tmp_path / "bad"
    bad_parent.mkdir()
    bad_parent.chmod(0o400)
    bad_dir = bad_parent / "nested"

    with pytest.raises(QQError, match=f"Could not create a directory '{bad_dir}'"):
        QQSlurmIT4I.makeRemoteDir("host", bad_dir)


def test_qqslurmit4i_list_remote_dir_lists_successfully(tmp_path):
    (tmp_path / "a.txt").write_text("A")
    (tmp_path / "b.txt").write_text("B")
    result = QQSlurmIT4I.listRemoteDir("host", tmp_path)
    assert set(result) == {tmp_path / "a.txt", tmp_path / "b.txt"}


def test_qqslurmit4i_list_remote_dir_raises_on_invalid_path(tmp_path):
    bad_dir = tmp_path / "nonexistent"
    with pytest.raises(QQError, match=f"Could not list a directory '{bad_dir}'"):
        QQSlurmIT4I.listRemoteDir("host", bad_dir)


@patch("qq_lib.batch.slurmit4i.qqslurm.logger.info")
@patch("qq_lib.batch.slurmit4i.qqslurm.QQBatchInterface._navigateSameHost")
def test_qqslurmit4i_navigate_to_destination_calls_interface(mock_nav, mock_info):
    QQSlurmIT4I.navigateToDestination("host", Path("/data"))
    mock_info.assert_called_once()
    mock_nav.assert_called_once_with(Path("/data"))


@patch("qq_lib.batch.slurmit4i.qqslurm.getpass.getuser", return_value="user1")
@patch("qq_lib.batch.slurmit4i.qqslurm.Path.mkdir")
@patch.dict(os.environ, {"SLURM_JOB_ACCOUNT": "ACCT"}, clear=True)
def test_qqslurmit4i_get_scratch_dir_creates_and_returns_path(mock_mkdir, mock_user):
    result = QQSlurmIT4I.getScratchDir("123")
    assert str(result).endswith("/scratch/project/acct/user1/qq-jobs/job_123")
    mock_user.assert_called_once()
    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)


@patch.dict(os.environ, {}, clear=True)
def test_qqslurmit4i_get_scratch_dir_raises_when_no_account():
    with pytest.raises(QQError, match="No account is defined for job '123'"):
        QQSlurmIT4I.getScratchDir("123")


@patch("qq_lib.batch.slurmit4i.qqslurm.getpass.getuser", return_value="user2")
@patch("qq_lib.batch.slurmit4i.qqslurm.Path.mkdir", side_effect=OSError("disk error"))
@patch.dict(os.environ, {"SLURM_JOB_ACCOUNT": "ACCT2"}, clear=True)
def test_qqslurmit4i_get_scratch_dir_raises_on_mkdir_failure(mock_mkdir, mock_user):
    with pytest.raises(
        QQError, match="Could not create a scratch directory for job '456'"
    ):
        QQSlurmIT4I.getScratchDir("456")
    mock_user.assert_called_once()
    mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
