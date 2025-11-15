# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.batch.interface import BatchInterface, BatchMeta
from qq_lib.batch.interface.interface import CFG
from qq_lib.batch.pbs import PBS
from qq_lib.core.error import QQError


def test_translate_ssh_command():
    host = "node1"
    directory = Path("/tmp/work")
    cmd = BatchInterface._translateSSHCommand(host, directory)
    assert cmd == [
        "ssh",
        "-o PasswordAuthentication=no",
        "-o GSSAPIAuthentication=yes",
        f"-o ConnectTimeout={CFG.timeouts.ssh}",
        host,
        "-t",
        f"cd {directory} || exit {BatchInterface.CD_FAIL} && exec bash -l",
    ]


def test_navigate_same_host_success(tmp_path):
    directory = tmp_path

    with patch("subprocess.run") as mock_run:
        BatchInterface._navigateSameHost(directory)
        # check that subprocess was called properly
        mock_run.assert_called_once_with(["bash"], cwd=directory)

        # should not raise


def test_navigate_same_host_error():
    # nonexistent directory
    directory = Path("/non/existent/directory")

    with (
        patch("subprocess.run") as mock_run,
        pytest.raises(QQError, match="Could not reach"),
    ):
        BatchInterface._navigateSameHost(directory)

        # check that subprocess was not called
        mock_run.assert_not_called()


def test_guess_pbs():
    BatchMeta._registry.clear()
    BatchMeta.register(PBS)

    with patch.object(PBS, "isAvailable", return_value=True):
        assert BatchMeta.guess() is PBS

    with (
        patch.object(PBS, "isAvailable", return_value=False),
        pytest.raises(QQError, match="Could not guess a batch system"),
    ):
        BatchMeta.guess()


def test_guess_empty_registry():
    BatchMeta._registry.clear()
    with pytest.raises(QQError, match="Could not guess a batch system"):
        BatchMeta.guess()


def test_from_str_success():
    BatchMeta._registry.clear()
    BatchMeta.register(PBS)

    assert BatchMeta.fromStr("PBS") is PBS


def test_from_str_pbs_not_registered():
    BatchMeta._registry.clear()

    with pytest.raises(QQError, match="No batch system registered"):
        BatchMeta.fromStr("PBS")


def test_from_str_none_registered():
    BatchMeta._registry.clear()

    with pytest.raises(QQError, match="No batch system registered"):
        BatchMeta.fromStr("PBS")


def test_env_var_or_guess_from_env_var_returns_value(monkeypatch):
    BatchMeta._registry.clear()
    BatchMeta.register(PBS)
    monkeypatch.setenv(CFG.env_vars.batch_system, "PBS")

    assert BatchMeta.fromEnvVarOrGuess() is PBS


def test_env_var_or_guess_from_env_var_not_set_calls_guess():
    BatchMeta._registry.clear()
    BatchMeta.register(PBS)
    if CFG.env_vars.batch_system in os.environ:
        del os.environ[CFG.env_vars.batch_system]

    with (
        patch.object(PBS, "isAvailable", return_value=True),
    ):
        assert BatchMeta.fromEnvVarOrGuess() is PBS


def test_from_env_var_not_set_calls_guess():
    BatchMeta._registry.clear()
    if CFG.env_vars.batch_system in os.environ:
        del os.environ[CFG.env_vars.batch_system]

    with pytest.raises(QQError, match="Could not guess a batch system"):
        BatchMeta.fromEnvVarOrGuess()


def test_obtain_with_name_registered():
    BatchMeta._registry.clear()
    BatchMeta.register(PBS)

    assert BatchMeta.obtain("PBS") is PBS


def test_obtain_with_name_not_registered():
    BatchMeta._registry.clear()

    with pytest.raises(QQError, match="No batch system registered"):
        BatchMeta.obtain("PBS")


def test_obtain_without_name_env_var(monkeypatch):
    BatchMeta._registry.clear()
    BatchMeta.register(PBS)
    monkeypatch.setenv(CFG.env_vars.batch_system, "PBS")

    assert BatchMeta.obtain(None) is PBS


def test_obtain_without_name_and_guess_fails():
    BatchMeta._registry.clear()
    if CFG.env_vars.batch_system in os.environ:
        del os.environ[CFG.env_vars.batch_system]

    with (
        patch.object(PBS, "isAvailable", return_value=False),
        pytest.raises(QQError, match="Could not guess a batch system"),
    ):
        BatchMeta.obtain(None)


def test_sync_with_exclusions_copies_new_files(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    # create files in src
    (src / "file1.txt").write_text("data1")
    (src / "file2.txt").write_text("data2")

    BatchInterface.syncWithExclusions(src, dest, None, None)

    # all files from src should exist in dest with same content
    for f in src.iterdir():
        dest_file = dest / f.name
        assert dest_file.exists()
        assert dest_file.read_text() == f.read_text()


def test_sync_with_exclusions_preserves_dest_files(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    # file in dest that is not in src
    (dest / "keep.txt").write_text("keep_me")
    # file in src
    (src / "new.txt").write_text("new_data")

    BatchInterface.syncWithExclusions(src, dest, None, None)

    # new file copied
    assert (dest / "new.txt").exists()
    assert (dest / "new.txt").read_text() == "new_data"
    # old file preserved
    assert (dest / "keep.txt").exists()
    assert (dest / "keep.txt").read_text() == "keep_me"
    # destination file not copied to src
    assert not (src / "keep.txt").exists()


def test_sync_with_exclusions_skips_excluded_files(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    (src / "include.txt").write_text("include")
    (src / "exclude.txt").write_text("exclude")

    BatchInterface.syncWithExclusions(
        src, dest, None, None, exclude_files=[src / "exclude.txt"]
    )

    assert (dest / "include.txt").exists()
    assert not (dest / "exclude.txt").exists()


def test_sync_with_exclusions_updates_changed_files(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    # same file in both, dest outdated
    # note that these files have the same time of creation,
    # so they have to have different size for rsync to work properly
    (src / "file.txt").write_text("new")
    (dest / "file.txt").write_text("older")

    BatchInterface.syncWithExclusions(src, dest, None, None)

    assert (dest / "file.txt").exists()
    assert (dest / "file.txt").read_text() == "new"


def test_sync_with_exclusions_rsync_failure(tmp_path, monkeypatch):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    # create a file to sync
    (src / "file.txt").write_text("data")

    # patch subprocess.run to simulate rsync failure
    def fake_run(_command, capture_output=True, text=True, timeout=0.0):
        _ = capture_output
        _ = text
        _ = timeout

        class Result:
            returncode = 1
            stderr = "rsync error"

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(QQError, match="Could not rsync files between"):
        BatchInterface.syncWithExclusions(src, dest, None, None)


def test_sync_with_exclusions_rsync_timeout(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    # create files in src
    (src / "file1.txt").write_text("data1")
    (src / "file2.txt").write_text("data2")

    with (
        pytest.raises(QQError, match="Could not rsync files"),
        patch("qq_lib.batch.interface.interface.CFG.timeouts.rsync", 0),
    ):
        BatchInterface.syncWithExclusions(src, dest, None, None)


def test_translate_rsync_excluded_command_local_to_local():
    src = Path("/source")
    dest = Path("/dest")
    cmd = BatchInterface._translateRsyncExcludedCommand(src, dest, None, None, [])
    assert cmd == [
        "rsync",
        "-e",
        "ssh -o GSSAPIAuthentication=yes -o PasswordAuthentication=no",
        "-rltD",
        "/source/",
        "/dest",
    ]


def test_translate_rsync_excluded_command_local_to_remote():
    src = Path("/source")
    dest = Path("/dest")
    cmd = BatchInterface._translateRsyncExcludedCommand(
        src, dest, None, "remotehost", []
    )
    assert cmd == [
        "rsync",
        "-e",
        "ssh -o GSSAPIAuthentication=yes -o PasswordAuthentication=no",
        "-rltD",
        "/source/",
        "remotehost:/dest",
    ]


def test_translate_rsync_excluded_command_remote_to_local():
    src = Path("/source")
    dest = Path("/dest")
    cmd = BatchInterface._translateRsyncExcludedCommand(
        src, dest, "remotehost", None, []
    )
    assert cmd == [
        "rsync",
        "-e",
        "ssh -o GSSAPIAuthentication=yes -o PasswordAuthentication=no",
        "-rltD",
        "remotehost:/source/",
        "/dest",
    ]


def test_translate_rsync_excluded_command_with_excludes():
    src = Path("/source")
    dest = Path("/dest")
    excludes = [Path("temp"), Path("logs/debug.log")]
    cmd = BatchInterface._translateRsyncExcludedCommand(src, dest, None, None, excludes)
    expected = [
        "rsync",
        "-e",
        "ssh -o GSSAPIAuthentication=yes -o PasswordAuthentication=no",
        "-rltD",
        "--exclude",
        "temp",
        "--exclude",
        "logs/debug.log",
        "/source/",
        "/dest",
    ]
    assert cmd == expected


def test_translate_rsync_excluded_command_empty_excludes_list():
    src = Path("/source")
    dest = Path("/dest")
    cmd = BatchInterface._translateRsyncExcludedCommand(src, dest, None, None, [])
    expected = [
        "rsync",
        "-e",
        "ssh -o GSSAPIAuthentication=yes -o PasswordAuthentication=no",
        "-rltD",
        "/source/",
        "/dest",
    ]
    assert cmd == expected


def test_sync_selected_copies_only_included_files(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    (src / "include.txt").write_text("include")
    (src / "skip.txt").write_text("skip")

    BatchInterface.syncSelected(
        src, dest, None, None, include_files=[src / "include.txt"]
    )

    assert (dest / "include.txt").exists()
    assert (dest / "include.txt").read_text() == "include"
    assert not (dest / "skip.txt").exists()


def test_sync_selected_preserves_other_dest_files(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    (src / "new.txt").write_text("new_data")
    (dest / "keep.txt").write_text("keep_me")

    BatchInterface.syncSelected(src, dest, None, None, include_files=[src / "new.txt"])

    # new file copied
    assert (dest / "new.txt").exists()
    assert (dest / "new.txt").read_text() == "new_data"
    # old file preserved
    assert (dest / "keep.txt").exists()
    assert (dest / "keep.txt").read_text() == "keep_me"


def test_sync_selected_empty_include_list(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    (src / "file.txt").write_text("data")

    # no include_files provided -> nothing should be synced
    BatchInterface.syncSelected(src, dest, None, None)

    assert not (dest / "file.txt").exists()


def test_sync_selected_rsync_failure(tmp_path, monkeypatch):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()
    (src / "file.txt").write_text("data")

    def fake_run(_command, capture_output=True, text=True, timeout=0.0):
        _ = capture_output
        _ = text
        _ = timeout

        class Result:
            returncode = 1
            stderr = "rsync error"

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(QQError, match="Could not rsync files between"):
        BatchInterface.syncSelected(
            src, dest, None, None, include_files=[src / "file.txt"]
        )


def test_sync_selected_rsync_timeout(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()
    (src / "file.txt").write_text("data")

    with (
        pytest.raises(QQError, match="Could not rsync files"),
        patch("qq_lib.batch.interface.interface.CFG.timeouts.rsync", 0),
    ):
        BatchInterface.syncSelected(
            src, dest, None, None, include_files=[src / "file.txt"]
        )


def test_translate_rsync_included_command_local_to_local():
    src = Path("/source")
    dest = Path("/dest")
    included = [Path("file1.txt"), Path("dir/file2.txt")]

    cmd = BatchInterface._translateRsyncIncludedCommand(src, dest, None, None, included)

    expected = [
        "rsync",
        "-e",
        "ssh -o GSSAPIAuthentication=yes -o PasswordAuthentication=no",
        "-rltD",
        "--include",
        "file1.txt",
        "--include",
        "file1.txt/***",
        "--include",
        "dir/file2.txt",
        "--include",
        "dir/file2.txt/***",
        "--exclude",
        "*",
        "/source/",
        "/dest",
    ]
    assert cmd == expected


def test_translate_rsync_included_command_local_to_remote():
    src = Path("/source")
    dest = Path("/dest")
    included = [Path("file1.txt")]

    cmd = BatchInterface._translateRsyncIncludedCommand(
        src, dest, None, "remotehost", included
    )

    expected = [
        "rsync",
        "-e",
        "ssh -o GSSAPIAuthentication=yes -o PasswordAuthentication=no",
        "-rltD",
        "--include",
        "file1.txt",
        "--include",
        "file1.txt/***",
        "--exclude",
        "*",
        "/source/",
        "remotehost:/dest",
    ]
    assert cmd == expected


def test_translate_rsync_included_command_remote_to_local():
    src = Path("/source")
    dest = Path("/dest")
    included = [Path("file1.txt")]

    cmd = BatchInterface._translateRsyncIncludedCommand(
        src, dest, "remotehost", None, included
    )

    expected = [
        "rsync",
        "-e",
        "ssh -o GSSAPIAuthentication=yes -o PasswordAuthentication=no",
        "-rltD",
        "--include",
        "file1.txt",
        "--include",
        "file1.txt/***",
        "--exclude",
        "*",
        "remotehost:/source/",
        "/dest",
    ]
    assert cmd == expected


def test_translate_rsync_included_command_no_files():
    src = Path("/source")
    dest = Path("/dest")
    included = []

    cmd = BatchInterface._translateRsyncIncludedCommand(src, dest, None, None, included)

    expected = [
        "rsync",
        "-e",
        "ssh -o GSSAPIAuthentication=yes -o PasswordAuthentication=no",
        "-rltD",
        "--exclude",
        "*",
        "/source/",
        "/dest",
    ]
    assert cmd == expected


def test_translate_move_command_single_file():
    files = [Path("source.txt")]
    moved_files = [Path("dest") / "dest.txt"]

    cmd = BatchInterface._translateMoveCommand(files, moved_files)
    assert cmd == "mv 'source.txt' 'dest/dest.txt'"


def test_translate_move_command_multiple_files():
    files = [Path("a.txt"), Path("b.txt")]
    moved_files = [Path("x.txt"), Path("y.txt")]

    cmd = BatchInterface._translateMoveCommand(files, moved_files)
    assert cmd == "mv 'a.txt' 'x.txt' && mv 'b.txt' 'y.txt'"


def test_translate_move_command_mismatched_lengths():
    files = [Path("a.txt")]
    moved_files = [Path("b.txt"), Path("c.txt")]

    with pytest.raises(QQError, match="must have the same length"):
        BatchInterface._translateMoveCommand(files, moved_files)


def test_translate_move_command_empty_lists():
    cmd = BatchInterface._translateMoveCommand([], [])
    assert cmd == ""


def test_is_shared_returns_false_for_local(monkeypatch, tmp_path):
    def fake_run(cmd, **kwargs):
        _ = cmd
        _ = kwargs

        class Result:
            returncode = 0

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)
    assert BatchInterface.isShared(tmp_path) is False


def test_is_shared_returns_true_for_shared(monkeypatch, tmp_path):
    def fake_run(cmd, **kwargs):
        _ = cmd
        _ = kwargs

        class Result:
            returncode = 1

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)

    assert BatchInterface.isShared(tmp_path) is True


def test_is_shared_passes_correct_command(monkeypatch, tmp_path):
    captured = {}

    def fake_run(cmd, **kwargs):
        _ = kwargs
        captured["cmd"] = cmd

        class Result:
            returncode = 0

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)

    BatchInterface.isShared(tmp_path)

    assert captured["cmd"][0:2] == ["df", "-l"]
    assert Path(captured["cmd"][2]) == tmp_path


class DummyJob:
    def __init__(self, job_id):
        self._id = job_id

    def getId(self):
        return self._id


def test_batch_interface_sort_jobs_sorts_by_id():
    jobs = [DummyJob("c"), DummyJob("a"), DummyJob("b")]
    BatchInterface.sortJobs(jobs)
    ids = [job.getId() for job in jobs]
    assert ids == ["a", "b", "c"]


def test_batch_interface_sort_jobs_with_numeric_ids():
    jobs = [DummyJob("10"), DummyJob("2"), DummyJob("1")]
    BatchInterface.sortJobs(jobs)
    ids = [job.getId() for job in jobs]
    assert ids == ["1", "10", "2"]


def test_batch_interface_sort_jobs_empty_list():
    jobs = []
    BatchInterface.sortJobs(jobs)
    assert jobs == []


@patch("qq_lib.batch.interface.interface.subprocess.run")
def test_batchinterface_delete_remote_dir_success(mock_run):
    mock_run.return_value = MagicMock(returncode=0)

    BatchInterface.deleteRemoteDir("remote_host", Path("/remote/dir"))

    mock_run.assert_called_once_with(
        [
            "ssh",
            "-o PasswordAuthentication=no",
            "-o GSSAPIAuthentication=yes",
            f"-o ConnectTimeout={CFG.timeouts.ssh}",
            "remote_host",
            "yes | rm -r /remote/dir",
        ],
        capture_output=True,
        text=True,
    )


@patch("qq_lib.batch.interface.interface.subprocess.run")
def test_batchinterface_delete_remote_dir_raises_error(mock_run):
    mock_run.return_value = MagicMock(returncode=1, stderr="permission denied")

    with pytest.raises(
        QQError,
        match="Could not delete remote directory '/remote/dir' on 'remote_host': permission denied.",
    ):
        BatchInterface.deleteRemoteDir("remote_host", Path("/remote/dir"))

    mock_run.assert_called_once()
