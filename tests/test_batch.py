# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from qq_lib.batch import QQBatchInterface, QQBatchMeta
from qq_lib.constants import BATCH_SYSTEM, SSH_TIMEOUT
from qq_lib.error import QQError
from qq_lib.pbs import QQPBS
from qq_lib.vbs import QQVBS


def test_translate_ssh_command():
    host = "node1"
    directory = Path("/tmp/work")
    cmd = QQBatchInterface._translateSSHCommand(host, directory)
    assert cmd == [
        "ssh",
        "-o PasswordAuthentication=no",
        f"-o ConnectTimeout={SSH_TIMEOUT}",
        host,
        "-t",
        f"cd {directory} || exit {QQBatchInterface.CD_FAIL} && exec bash -l",
    ]


def test_navigate_same_host_success(tmp_path):
    directory = tmp_path

    with patch("subprocess.run") as mock_run:
        QQBatchInterface._navigateSameHost(directory)
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
        QQBatchInterface._navigateSameHost(directory)

        # check that subprocess was not called
        mock_run.assert_not_called()


def test_guess_only_vbs_in_registry():
    QQBatchMeta._registry.clear()
    QQBatchMeta.register(QQVBS)
    with patch.object(QQVBS, "isAvailable", return_value=True):
        assert QQBatchMeta.guess() is QQVBS

    with (
        patch.object(QQVBS, "isAvailable", return_value=False),
        pytest.raises(QQError, match="Could not guess a batch system"),
    ):
        QQBatchMeta.guess()


def test_guess_pbs_first_then_vbs():
    QQBatchMeta._registry.clear()
    QQBatchMeta.register(QQPBS)
    QQBatchMeta.register(QQVBS)

    with patch.object(QQPBS, "isAvailable", return_value=True):
        assert QQBatchMeta.guess() is QQPBS

    with (
        patch.object(QQPBS, "isAvailable", return_value=False),
        patch.object(QQVBS, "isAvailable", return_value=True),
    ):
        assert QQBatchMeta.guess() is QQVBS

    with (
        patch.object(QQPBS, "isAvailable", return_value=False),
        patch.object(QQVBS, "isAvailable", return_value=False),
        pytest.raises(QQError, match="Could not guess a batch system"),
    ):
        QQBatchMeta.guess()


def test_guess_vbs_first_then_pbs():
    QQBatchMeta._registry.clear()
    QQBatchMeta.register(QQVBS)
    QQBatchMeta.register(QQPBS)

    with patch.object(QQVBS, "isAvailable", return_value=True):
        assert QQBatchMeta.guess() is QQVBS

    with (
        patch.object(QQVBS, "isAvailable", return_value=False),
        patch.object(QQPBS, "isAvailable", return_value=True),
    ):
        assert QQBatchMeta.guess() is QQPBS

    with (
        patch.object(QQVBS, "isAvailable", return_value=False),
        patch.object(QQPBS, "isAvailable", return_value=False),
        pytest.raises(QQError, match="Could not guess a batch system"),
    ):
        QQBatchMeta.guess()


def test_guess_empty_registry():
    QQBatchMeta._registry.clear()
    with pytest.raises(QQError, match="Could not guess a batch system"):
        QQBatchMeta.guess()


def test_from_str_success():
    QQBatchMeta._registry.clear()
    QQBatchMeta.register(QQVBS)
    QQBatchMeta.register(QQPBS)

    assert QQBatchMeta.fromStr("PBS") is QQPBS
    assert QQBatchMeta.fromStr("VBS") is QQVBS


def test_from_str_pbs_not_registered():
    QQBatchMeta._registry.clear()
    QQBatchMeta.register(QQVBS)

    with pytest.raises(QQError, match="No batch system registered"):
        QQBatchMeta.fromStr("PBS")


def test_from_str_none_registered():
    QQBatchMeta._registry.clear()

    with pytest.raises(QQError, match="No batch system registered"):
        QQBatchMeta.fromStr("PBS")


def test_env_var_or_guess_from_env_var_returns_value(monkeypatch):
    QQBatchMeta._registry.clear()
    QQBatchMeta.register(QQPBS)
    monkeypatch.setenv(BATCH_SYSTEM, "PBS")

    assert QQBatchMeta.fromEnvVarOrGuess() is QQPBS


def test_env_var_or_guess_from_env_var_not_set_calls_guess():
    QQBatchMeta._registry.clear()
    QQBatchMeta.register(QQPBS)
    QQBatchMeta.register(QQVBS)
    if BATCH_SYSTEM in os.environ:
        del os.environ[BATCH_SYSTEM]

    with (
        patch.object(QQPBS, "isAvailable", return_value=True),
        patch.object(QQVBS, "isAvailable", return_value=True),
    ):
        assert QQBatchMeta.fromEnvVarOrGuess() is QQPBS


def test_from_env_var_not_set_calls_guess():
    QQBatchMeta._registry.clear()
    if BATCH_SYSTEM in os.environ:
        del os.environ[BATCH_SYSTEM]

    with pytest.raises(QQError, match="Could not guess a batch system"):
        QQBatchMeta.fromEnvVarOrGuess()


def test_obtain_with_name_registered():
    QQBatchMeta._registry.clear()
    QQBatchMeta.register(QQVBS)
    QQBatchMeta.register(QQPBS)

    assert QQBatchMeta.obtain("PBS") is QQPBS
    assert QQBatchMeta.obtain("VBS") is QQVBS


def test_obtain_with_name_not_registered():
    QQBatchMeta._registry.clear()
    QQBatchMeta.register(QQVBS)

    with pytest.raises(QQError, match="No batch system registered"):
        QQBatchMeta.obtain("PBS")


def test_obtain_without_name_env_var(monkeypatch):
    QQBatchMeta._registry.clear()
    QQBatchMeta.register(QQPBS)
    monkeypatch.setenv(BATCH_SYSTEM, "PBS")

    assert QQBatchMeta.obtain(None) is QQPBS


def test_obtain_without_name_calls_guess():
    QQBatchMeta._registry.clear()
    QQBatchMeta.register(QQPBS)
    QQBatchMeta.register(QQVBS)
    if BATCH_SYSTEM in os.environ:
        del os.environ[BATCH_SYSTEM]

    with (
        patch.object(QQVBS, "isAvailable", return_value=True),
        patch.object(QQPBS, "isAvailable", return_value=False),
    ):
        assert QQBatchMeta.obtain(None) is QQVBS


def test_obtain_without_name_and_guess_fails():
    QQBatchMeta._registry.clear()
    if BATCH_SYSTEM in os.environ:
        del os.environ[BATCH_SYSTEM]

    with (
        patch.object(QQVBS, "isAvailable", return_value=False),
        patch.object(QQPBS, "isAvailable", return_value=False),
        pytest.raises(QQError, match="Could not guess a batch system"),
    ):
        QQBatchMeta.obtain(None)


def test_sync_directories_copies_new_files(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    # create files in src
    (src / "file1.txt").write_text("data1")
    (src / "file2.txt").write_text("data2")

    QQBatchInterface.syncDirectories(src, dest, None, None)

    # all files from src should exist in dest with same content
    for f in src.iterdir():
        dest_file = dest / f.name
        assert dest_file.exists()
        assert dest_file.read_text() == f.read_text()


def test_sync_directories_preserves_dest_files(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    # file in dest that is not in src
    (dest / "keep.txt").write_text("keep_me")
    # file in src
    (src / "new.txt").write_text("new_data")

    QQBatchInterface.syncDirectories(src, dest, None, None)

    # new file copied
    assert (dest / "new.txt").exists()
    assert (dest / "new.txt").read_text() == "new_data"
    # old file preserved
    assert (dest / "keep.txt").exists()
    assert (dest / "keep.txt").read_text() == "keep_me"
    # destination file not copied to src
    assert not (src / "keep.txt").exists()


def test_sync_directories_skips_excluded_files(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    (src / "include.txt").write_text("include")
    (src / "exclude.txt").write_text("exclude")

    QQBatchInterface.syncDirectories(
        src, dest, None, None, exclude_files=[src / "exclude.txt"]
    )

    assert (dest / "include.txt").exists()
    assert not (dest / "exclude.txt").exists()


def test_sync_directories_updates_changed_files(tmp_path):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    # same file in both, dest outdated
    # note that these files have the same time of creation,
    # so they have to have different size for rsync to work properly
    (src / "file.txt").write_text("new")
    (dest / "file.txt").write_text("older")

    QQBatchInterface.syncDirectories(src, dest, None, None)

    assert (dest / "file.txt").exists()
    assert (dest / "file.txt").read_text() == "new"


def test_sync_directories_rsync_failure(tmp_path, monkeypatch):
    src = tmp_path / "src"
    dest = tmp_path / "dest"
    src.mkdir()
    dest.mkdir()

    # create a file to sync
    (src / "file.txt").write_text("data")

    # patch subprocess.run to simulate rsync failure
    def fake_run(_command, capture_output=True, text=True):
        _ = capture_output
        _ = text

        class Result:
            returncode = 1
            stderr = "rsync error"

        return Result()

    monkeypatch.setattr(subprocess, "run", fake_run)

    with pytest.raises(QQError, match="Could not rsync files between"):
        QQBatchInterface.syncDirectories(src, dest, None, None)


def test_build_rsync_command_local_to_local():
    src = Path("/source")
    dest = Path("/dest")
    cmd = QQBatchInterface._buildRsyncCommand(src, dest, None, None, [])
    assert cmd == ["rsync", "-a", "/source/", "/dest"]


def test_build_rsync_command_local_to_remote():
    src = Path("/source")
    dest = Path("/dest")
    cmd = QQBatchInterface._buildRsyncCommand(src, dest, None, "remotehost", [])
    assert cmd == ["rsync", "-a", "/source/", "remotehost:/dest"]


def test_build_rsync_command_remote_to_local():
    src = Path("/source")
    dest = Path("/dest")
    cmd = QQBatchInterface._buildRsyncCommand(src, dest, "remotehost", None, [])
    assert cmd == ["rsync", "-a", "remotehost:/source/", "/dest"]


def test_build_rsync_command_with_excludes():
    src = Path("/source")
    dest = Path("/dest")
    excludes = [Path("temp"), Path("logs/debug.log")]
    cmd = QQBatchInterface._buildRsyncCommand(src, dest, None, None, excludes)
    expected = [
        "rsync",
        "-a",
        "--exclude",
        "temp",
        "--exclude",
        "logs/debug.log",
        "/source/",
        "/dest",
    ]
    assert cmd == expected


def test_build_rsync_command_empty_excludes_list():
    src = Path("/source")
    dest = Path("/dest")
    cmd = QQBatchInterface._buildRsyncCommand(src, dest, None, None, [])
    expected = ["rsync", "-a", "/source/", "/dest"]
    assert cmd == expected
