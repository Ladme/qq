# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

# ruff: noqa: W291

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.batch.interface import QQBatchInterface
from qq_lib.batch.pbs import QQPBS, PBSJobInfo
from qq_lib.batch.pbs.node import PBSNode
from qq_lib.batch.pbs.qqpbs import CFG
from qq_lib.core.error import QQError
from qq_lib.properties.depend import Depend, DependType
from qq_lib.properties.resources import QQResources
from qq_lib.properties.size import Size


@pytest.fixture
def resources():
    return QQResources(
        nnodes=1, mem_per_cpu="1gb", ncpus=4, work_dir="scratch_local", work_size="16gb"
    )


def test_translate_kill_force():
    job_id = "123"
    cmd = QQPBS._translateKillForce(job_id)
    assert cmd == f"qdel -W force {job_id}"


def test_translate_kill():
    job_id = "123"
    cmd = QQPBS._translateKill(job_id)
    assert cmd == f"qdel {job_id}"


def test_navigate_success(tmp_path):
    directory = tmp_path

    with patch("subprocess.run") as mock_run:
        QQPBS.navigateToDestination("fake.host.org", directory)
        # check that subprocess was called properly
        mock_run.assert_called_once_with(
            [
                "ssh",
                "-o PasswordAuthentication=no",
                f"-o ConnectTimeout={CFG.timeouts.ssh}",
                "fake.host.org",
                "-t",
                f"cd {directory} || exit {QQBatchInterface.CD_FAIL} && exec bash -l",
            ]
        )

        # should not raise


def test_shared_guard_sets_env_var():
    env_vars = {CFG.env_vars.guard: "true"}

    # patch isShared to return True
    with patch.object(QQPBS, "isShared", return_value=True):
        QQPBS._sharedGuard(QQResources(work_dir="scratch_local"), env_vars)
        assert env_vars[CFG.env_vars.shared_submit] == "true"
        # previous env vars not removed
        assert env_vars[CFG.env_vars.guard] == "true"


def test_shared_guard_does_not_set_env_var():
    env_vars = {CFG.env_vars.guard: "true"}

    # patch isShared to return False
    with patch.object(QQPBS, "isShared", return_value=False):
        QQPBS._sharedGuard(QQResources(work_dir="scratch_local"), env_vars)
        assert CFG.env_vars.shared_submit not in env_vars
        # previous env vars not removed
        assert env_vars[CFG.env_vars.guard] == "true"


@pytest.mark.parametrize("dir", ["input_dir", "job_dir"])
def test_shared_guard_input_dir_does_not_raise(dir):
    env_vars = {}

    # patch isShared to return True
    with patch.object(QQPBS, "isShared", return_value=True):
        QQPBS._sharedGuard(QQResources(work_dir=dir), env_vars)
        assert env_vars[CFG.env_vars.shared_submit] == "true"


@pytest.mark.parametrize("dir", ["input_dir", "job_dir"])
def test_shared_guard_input_dir_raises(dir):
    env_vars = {}

    # patch isShared to return False
    with (
        patch.object(QQPBS, "isShared", return_value=False),
        pytest.raises(
            QQError,
            match="Job was requested to run directly in the submission directory",
        ),
    ):
        QQPBS._sharedGuard(QQResources(work_dir=dir), env_vars)
        assert CFG.env_vars.shared_submit not in env_vars


def test_sync_with_exclusions_shared_storage_sets_local(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = [Path("file1"), Path("file2")]

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    with patch.object(QQBatchInterface, "syncWithExclusions") as mock_sync:
        QQPBS.syncWithExclusions(src_dir, dest_dir, "host1", "host2", exclude_files)
        mock_sync.assert_called_once_with(src_dir, dest_dir, None, None, exclude_files)

    monkeypatch.delenv(CFG.env_vars.shared_submit)


def test_sync_with_exclusions_local_src(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = [Path("file1")]
    local_host = "myhost"

    monkeypatch.setenv(CFG.env_vars.shared_submit, "")

    with (
        patch.object(QQBatchInterface, "syncWithExclusions") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        # source is local, destination is remote
        QQPBS.syncWithExclusions(
            src_dir, dest_dir, local_host, "remotehost", exclude_files
        )
        mock_sync.assert_called_once_with(
            src_dir, dest_dir, None, "remotehost", exclude_files
        )


def test_sync_with_exclusions_local_dest(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = []
    local_host = "myhost"

    monkeypatch.setenv(CFG.env_vars.shared_submit, "")

    with (
        patch.object(QQBatchInterface, "syncWithExclusions") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        # destination is local, source is remote
        QQPBS.syncWithExclusions(
            src_dir, dest_dir, "remotehost", local_host, exclude_files
        )
        mock_sync.assert_called_once_with(
            src_dir, dest_dir, "remotehost", None, exclude_files
        )


def test_sync_with_exclusions_one_remote(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = None
    local_host = "myhost"

    monkeypatch.setenv(CFG.env_vars.shared_submit, "")

    with (
        patch.object(QQBatchInterface, "syncWithExclusions") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        # source local, destination local -> uses None
        QQPBS.syncWithExclusions(src_dir, dest_dir, None, local_host, exclude_files)
        mock_sync.assert_called_once_with(src_dir, dest_dir, None, None, exclude_files)


def test_sync_with_exclusions_both_remote_raises(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = None

    monkeypatch.setenv(CFG.env_vars.shared_submit, "")

    with (
        patch("socket.gethostname", return_value="localhost"),
        pytest.raises(QQError, match="cannot be both remote"),
    ):
        # both source and destination are remote and job directory is not shared
        QQPBS.syncWithExclusions(src_dir, dest_dir, "remote1", "remote2", exclude_files)


def test_sync_selected_shared_storage_sets_local(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    include_files = [Path("file1"), Path("file2")]

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    with patch.object(QQBatchInterface, "syncSelected") as mock_sync:
        QQPBS.syncSelected(src_dir, dest_dir, "host1", "host2", include_files)
        mock_sync.assert_called_once_with(src_dir, dest_dir, None, None, include_files)

    monkeypatch.delenv(CFG.env_vars.shared_submit)


def test_sync_selected_local_src(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    include_files = [Path("file1")]
    local_host = "myhost"

    monkeypatch.setenv(CFG.env_vars.shared_submit, "")

    with (
        patch.object(QQBatchInterface, "syncSelected") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        QQPBS.syncSelected(src_dir, dest_dir, local_host, "remotehost", include_files)
        mock_sync.assert_called_once_with(
            src_dir, dest_dir, None, "remotehost", include_files
        )


def test_sync_selected_local_dest(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    include_files = []
    local_host = "myhost"

    monkeypatch.setenv(CFG.env_vars.shared_submit, "")

    with (
        patch.object(QQBatchInterface, "syncSelected") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        QQPBS.syncSelected(src_dir, dest_dir, "remotehost", local_host, include_files)
        mock_sync.assert_called_once_with(
            src_dir, dest_dir, "remotehost", None, include_files
        )


def test_sync_selected_one_remote(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    include_files = None
    local_host = "myhost"

    monkeypatch.setenv(CFG.env_vars.shared_submit, "")

    with (
        patch.object(QQBatchInterface, "syncSelected") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        QQPBS.syncSelected(src_dir, dest_dir, None, local_host, include_files)
        mock_sync.assert_called_once_with(src_dir, dest_dir, None, None, include_files)


def test_sync_selected_both_remote_raises(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    include_files = None

    monkeypatch.setenv(CFG.env_vars.shared_submit, "")

    with (
        patch("socket.gethostname", return_value="localhost"),
        pytest.raises(QQError, match="cannot be both remote"),
    ):
        QQPBS.syncSelected(src_dir, dest_dir, "remote1", "remote2", include_files)


def test_read_remote_file_shared_storage(tmp_path, monkeypatch):
    file_path = tmp_path / "testfile.txt"
    content = "Hello, QQ!"
    file_path.write_text(content)

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    result = QQPBS.readRemoteFile("remotehost", file_path)
    assert result == content

    monkeypatch.delenv(CFG.env_vars.shared_submit)


def test_read_remote_file_shared_storage_file_missing(tmp_path, monkeypatch):
    file_path = tmp_path / "nonexistent.txt"

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    with pytest.raises(QQError, match="Could not read file"):
        QQPBS.readRemoteFile("remotehost", file_path)

    monkeypatch.delenv(CFG.env_vars.shared_submit)


def test_read_remote_file_remote():
    file_path = Path("/remote/file.txt")
    with patch.object(
        QQBatchInterface, "readRemoteFile", return_value="data"
    ) as mock_read:
        result = QQPBS.readRemoteFile("remotehost", file_path)
        mock_read.assert_called_once_with("remotehost", file_path)
        assert result == "data"


def test_write_remote_file_shared_storage(tmp_path, monkeypatch):
    file_path = tmp_path / "output.txt"
    content = "Test content"

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    QQPBS.writeRemoteFile("remotehost", file_path, content)
    assert file_path.read_text() == content


def test_write_remote_file_shared_storage_exception(tmp_path, monkeypatch):
    # using a directory instead of a file to cause write_text to fail
    dir_path = tmp_path / "dir"
    dir_path.mkdir()

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    with pytest.raises(QQError, match="Could not write file"):
        QQPBS.writeRemoteFile("remotehost", dir_path, "content")


def test_write_remote_file_remote():
    file_path = Path("/remote/output.txt")
    content = "data"

    with patch.object(QQBatchInterface, "writeRemoteFile") as mock_write:
        QQPBS.writeRemoteFile("remotehost", file_path, content)
        mock_write.assert_called_once_with("remotehost", file_path, content)


def test_make_remote_dir_shared_storage(tmp_path, monkeypatch):
    dir_path = tmp_path / "newdir"

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    QQPBS.makeRemoteDir("remotehost", dir_path)

    assert dir_path.exists() and dir_path.is_dir()


def test_make_remote_dir_shared_storage_exception(tmp_path, monkeypatch):
    file_path = tmp_path / "conflict"
    file_path.write_text("dummy")

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    with pytest.raises(QQError, match="Could not create a directory"):
        QQPBS.makeRemoteDir("remotehost", file_path)


def test_make_remote_dir_shared_storage_already_exists_ok(tmp_path, monkeypatch):
    dir_path = tmp_path / "newdir"
    dir_path.mkdir()

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    # ignore that the directory already exists
    QQPBS.makeRemoteDir("remotehost", dir_path)

    assert dir_path.exists() and dir_path.is_dir()


def test_make_remote_dir_remote():
    dir_path = Path("/remote/newdir")

    with patch.object(QQBatchInterface, "makeRemoteDir") as mock_make:
        QQPBS.makeRemoteDir("remotehost", dir_path)
        mock_make.assert_called_once_with("remotehost", dir_path)


def test_list_remote_dir_shared_storage(tmp_path, monkeypatch):
    (tmp_path / "file1.txt").write_text("one")
    (tmp_path / "file2.txt").write_text("two")
    (tmp_path / "subdir").mkdir()

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    result = QQPBS.listRemoteDir("remotehost", tmp_path)

    result_names = sorted([p.name for p in result])
    assert result_names == ["file1.txt", "file2.txt", "subdir"]


def test_list_remote_dir_shared_storage_exception(tmp_path, monkeypatch):
    # use a file instead of directory -> .iterdir() should fail
    bad_path = tmp_path / "notadir"
    bad_path.write_text("oops")

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    with pytest.raises(QQError, match="Could not list a directory"):
        QQPBS.listRemoteDir("remotehost", bad_path)


def test_list_remote_dir_remote():
    dir_path = Path("/remote/dir")

    with patch.object(QQBatchInterface, "listRemoteDir") as mock_list:
        QQPBS.listRemoteDir("remotehost", dir_path)
        mock_list.assert_called_once_with("remotehost", dir_path)


def test_move_remote_files_shared_storage(tmp_path, monkeypatch):
    src1 = tmp_path / "file1.txt"
    src2 = tmp_path / "file2.txt"
    src1.write_text("one")
    src2.write_text("two")

    dst_dir = tmp_path / "dest"
    dst_dir.mkdir()
    dst1 = tmp_path / "dest1.txt"
    dst2 = dst_dir / "dest2.txt"

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    QQPBS.moveRemoteFiles("remotehost", [src1, src2], [dst1, dst2])

    # check that files were moved
    assert dst1.exists() and dst1.read_text() == "one"
    assert dst2.exists() and dst2.read_text() == "two"
    assert not src1.exists()
    assert not src2.exists()


def test_move_remote_files_shared_storage_exception(tmp_path, monkeypatch):
    bad_src = tmp_path / "dir"
    bad_src.mkdir()
    dst = tmp_path / "dest"

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    # normally shutil.move would move a directory,
    # so we force an error by making the destination a file
    (dst).write_text("dummy")

    with pytest.raises(Exception):
        QQPBS.moveRemoteFiles("remotehost", [bad_src], [dst])


def test_move_remote_files_length_mismatch(tmp_path, monkeypatch):
    src = tmp_path / "file1.txt"
    src.write_text("data")
    dst1 = tmp_path / "dest1.txt"
    dst2 = tmp_path / "dest2.txt"

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    with pytest.raises(QQError, match="must have the same length"):
        QQPBS.moveRemoteFiles("remotehost", [src], [dst1, dst2])


def test_move_remote_files_remote():
    src = Path("/remote/file.txt")
    dst = Path("/remote/dest.txt")

    with patch.object(QQBatchInterface, "moveRemoteFiles") as mock_move:
        QQPBS.moveRemoteFiles("remotehost", [src], [dst])
        mock_move.assert_called_once_with("remotehost", [src], [dst])


def test_translate_work_dir_input_dir_returns_none():
    res = QQResources(nnodes=1, work_dir="input_dir")
    assert QQPBS._translateWorkDir(res) is None


def test_translate_work_dir_scratch_shm_returns_true_string():
    res = QQResources(nnodes=3, work_dir="scratch_shm")
    assert QQPBS._translateWorkDir(res) == "scratch_shm=true"


def test_translate_work_dir_work_size_divided_by_nnodes():
    res = QQResources(nnodes=2, work_dir="scratch_local", work_size="7mb")
    result = QQPBS._translateWorkDir(res)
    assert result == "scratch_local=3584kb"


def test_translate_work_dir_work_size_per_cpu_and_ncpus():
    res = QQResources(
        nnodes=4, ncpus=5, work_dir="scratch_local", work_size_per_cpu="3mb"
    )
    result = QQPBS._translateWorkDir(res)
    assert result == "scratch_local=3840kb"


def test_translate_work_dir_missing_work_size_raises():
    res = QQResources(nnodes=2, ncpus=4, work_dir="scratch_local")
    with pytest.raises(QQError, match="work-size"):
        QQPBS._translateWorkDir(res)


def test_translate_work_dir_missing_ncpus_with_work_size_per_cpu_raises():
    res = QQResources(nnodes=2, work_dir="scratch_local", work_size_per_cpu="3mb")
    with pytest.raises(QQError, match="work-size"):
        QQPBS._translateWorkDir(res)


def test_translate_per_chunk_resources_nnones_missing_raises():
    res = QQResources(nnodes=None, ncpus=2, mem="4mb")
    with pytest.raises(QQError, match="nnodes"):
        QQPBS._translatePerChunkResources(res)


def test_translate_per_chunk_resources_nnones_zero_raises():
    res = QQResources(nnodes=0, ncpus=2, mem="4mb")
    with pytest.raises(QQError, match="nnodes"):
        QQPBS._translatePerChunkResources(res)


def test_translate_per_chunk_resources_ncpus_not_divisible_raises():
    res = QQResources(nnodes=3, ncpus=4, mem="4mb")
    with pytest.raises(QQError, match="ncpus"):
        QQPBS._translatePerChunkResources(res)


def test_translate_per_chunk_resources_ngpus_not_divisible_raises():
    res = QQResources(nnodes=2, ncpus=2, ngpus=3, mem="4mb")
    with pytest.raises(QQError, match="ngpus"):
        QQPBS._translatePerChunkResources(res)


def test_translate_per_chunk_resources_mem_division():
    res = QQResources(nnodes=2, ncpus=4, mem="7mb", work_dir="input_dir")
    result = QQPBS._translatePerChunkResources(res)
    assert "ncpus=2" in result
    assert "mem=3584kb" in result


def test_translate_per_chunk_resources_mem_per_cpu_used():
    res = QQResources(nnodes=2, ncpus=4, mem_per_cpu="2mb", work_dir="input_dir")
    result = QQPBS._translatePerChunkResources(res)
    # 2mb * 4 / 2 = 4mb
    assert "mem=4096kb" in result


def test_translate_per_chunk_resources_ngpus_included():
    res = QQResources(nnodes=3, ncpus=9, mem="8mb", ngpus=6, work_dir="input_dir")
    result = QQPBS._translatePerChunkResources(res)
    assert "ngpus=2" in result


def test_translate_per_chunk_resources_work_dir_translated():
    res = QQResources(
        nnodes=2, ncpus=4, mem="8mb", work_dir="scratch_local", work_size="1mb"
    )
    result = QQPBS._translatePerChunkResources(res)
    assert "scratch_local=512kb" in result


def test_translate_per_chunk_resources_missing_memory_raises():
    res = QQResources(nnodes=2, ncpus=4)
    with pytest.raises(QQError, match="mem"):
        QQPBS._translatePerChunkResources(res)


def test_translate_submit_minimal_fields():
    res = QQResources(nnodes=1, ncpus=1, mem="1gb", work_dir="input_dir")
    assert (
        QQPBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=1,mem=1048576kb script.sh"
    )


def test_translate_submit_with_env_vars():
    res = QQResources(nnodes=1, ncpus=1, mem="1gb", work_dir="input_dir")
    assert (
        QQPBS._translateSubmit(
            res,
            "gpu",
            Path("tmp"),
            "script.sh",
            "job",
            [],
            {CFG.env_vars.guard: "true", CFG.env_vars.batch_system: "PBS"},
        )
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -v \"{CFG.env_vars.guard}='true'\",\"{CFG.env_vars.batch_system}='PBS'\" -l ncpus=1,mem=1048576kb script.sh"
    )


def test_translate_submit_multiple_nodes():
    res = QQResources(nnodes=4, ncpus=8, mem="1gb", work_dir="input_dir")
    assert (
        QQPBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l select=4:ncpus=2:mem=262144kb -l place=vscatter script.sh"
    )


def test_translate_submit_multiple_nodes_with_env_vars():
    res = QQResources(nnodes=4, ncpus=8, mem="1gb", work_dir="input_dir")
    assert (
        QQPBS._translateSubmit(
            res,
            "gpu",
            Path("tmp"),
            "script.sh",
            "job",
            [],
            {CFG.env_vars.guard: "true", CFG.env_vars.batch_system: "PBS"},
        )
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -v \"{CFG.env_vars.guard}='true'\",\"{CFG.env_vars.batch_system}='PBS'\" -l select=4:ncpus=2:mem=262144kb -l place=vscatter script.sh"
    )


def test_translate_submit_with_walltime():
    res = QQResources(
        nnodes=1, ncpus=2, mem="2gb", walltime="1d24m121s", work_dir="input_dir"
    )
    assert (
        QQPBS._translateSubmit(res, "queue", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q queue -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=2,mem=2097152kb -l walltime=24:26:01 script.sh"
    )


def test_translate_submit_with_walltime2():
    res = QQResources(
        nnodes=1, ncpus=2, mem="2gb", walltime="12:30:15", work_dir="input_dir"
    )
    assert (
        QQPBS._translateSubmit(res, "queue", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q queue -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=2,mem=2097152kb -l walltime=12:30:15 script.sh"
    )


def test_translate_submit_with_walltime_and_env_vars():
    res = QQResources(
        nnodes=1, ncpus=2, mem="2gb", walltime="1d24m121s", work_dir="input_dir"
    )
    assert (
        QQPBS._translateSubmit(
            res,
            "queue",
            Path("tmp"),
            "script.sh",
            "job",
            [],
            {CFG.env_vars.guard: "true", CFG.env_vars.batch_system: "PBS"},
        )
        == f"qsub -N job -q queue -j eo -e tmp/job{CFG.suffixes.qq_out} -v \"{CFG.env_vars.guard}='true'\",\"{CFG.env_vars.batch_system}='PBS'\" -l ncpus=2,mem=2097152kb -l walltime=24:26:01 script.sh"
    )


def test_translate_submit_work_dir_scratch_shm():
    res = QQResources(nnodes=1, ncpus=1, mem="8gb", work_dir="scratch_shm")
    assert (
        QQPBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=1,mem=8388608kb,scratch_shm=true script.sh"
    )


def test_translate_submit_scratch_local_work_size():
    res = QQResources(
        nnodes=2, ncpus=2, mem="4gb", work_dir="scratch_local", work_size="16gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l select=2:ncpus=1:mem=2097152kb:scratch_local=8388608kb -l place=vscatter script.sh"
    )


def test_translate_submit_scratch_ssd_work_size():
    res = QQResources(
        nnodes=2, ncpus=2, mem="4gb", work_dir="scratch_ssd", work_size="16gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l select=2:ncpus=1:mem=2097152kb:scratch_ssd=8388608kb -l place=vscatter script.sh"
    )


def test_translate_submit_scratch_shared_work_size():
    res = QQResources(
        nnodes=2, ncpus=2, mem="4gb", work_dir="scratch_shared", work_size="16gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l select=2:ncpus=1:mem=2097152kb:scratch_shared=8388608kb -l place=vscatter script.sh"
    )


def test_translate_submit_work_size_per_cpu():
    res = QQResources(
        nnodes=1, ncpus=8, mem="4gb", work_dir="scratch_local", work_size_per_cpu="2gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=8,mem=4194304kb,scratch_local=16777216kb script.sh"
    )


def test_translate_submit_work_size_per_cpu_multiple_nodes():
    res = QQResources(
        nnodes=3, ncpus=3, mem="4gb", work_dir="scratch_local", work_size_per_cpu="2gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l select=3:ncpus=1:mem=1398102kb:scratch_local=2097152kb -l place=vscatter script.sh"
    )


def test_translate_submit_mem_per_cpu():
    res = QQResources(
        nnodes=1, ncpus=4, mem_per_cpu="2gb", work_dir="scratch_local", work_size="10gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=4,mem=8388608kb,scratch_local=10485760kb script.sh"
    )


def test_translate_submit_mem_per_cpu_multiple_nodes():
    res = QQResources(
        nnodes=2, ncpus=4, mem_per_cpu="2gb", work_dir="scratch_local", work_size="20gb"
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l select=2:ncpus=2:mem=4194304kb:scratch_local=10485760kb -l place=vscatter script.sh"
    )


def test_translate_submit_mem_per_cpu_and_work_size_per_cpu():
    res = QQResources(
        nnodes=1,
        ncpus=4,
        mem_per_cpu="2gb",
        work_dir="scratch_local",
        work_size_per_cpu="5gb",
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=4,mem=8388608kb,scratch_local=20971520kb script.sh"
    )


def test_translate_submit_mem_per_cpu_and_work_size_per_cpu_multiple_nodes():
    res = QQResources(
        nnodes=2,
        ncpus=4,
        mem_per_cpu="2gb",
        work_dir="scratch_local",
        work_size_per_cpu="5gb",
    )
    assert (
        QQPBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l select=2:ncpus=2:mem=4194304kb:scratch_local=10485760kb -l place=vscatter script.sh"
    )


def test_translate_submit_with_props():
    res = QQResources(
        nnodes=1,
        ncpus=1,
        mem="1gb",
        props={"vnode": "my_node", "infiniband": "true"},
        work_dir="input_dir",
    )
    assert (
        QQPBS._translateSubmit(res, "queue", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q queue -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=1,mem=1048576kb,vnode=my_node,infiniband=true script.sh"
    )


def test_translate_submit_with_props_and_env_vars():
    res = QQResources(
        nnodes=1,
        ncpus=1,
        mem="1gb",
        props={"vnode": "my_node", "infiniband": "true"},
        work_dir="input_dir",
    )
    assert (
        QQPBS._translateSubmit(
            res,
            "queue",
            Path("tmp"),
            "script.sh",
            "job",
            [],
            {CFG.env_vars.guard: "true", CFG.env_vars.batch_system: "PBS"},
        )
        == f"qsub -N job -q queue -j eo -e tmp/job{CFG.suffixes.qq_out} -v \"{CFG.env_vars.guard}='true'\",\"{CFG.env_vars.batch_system}='PBS'\" -l ncpus=1,mem=1048576kb,vnode=my_node,infiniband=true script.sh"
    )


def test_translate_submit_complex_case():
    res = QQResources(
        nnodes=3,
        ncpus=6,
        mem="5gb",
        ngpus=3,
        walltime="1h30m",
        work_dir="scratch_local",
        work_size_per_cpu="2gb",
        props={"cl_cluster": "true"},
    )
    assert QQPBS._translateSubmit(
        res,
        "gpu",
        Path("tmp"),
        "myscript.sh",
        "job",
        [],
        {
            CFG.env_vars.info_file: "/path/to/job/job.qqinfo",
            CFG.env_vars.input_dir: "/path/to/job/",
            CFG.env_vars.guard: "true",
        },
    ) == (
        f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} "
        f"-v \"{CFG.env_vars.info_file}='/path/to/job/job.qqinfo'\",\"{CFG.env_vars.input_dir}='/path/to/job/'\",\"{CFG.env_vars.guard}='true'\" "
        f"-l select=3:ncpus=2:mem=1747627kb:ngpus=1:scratch_local=4194304kb:cl_cluster=true "
        f"-l walltime=1:30:00 -l place=vscatter myscript.sh"
    )


def test_translate_submit_single_depend():
    res = QQResources(nnodes=1, ncpus=1, mem="1gb", work_dir="input_dir")
    depend = [Depend(DependType.AFTER_START, ["123"])]
    cmd = QQPBS._translateSubmit(
        res, "queue", Path("tmp"), "script.sh", "job", depend, {}
    )
    expected = f"qsub -N job -q queue -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=1,mem=1048576kb -W depend=after:123 script.sh"
    assert cmd == expected


def test_translate_submit_multiple_jobs_depend():
    res = QQResources(nnodes=1, ncpus=1, mem="1gb", work_dir="input_dir")
    depend = [Depend(DependType.AFTER_SUCCESS, ["1", "2"])]
    cmd = QQPBS._translateSubmit(
        res, "queue", Path("tmp"), "script.sh", "job", depend, {}
    )
    expected = f"qsub -N job -q queue -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=1,mem=1048576kb -W depend=afterok:1:2 script.sh"
    assert cmd == expected


def test_translate_submit_multiple_dependencies():
    res = QQResources(nnodes=1, ncpus=1, mem="1gb", work_dir="input_dir")
    depend = [
        Depend(DependType.AFTER_SUCCESS, ["1"]),
        Depend(DependType.AFTER_FAILURE, ["2"]),
    ]
    cmd = QQPBS._translateSubmit(
        res, "queue", Path("tmp"), "script.sh", "job", depend, {}
    )
    expected = f"qsub -N job -q queue -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=1,mem=1048576kb -W depend=afterok:1,afternotok:2 script.sh"
    assert cmd == expected


def test_translate_submit_complex_with_depend():
    res = QQResources(
        nnodes=2,
        ncpus=4,
        mem="4gb",
        walltime="01:00:00",
        work_dir="scratch_local",
        work_size_per_cpu="2gb",
        props={"cl_cluster": "true"},
    )
    depend = [Depend(DependType.AFTER_COMPLETION, ["42", "43"])]
    cmd = QQPBS._translateSubmit(
        res,
        "gpu",
        Path("tmp"),
        "myscript.sh",
        "job",
        depend,
        {
            CFG.env_vars.info_file: "/path/to/job/job.qqinfo",
            CFG.env_vars.input_dir: "/path/to/job/",
            CFG.env_vars.guard: "true",
        },
    )

    expected = (
        f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} "
        f"-v \"{CFG.env_vars.info_file}='/path/to/job/job.qqinfo'\",\"{CFG.env_vars.input_dir}='/path/to/job/'\",\"{CFG.env_vars.guard}='true'\" "
        f"-l select=2:ncpus=2:mem=2097152kb:scratch_local=4194304kb:cl_cluster=true "
        "-l walltime=01:00:00 -l place=vscatter -W depend=afterany:42:43 myscript.sh"
    )
    assert cmd == expected


def test_transform_resources_input_dir_warns_and_sets_work_dir():
    provided = QQResources(work_dir="input_dir", work_size="10gb")
    with (
        patch.object(QQPBS, "_getDefaultQueueResources", return_value=QQResources()),
        patch.object(QQPBS, "_getDefaultServerResources", return_value=QQResources()),
        patch.object(QQResources, "mergeResources", return_value=provided),
        patch("qq_lib.batch.pbs.qqpbs.logger.warning") as mock_warning,
    ):
        res = QQPBS.transformResources(
            "gpu", QQResources(work_dir="input_dir", work_size="10gb")
        )

    assert res.work_dir == "input_dir"

    called_args = mock_warning.call_args[0]
    assert "Setting work-size is not supported" in called_args[0]
    assert "input_dir" in called_args[0]


def test_transform_resources_job_dir_warns_and_sets_work_dir():
    provided = QQResources(work_dir="input_dir", work_size="10gb")
    with (
        patch.object(QQPBS, "_getDefaultQueueResources", return_value=QQResources()),
        patch.object(QQPBS, "_getDefaultServerResources", return_value=QQResources()),
        patch.object(QQResources, "mergeResources", return_value=provided),
        patch("qq_lib.batch.pbs.qqpbs.logger.warning") as mock_warning,
    ):
        res = QQPBS.transformResources(
            "gpu", QQResources(work_dir="job_dir", work_size="10gb")
        )

    assert res.work_dir == "input_dir"

    called_args = mock_warning.call_args[0]
    assert "Setting work-size is not supported" in called_args[0]
    assert "job_dir" in called_args[0]


def test_transform_resources_scratch_shm_warns_and_clears_work_size():
    provided = QQResources(work_dir="scratch_shm", work_size="10gb")
    with (
        patch.object(QQPBS, "_getDefaultQueueResources", return_value=QQResources()),
        patch.object(QQPBS, "_getDefaultServerResources", return_value=QQResources()),
        patch.object(QQResources, "mergeResources", return_value=provided),
        patch("qq_lib.batch.pbs.qqpbs.logger.warning") as mock_warning,
    ):
        res = QQPBS.transformResources(
            "gpu", QQResources(work_dir="scratch_shm", work_size="10gb")
        )

    assert res.work_dir == "scratch_shm"
    assert res.work_size is None

    called_args = mock_warning.call_args[0]
    assert "Setting work-size is not supported" in called_args[0]
    assert "scratch_shm" in called_args[0]


def test_transform_resources_supported_scratch():
    for scratch in QQPBS.SUPPORTED_SCRATCHES:
        provided = QQResources(work_dir=scratch, work_size="10gb")
        with (
            patch.object(
                QQPBS, "_getDefaultQueueResources", return_value=QQResources()
            ),
            patch.object(
                QQPBS, "_getDefaultServerResources", return_value=QQResources()
            ),
            patch.object(QQResources, "mergeResources", return_value=provided),
        ):
            res = QQPBS.transformResources(
                "gpu", QQResources(work_dir=scratch, work_size="10gb")
            )

        assert res.work_dir == scratch


def test_transform_resources_supported_scratch_unnormalized():
    for scratch in QQPBS.SUPPORTED_SCRATCHES:
        provided = QQResources(
            work_dir=scratch.upper().replace("_", "-"), work_size="10gb"
        )
        with (
            patch.object(
                QQPBS, "_getDefaultQueueResources", return_value=QQResources()
            ),
            patch.object(
                QQPBS, "_getDefaultServerResources", return_value=QQResources()
            ),
            patch.object(QQResources, "mergeResources", return_value=provided),
        ):
            res = QQPBS.transformResources(
                "gpu",
                QQResources(
                    work_dir=scratch.upper().replace("_", "-"), work_size="10gb"
                ),
            )

        assert res.work_dir == scratch


def test_transform_resources_unknown_work_dir_raises():
    provided = QQResources(work_dir="unknown_scratch")
    with (
        patch.object(QQPBS, "_getDefaultQueueResources", return_value=QQResources()),
        patch.object(QQPBS, "_getDefaultServerResources", return_value=QQResources()),
        patch.object(QQResources, "mergeResources", return_value=provided),
        pytest.raises(QQError, match="Unknown working directory type specified"),
    ):
        QQPBS.transformResources("gpu", QQResources(work_dir="unknown_scratch"))


def test_transform_resources_missing_work_dir_raises():
    provided = QQResources(work_dir=None)
    with (
        patch.object(QQPBS, "_getDefaultQueueResources", return_value=QQResources()),
        patch.object(QQPBS, "_getDefaultServerResources", return_value=QQResources()),
        patch.object(QQResources, "mergeResources", return_value=provided),
        pytest.raises(
            QQError, match="Work-dir is not set after filling in default attributes"
        ),
    ):
        QQPBS.transformResources("gpu", QQResources())


@pytest.fixture
def sample_multi_dump_file():
    return """Job Id: 123456.fake-cluster.example.com
    Job_Name = example_job_1
    Job_Owner = user@EXAMPLE
    resources_used.cpupercent = 50
    resources_used.ncpus = 4
    job_state = R
    queue = gpu

Job Id: 123457.fake-cluster.example.com
    Job_Name = example_job_2
    Job_Owner = user@EXAMPLE
    resources_used.cpupercent = 75
    resources_used.ncpus = 8
    job_state = Q
    queue = cpu

Job Id: 123458.fake-cluster.example.com
    Job_Name = example_job_3
    Job_Owner = user@EXAMPLE
    resources_used.cpupercent = 100
    resources_used.ncpus = 16
    job_state = H
    queue = gpu
"""


def test_get_jobs_info_using_command_success(sample_multi_dump_file):
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=0, stdout=sample_multi_dump_file, stderr=""
        )

        jobs = QQPBS._getBatchJobsUsingCommand("fake command - unused")

        assert len(jobs) == 3
        assert all(isinstance(job, PBSJobInfo) for job in jobs)

        expected_ids = [
            "123456.fake-cluster.example.com",
            "123457.fake-cluster.example.com",
            "123458.fake-cluster.example.com",
        ]
        assert [job._job_id for job in jobs] == expected_ids  # ty: ignore[unresolved-attribute]

        assert [job._info["Job_Name"] for job in jobs] == [  # ty: ignore[unresolved-attribute]
            "example_job_1",
            "example_job_2",
            "example_job_3",
        ]
        assert [job._info["job_state"] for job in jobs] == [  # ty: ignore[unresolved-attribute]
            "R",
            "Q",
            "H",
        ]

        mock_run.assert_called_once_with(
            ["bash"],
            input="fake command - unused",
            text=True,
            check=False,
            capture_output=True,
            errors="replace",
        )


def test_get_jobs_info_using_command_nonzero_returncode():
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(
            returncode=1, stdout="", stderr="Some error occurred"
        )
        with pytest.raises(
            QQError,
            match="Could not retrieve information about jobs: Some error occurred",
        ):
            QQPBS._getBatchJobsUsingCommand("will not be used")


@pytest.mark.parametrize(
    "depend_list, expected",
    [
        ([], None),
        ([Depend.fromStr("after=12345")], "after:12345"),
        ([Depend.fromStr("afterok=1:2:3")], "afterok:1:2:3"),
        (
            [Depend.fromStr("after=10"), Depend.fromStr("afternotok=20")],
            "after:10,afternotok:20",
        ),
        (
            [Depend.fromStr("afterany=100:101"), Depend.fromStr("afterok=200:201")],
            "afterany:100:101,afterok:200:201",
        ),
    ],
)
def test_translate_dependencies_various_cases(depend_list, expected):
    result = QQPBS._translateDependencies(depend_list)
    assert result == expected


def test_collect_ams_env_vars(monkeypatch):
    from qq_lib.batch.pbs.qqpbs import QQPBS

    # mock environment with a mix of AMS and non-AMS vars
    env_vars = {
        "AMS_ACTIVE_MODULES": "mod1,mod2",
        "AMS_ROOT": "/opt/ams",
        "OTHER_VAR": "ignore_me",
        "AMS_BUNDLE_PATH": "/ams/bundle",
        "PATH": "/usr/bin",
    }
    monkeypatch.setattr(os, "environ", env_vars)

    result = QQPBS._collectAMSEnvVars()

    # assert that only AMS variables were collected
    expected = {
        "AMS_ACTIVE_MODULES": "mod1,mod2",
        "AMS_ROOT": "/opt/ams",
        "AMS_BUNDLE_PATH": "/ams/bundle",
    }
    assert result == expected


@patch("qq_lib.batch.pbs.qqpbs.subprocess.run")
def test_qqpbs_get_queues_returns_list(mock_run):
    mock_run.return_value = MagicMock(returncode=0, stdout="mock_stdout", stderr="")

    with (
        patch(
            "qq_lib.batch.pbs.qqpbs.parseMultiPBSDumpToDictionaries",
            return_value=[({"key": "value"}, "queue1")],
        ) as mock_parse,
        patch(
            "qq_lib.batch.pbs.qqpbs.PBSQueue.fromDict", return_value="mock_queue"
        ) as mock_from_dict,
    ):
        result = QQPBS.getQueues()

    mock_run.assert_called_once_with(
        ["bash"],
        input="qstat -Qfw",
        text=True,
        check=False,
        capture_output=True,
        errors="replace",
    )

    mock_parse.assert_called_once_with("mock_stdout", "Queue")
    mock_from_dict.assert_called_once_with("queue1", {"key": "value"})

    assert result == ["mock_queue"]


@patch("qq_lib.batch.pbs.qqpbs.subprocess.run")
def test_qqpbs_get_queues_raises_on_failure(mock_run):
    mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error_message")

    with pytest.raises(QQError, match="error_message"):
        QQPBS.getQueues()


@patch("qq_lib.batch.pbs.qqpbs.subprocess.run")
def test_qqpbs_get_queues_multiple_queues(mock_run):
    mock_run.return_value = MagicMock(returncode=0, stdout="mock_stdout", stderr="")

    with (
        patch(
            "qq_lib.batch.pbs.qqpbs.parseMultiPBSDumpToDictionaries",
            return_value=[
                ({"data1": "value1"}, "queue1"),
                ({"data2": "value2"}, "queue2"),
            ],
        ) as mock_parse,
        patch(
            "qq_lib.batch.pbs.qqpbs.PBSQueue.fromDict",
            side_effect=["queue_obj1", "queue_obj2"],
        ) as mock_from_dict,
    ):
        result = QQPBS.getQueues()

    mock_parse.assert_called_once_with("mock_stdout", "Queue")
    assert mock_from_dict.call_count == 2

    assert result == ["queue_obj1", "queue_obj2"]


@patch("qq_lib.batch.pbs.qqpbs.PBSQueue")
def test_qqpbs_get_default_queue_resources_returns_resources(mock_pbsqueue):
    mock_instance = MagicMock()
    mock_instance.getDefaultResources.return_value = {"mem": "8gb", "ncpus": 4}
    mock_pbsqueue.return_value = mock_instance

    result = QQPBS._getDefaultQueueResources("gpu")
    mock_pbsqueue.assert_called_once_with("gpu")
    mock_instance.getDefaultResources.assert_called_once()

    assert isinstance(result, QQResources)
    assert result.mem == Size(8, "gb")
    assert result.ncpus == 4


@patch("qq_lib.batch.pbs.qqpbs.subprocess.run")
def test_qqpbs_get_nodes_returns_list(mock_run):
    mock_run.return_value = MagicMock(returncode=0, stdout="mock_stdout", stderr="")
    with patch(
        "qq_lib.batch.pbs.qqpbs.parseMultiPBSDumpToDictionaries",
        return_value=[({"key": "value"}, "node1")],
    ) as mock_parse:
        result = QQPBS.getNodes()

    mock_run.assert_called_once_with(
        ["bash"],
        input="pbsnodes -a",
        text=True,
        check=False,
        capture_output=True,
        errors="replace",
    )
    mock_parse.assert_called_once_with("mock_stdout", None)
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], PBSNode)
    assert result[0]._name == "node1"
    assert result[0]._info == {"key": "value"}


@patch("qq_lib.batch.pbs.qqpbs.subprocess.run")
def test_qqpbs_get_nodes_raises_on_failure(mock_run):
    mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error_message")
    with pytest.raises(QQError, match="error_message"):
        QQPBS.getNodes()


@patch("qq_lib.batch.pbs.qqpbs.subprocess.run")
def test_qqpbs_get_nodes_multiple_nodes(mock_run):
    mock_run.return_value = MagicMock(returncode=0, stdout="mock_stdout", stderr="")
    with patch(
        "qq_lib.batch.pbs.qqpbs.parseMultiPBSDumpToDictionaries",
        return_value=[
            ({"data1": "value1"}, "node1"),
            ({"data2": "value2"}, "node2"),
        ],
    ) as mock_parse:
        result = QQPBS.getNodes()

    mock_parse.assert_called_once_with("mock_stdout", None)
    assert isinstance(result, list)
    assert len(result) == 2
    assert all(isinstance(n, PBSNode) for n in result)
    assert {n._name for n in result} == {"node1", "node2"}


def test_qqpbs_get_job_id_returns_value():
    with patch.dict(os.environ, {"PBS_JOBID": "12345.random.server.org"}):
        result = QQPBS.getJobId()
    assert result == "12345.random.server.org"


def test_qqpbs_get_job_id_returns_none_when_missing():
    with patch.dict(os.environ, {}, clear=True):
        result = QQPBS.getJobId()
    assert result is None
