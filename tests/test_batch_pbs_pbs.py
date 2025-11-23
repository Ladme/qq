# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

# ruff: noqa: W291

import os
import shutil
import socket
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.batch.interface import BatchInterface
from qq_lib.batch.pbs import PBS, PBSJob
from qq_lib.batch.pbs.node import PBSNode
from qq_lib.batch.pbs.pbs import CFG
from qq_lib.core.error import QQError
from qq_lib.properties.depend import Depend, DependType
from qq_lib.properties.resources import Resources


@pytest.fixture
def resources():
    return Resources(
        nnodes=1, mem_per_cpu="1gb", ncpus=4, work_dir="scratch_local", work_size="16gb"
    )


def test_translate_kill_force():
    job_id = "123"
    cmd = PBS._translateKillForce(job_id)
    assert cmd == f"qdel -W force {job_id}"


def test_translate_kill():
    job_id = "123"
    cmd = PBS._translateKill(job_id)
    assert cmd == f"qdel {job_id}"


def test_navigate_success(tmp_path):
    directory = tmp_path

    with patch("subprocess.run") as mock_run:
        PBS.navigateToDestination("fake.host.org", directory)
        # check that subprocess was called properly
        mock_run.assert_called_once_with(
            [
                "ssh",
                "-o PasswordAuthentication=no",
                "-o GSSAPIAuthentication=yes",
                f"-o ConnectTimeout={CFG.timeouts.ssh}",
                "fake.host.org",
                "-t",
                f"cd {directory} || exit {BatchInterface.CD_FAIL} && exec bash -l",
            ]
        )

        # should not raise


def test_shared_guard_sets_env_var():
    env_vars = {CFG.env_vars.guard: "true"}

    # patch isShared to return True
    with patch.object(PBS, "isShared", return_value=True):
        PBS._sharedGuard(Resources(work_dir="scratch_local"), env_vars)
        assert env_vars[CFG.env_vars.shared_submit] == "true"
        # previous env vars not removed
        assert env_vars[CFG.env_vars.guard] == "true"


def test_shared_guard_does_not_set_env_var():
    env_vars = {CFG.env_vars.guard: "true"}

    # patch isShared to return False
    with patch.object(PBS, "isShared", return_value=False):
        PBS._sharedGuard(Resources(work_dir="scratch_local"), env_vars)
        assert CFG.env_vars.shared_submit not in env_vars
        # previous env vars not removed
        assert env_vars[CFG.env_vars.guard] == "true"


@pytest.mark.parametrize("dir", ["input_dir", "job_dir"])
def test_shared_guard_input_dir_does_not_raise(dir):
    env_vars = {}

    # patch isShared to return True
    with patch.object(PBS, "isShared", return_value=True):
        PBS._sharedGuard(Resources(work_dir=dir), env_vars)
        assert env_vars[CFG.env_vars.shared_submit] == "true"


@pytest.mark.parametrize("dir", ["input_dir", "job_dir"])
def test_shared_guard_input_dir_raises(dir):
    env_vars = {}

    # patch isShared to return False
    with (
        patch.object(PBS, "isShared", return_value=False),
        pytest.raises(
            QQError,
            match="Job was requested to run directly in the submission directory",
        ),
    ):
        PBS._sharedGuard(Resources(work_dir=dir), env_vars)
        assert CFG.env_vars.shared_submit not in env_vars


def test_sync_with_exclusions_shared_storage_sets_local(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = [Path("file1"), Path("file2")]

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    with patch.object(BatchInterface, "syncWithExclusions") as mock_sync:
        PBS.syncWithExclusions(src_dir, dest_dir, "host1", "host2", exclude_files)
        mock_sync.assert_called_once_with(src_dir, dest_dir, None, None, exclude_files)

    monkeypatch.delenv(CFG.env_vars.shared_submit)


def test_sync_with_exclusions_local_src(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    exclude_files = [Path("file1")]
    local_host = "myhost"

    monkeypatch.setenv(CFG.env_vars.shared_submit, "")

    with (
        patch.object(BatchInterface, "syncWithExclusions") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        # source is local, destination is remote
        PBS.syncWithExclusions(
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
        patch.object(BatchInterface, "syncWithExclusions") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        # destination is local, source is remote
        PBS.syncWithExclusions(
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
        patch.object(BatchInterface, "syncWithExclusions") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        # source local, destination local -> uses None
        PBS.syncWithExclusions(src_dir, dest_dir, None, local_host, exclude_files)
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
        PBS.syncWithExclusions(src_dir, dest_dir, "remote1", "remote2", exclude_files)


def test_sync_selected_shared_storage_sets_local(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    include_files = [Path("file1"), Path("file2")]

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    with patch.object(BatchInterface, "syncSelected") as mock_sync:
        PBS.syncSelected(src_dir, dest_dir, "host1", "host2", include_files)
        mock_sync.assert_called_once_with(src_dir, dest_dir, None, None, include_files)

    monkeypatch.delenv(CFG.env_vars.shared_submit)


def test_sync_selected_local_src(monkeypatch):
    src_dir = Path("/src")
    dest_dir = Path("/dest")
    include_files = [Path("file1")]
    local_host = "myhost"

    monkeypatch.setenv(CFG.env_vars.shared_submit, "")

    with (
        patch.object(BatchInterface, "syncSelected") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        PBS.syncSelected(src_dir, dest_dir, local_host, "remotehost", include_files)
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
        patch.object(BatchInterface, "syncSelected") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        PBS.syncSelected(src_dir, dest_dir, "remotehost", local_host, include_files)
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
        patch.object(BatchInterface, "syncSelected") as mock_sync,
        patch("socket.gethostname", return_value=local_host),
    ):
        PBS.syncSelected(src_dir, dest_dir, None, local_host, include_files)
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
        PBS.syncSelected(src_dir, dest_dir, "remote1", "remote2", include_files)


def test_read_remote_file_shared_storage(tmp_path, monkeypatch):
    file_path = tmp_path / "testfile.txt"
    content = "Hello, QQ!"
    file_path.write_text(content)

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    result = PBS.readRemoteFile("remotehost", file_path)
    assert result == content

    monkeypatch.delenv(CFG.env_vars.shared_submit)


def test_read_remote_file_shared_storage_file_missing(tmp_path, monkeypatch):
    file_path = tmp_path / "nonexistent.txt"

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    with pytest.raises(QQError, match="Could not read file"):
        PBS.readRemoteFile("remotehost", file_path)

    monkeypatch.delenv(CFG.env_vars.shared_submit)


def test_read_remote_file_remote():
    file_path = Path("/remote/file.txt")
    with patch.object(
        BatchInterface, "readRemoteFile", return_value="data"
    ) as mock_read:
        result = PBS.readRemoteFile("remotehost", file_path)
        mock_read.assert_called_once_with("remotehost", file_path)
        assert result == "data"


def test_write_remote_file_shared_storage(tmp_path, monkeypatch):
    file_path = tmp_path / "output.txt"
    content = "Test content"

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    PBS.writeRemoteFile("remotehost", file_path, content)
    assert file_path.read_text() == content


def test_write_remote_file_shared_storage_exception(tmp_path, monkeypatch):
    # using a directory instead of a file to cause write_text to fail
    dir_path = tmp_path / "dir"
    dir_path.mkdir()

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    with pytest.raises(QQError, match="Could not write file"):
        PBS.writeRemoteFile("remotehost", dir_path, "content")


def test_write_remote_file_remote():
    file_path = Path("/remote/output.txt")
    content = "data"

    with patch.object(BatchInterface, "writeRemoteFile") as mock_write:
        PBS.writeRemoteFile("remotehost", file_path, content)
        mock_write.assert_called_once_with("remotehost", file_path, content)


def test_make_remote_dir_shared_storage(tmp_path, monkeypatch):
    dir_path = tmp_path / "newdir"

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    PBS.makeRemoteDir("remotehost", dir_path)

    assert dir_path.exists() and dir_path.is_dir()


def test_make_remote_dir_shared_storage_exception(tmp_path, monkeypatch):
    file_path = tmp_path / "conflict"
    file_path.write_text("dummy")

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    with pytest.raises(QQError, match="Could not create a directory"):
        PBS.makeRemoteDir("remotehost", file_path)


def test_make_remote_dir_shared_storage_already_exists_ok(tmp_path, monkeypatch):
    dir_path = tmp_path / "newdir"
    dir_path.mkdir()

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    # ignore that the directory already exists
    PBS.makeRemoteDir("remotehost", dir_path)

    assert dir_path.exists() and dir_path.is_dir()


def test_make_remote_dir_remote():
    dir_path = Path("/remote/newdir")

    with patch.object(BatchInterface, "makeRemoteDir") as mock_make:
        PBS.makeRemoteDir("remotehost", dir_path)
        mock_make.assert_called_once_with("remotehost", dir_path)


def test_list_remote_dir_shared_storage(tmp_path, monkeypatch):
    (tmp_path / "file1.txt").write_text("one")
    (tmp_path / "file2.txt").write_text("two")
    (tmp_path / "subdir").mkdir()

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    result = PBS.listRemoteDir("remotehost", tmp_path)

    result_names = sorted([p.name for p in result])
    assert result_names == ["file1.txt", "file2.txt", "subdir"]


def test_list_remote_dir_shared_storage_exception(tmp_path, monkeypatch):
    # use a file instead of directory -> .iterdir() should fail
    bad_path = tmp_path / "notadir"
    bad_path.write_text("oops")

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    with pytest.raises(QQError, match="Could not list a directory"):
        PBS.listRemoteDir("remotehost", bad_path)


def test_list_remote_dir_remote():
    dir_path = Path("/remote/dir")

    with patch.object(BatchInterface, "listRemoteDir") as mock_list:
        PBS.listRemoteDir("remotehost", dir_path)
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

    PBS.moveRemoteFiles("remotehost", [src1, src2], [dst1, dst2])

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
        PBS.moveRemoteFiles("remotehost", [bad_src], [dst])


def test_move_remote_files_length_mismatch(tmp_path, monkeypatch):
    src = tmp_path / "file1.txt"
    src.write_text("data")
    dst1 = tmp_path / "dest1.txt"
    dst2 = tmp_path / "dest2.txt"

    monkeypatch.setenv(CFG.env_vars.shared_submit, "true")

    with pytest.raises(QQError, match="must have the same length"):
        PBS.moveRemoteFiles("remotehost", [src], [dst1, dst2])


def test_move_remote_files_remote():
    src = Path("/remote/file.txt")
    dst = Path("/remote/dest.txt")

    with patch.object(BatchInterface, "moveRemoteFiles") as mock_move:
        PBS.moveRemoteFiles("remotehost", [src], [dst])
        mock_move.assert_called_once_with("remotehost", [src], [dst])


def test_translate_work_dir_input_dir_returns_none():
    res = Resources(nnodes=1, work_dir="input_dir")
    assert PBS._translateWorkDir(res) is None


def test_translate_work_dir_scratch_shm_returns_true_string():
    res = Resources(nnodes=3, work_dir="scratch_shm")
    assert PBS._translateWorkDir(res) == "scratch_shm=true"


def test_translate_work_dir_work_size_divided_by_nnodes():
    res = Resources(nnodes=2, work_dir="scratch_local", work_size="7mb")
    result = PBS._translateWorkDir(res)
    assert result == "scratch_local=3584kb"


def test_translate_work_dir_work_size_per_cpu_and_ncpus():
    res = Resources(
        nnodes=4, ncpus=5, work_dir="scratch_local", work_size_per_cpu="3mb"
    )
    result = PBS._translateWorkDir(res)
    assert result == "scratch_local=3840kb"


def test_translate_work_dir_missing_work_size_raises():
    res = Resources(nnodes=2, ncpus=4, work_dir="scratch_local")
    with pytest.raises(QQError, match="work-size"):
        PBS._translateWorkDir(res)


def test_translate_work_dir_missing_ncpus_with_work_size_per_cpu_raises():
    res = Resources(nnodes=2, work_dir="scratch_local", work_size_per_cpu="3mb")
    with pytest.raises(QQError, match="work-size"):
        PBS._translateWorkDir(res)


def test_translate_per_chunk_resources_nnones_missing_raises():
    res = Resources(nnodes=None, ncpus=2, mem="4mb")
    with pytest.raises(QQError, match="nnodes"):
        PBS._translatePerChunkResources(res)


def test_translate_per_chunk_resources_nnones_zero_raises():
    res = Resources(nnodes=0, ncpus=2, mem="4mb")
    with pytest.raises(QQError, match="nnodes"):
        PBS._translatePerChunkResources(res)


def test_translate_per_chunk_resources_ncpus_not_divisible_raises():
    res = Resources(nnodes=3, ncpus=4, mem="4mb")
    with pytest.raises(QQError, match="ncpus"):
        PBS._translatePerChunkResources(res)


def test_translate_per_chunk_resources_ngpus_not_divisible_raises():
    res = Resources(nnodes=2, ncpus=2, ngpus=3, mem="4mb")
    with pytest.raises(QQError, match="ngpus"):
        PBS._translatePerChunkResources(res)


def test_translate_per_chunk_resources_mem_division():
    res = Resources(nnodes=2, ncpus=4, mem="7mb", work_dir="input_dir")
    result = PBS._translatePerChunkResources(res)
    assert "ncpus=2" in result
    assert "mem=3584kb" in result


def test_translate_per_chunk_resources_mem_per_cpu_used():
    res = Resources(nnodes=2, ncpus=4, mem_per_cpu="2mb", work_dir="input_dir")
    result = PBS._translatePerChunkResources(res)
    # 2mb * 4 / 2 = 4mb
    assert "mem=4096kb" in result


def test_translate_per_chunk_resources_ngpus_included():
    res = Resources(nnodes=3, ncpus=9, mem="8mb", ngpus=6, work_dir="input_dir")
    result = PBS._translatePerChunkResources(res)
    assert "ngpus=2" in result


def test_translate_per_chunk_resources_work_dir_translated():
    res = Resources(
        nnodes=2, ncpus=4, mem="8mb", work_dir="scratch_local", work_size="1mb"
    )
    result = PBS._translatePerChunkResources(res)
    assert "scratch_local=512kb" in result


def test_translate_per_chunk_resources_missing_memory_raises():
    res = Resources(nnodes=2, ncpus=4)
    with pytest.raises(QQError, match="mem"):
        PBS._translatePerChunkResources(res)


def test_translate_submit_minimal_fields():
    res = Resources(nnodes=1, ncpus=1, mem="1gb", work_dir="input_dir")
    assert (
        PBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=1,mpiprocs=1,mem=1048576kb script.sh"
    )


def test_translate_submit_ncpus_ngpus_per_node():
    res = Resources(
        nnodes=1, ncpus_per_node=1, ngpus_per_node=1, mem="1gb", work_dir="input_dir"
    )
    assert (
        PBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=1,mpiprocs=1,mem=1048576kb,ngpus=1 script.sh"
    )


def test_translate_submit_with_env_vars():
    res = Resources(nnodes=1, ncpus=1, mem="1gb", work_dir="input_dir")
    assert (
        PBS._translateSubmit(
            res,
            "gpu",
            Path("tmp"),
            "script.sh",
            "job",
            [],
            {CFG.env_vars.guard: "true", CFG.env_vars.batch_system: "PBS"},
        )
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -v \"{CFG.env_vars.guard}='true'\",\"{CFG.env_vars.batch_system}='PBS'\" -l ncpus=1,mpiprocs=1,mem=1048576kb script.sh"
    )


def test_translate_submit_multiple_nodes():
    res = Resources(nnodes=4, ncpus=8, mem="1gb", work_dir="input_dir")
    assert (
        PBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l select=4:ncpus=2:mpiprocs=2:mem=262144kb -l place=vscatter script.sh"
    )


def test_translate_submit_multiple_nodes_ncpus_and_ngpus_per_node():
    res = Resources(
        nnodes=4, ncpus_per_node=8, ngpus_per_node=1, mem="1gb", work_dir="input_dir"
    )
    assert (
        PBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l select=4:ncpus=8:mpiprocs=8:mem=262144kb:ngpus=1 -l place=vscatter script.sh"
    )


def test_translate_submit_multiple_nodes_with_env_vars():
    res = Resources(nnodes=4, ncpus=8, mem="1gb", work_dir="input_dir")
    assert (
        PBS._translateSubmit(
            res,
            "gpu",
            Path("tmp"),
            "script.sh",
            "job",
            [],
            {CFG.env_vars.guard: "true", CFG.env_vars.batch_system: "PBS"},
        )
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -v \"{CFG.env_vars.guard}='true'\",\"{CFG.env_vars.batch_system}='PBS'\" -l select=4:ncpus=2:mpiprocs=2:mem=262144kb -l place=vscatter script.sh"
    )


def test_translate_submit_with_walltime():
    res = Resources(
        nnodes=1, ncpus=2, mem="2gb", walltime="1d24m121s", work_dir="input_dir"
    )
    assert (
        PBS._translateSubmit(res, "queue", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q queue -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=2,mpiprocs=2,mem=2097152kb -l walltime=24:26:01 script.sh"
    )


def test_translate_submit_with_walltime2():
    res = Resources(
        nnodes=1, ncpus=2, mem="2gb", walltime="12:30:15", work_dir="input_dir"
    )
    assert (
        PBS._translateSubmit(res, "queue", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q queue -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=2,mpiprocs=2,mem=2097152kb -l walltime=12:30:15 script.sh"
    )


def test_translate_submit_with_walltime_and_env_vars():
    res = Resources(
        nnodes=1, ncpus=2, mem="2gb", walltime="1d24m121s", work_dir="input_dir"
    )
    assert (
        PBS._translateSubmit(
            res,
            "queue",
            Path("tmp"),
            "script.sh",
            "job",
            [],
            {CFG.env_vars.guard: "true", CFG.env_vars.batch_system: "PBS"},
        )
        == f"qsub -N job -q queue -j eo -e tmp/job{CFG.suffixes.qq_out} -v \"{CFG.env_vars.guard}='true'\",\"{CFG.env_vars.batch_system}='PBS'\" -l ncpus=2,mpiprocs=2,mem=2097152kb -l walltime=24:26:01 script.sh"
    )


def test_translate_submit_work_dir_scratch_shm():
    res = Resources(nnodes=1, ncpus=1, mem="8gb", work_dir="scratch_shm")
    assert (
        PBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=1,mpiprocs=1,mem=8388608kb,scratch_shm=true script.sh"
    )


def test_translate_submit_scratch_local_work_size():
    res = Resources(
        nnodes=2, ncpus=2, mem="4gb", work_dir="scratch_local", work_size="16gb"
    )
    assert (
        PBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l select=2:ncpus=1:mpiprocs=1:mem=2097152kb:scratch_local=8388608kb -l place=vscatter script.sh"
    )


def test_translate_submit_scratch_local_work_size_per_node():
    res = Resources(
        nnodes=2,
        ncpus=2,
        mem="4gb",
        work_dir="scratch_local",
        work_size_per_node="16gb",
    )
    assert (
        PBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l select=2:ncpus=1:mpiprocs=1:mem=2097152kb:scratch_local=16777216kb -l place=vscatter script.sh"
    )


def test_translate_submit_scratch_ssd_work_size():
    res = Resources(
        nnodes=2, ncpus=2, mem="4gb", work_dir="scratch_ssd", work_size="16gb"
    )
    assert (
        PBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l select=2:ncpus=1:mpiprocs=1:mem=2097152kb:scratch_ssd=8388608kb -l place=vscatter script.sh"
    )


def test_translate_submit_scratch_shared_work_size():
    res = Resources(
        nnodes=2, ncpus=2, mem="4gb", work_dir="scratch_shared", work_size="16gb"
    )
    assert (
        PBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l select=2:ncpus=1:mpiprocs=1:mem=2097152kb:scratch_shared=8388608kb -l place=vscatter script.sh"
    )


def test_translate_submit_work_size_per_cpu():
    res = Resources(
        nnodes=1, ncpus=8, mem="4gb", work_dir="scratch_local", work_size_per_cpu="2gb"
    )
    assert (
        PBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=8,mpiprocs=8,mem=4194304kb,scratch_local=16777216kb script.sh"
    )


def test_translate_submit_work_size_per_cpu_with_cpus_per_node():
    res = Resources(
        nnodes=1,
        ncpus_per_node=8,
        mem="4gb",
        work_dir="scratch_local",
        work_size_per_cpu="2gb",
    )
    assert (
        PBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=8,mpiprocs=8,mem=4194304kb,scratch_local=16777216kb script.sh"
    )


def test_translate_submit_work_size_per_cpu_multiple_nodes():
    res = Resources(
        nnodes=3, ncpus=3, mem="4gb", work_dir="scratch_local", work_size_per_cpu="2gb"
    )
    assert (
        PBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l select=3:ncpus=1:mpiprocs=1:mem=1398102kb:scratch_local=2097152kb -l place=vscatter script.sh"
    )


def test_translate_submit_mem_per_cpu():
    res = Resources(
        nnodes=1, ncpus=4, mem_per_cpu="2gb", work_dir="scratch_local", work_size="10gb"
    )
    assert (
        PBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=4,mpiprocs=4,mem=8388608kb,scratch_local=10485760kb script.sh"
    )


def test_translate_submit_mem_per_cpu_with_ncpus_per_node():
    res = Resources(
        nnodes=1,
        ncpus_per_node=4,
        mem_per_cpu="2gb",
        work_dir="scratch_local",
        work_size="10gb",
    )
    assert (
        PBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=4,mpiprocs=4,mem=8388608kb,scratch_local=10485760kb script.sh"
    )


def test_translate_submit_mem_per_node():
    res = Resources(
        nnodes=1,
        ncpus=4,
        mem_per_node="8gb",
        work_dir="scratch_local",
        work_size="10gb",
    )
    assert (
        PBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=4,mpiprocs=4,mem=8388608kb,scratch_local=10485760kb script.sh"
    )


def test_translate_submit_mem_per_cpu_multiple_nodes():
    res = Resources(
        nnodes=2, ncpus=4, mem_per_cpu="2gb", work_dir="scratch_local", work_size="20gb"
    )
    assert (
        PBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l select=2:ncpus=2:mpiprocs=2:mem=4194304kb:scratch_local=10485760kb -l place=vscatter script.sh"
    )


def test_translate_submit_mem_per_node_multiple_nodes():
    res = Resources(
        nnodes=2,
        ncpus=4,
        mem_per_node="4gb",
        work_dir="scratch_local",
        work_size="20gb",
    )
    assert (
        PBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l select=2:ncpus=2:mpiprocs=2:mem=4194304kb:scratch_local=10485760kb -l place=vscatter script.sh"
    )


def test_translate_submit_mem_per_cpu_and_work_size_per_cpu():
    res = Resources(
        nnodes=1,
        ncpus=4,
        mem_per_cpu="2gb",
        work_dir="scratch_local",
        work_size_per_cpu="5gb",
    )
    assert (
        PBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=4,mpiprocs=4,mem=8388608kb,scratch_local=20971520kb script.sh"
    )


def test_translate_submit_mem_per_cpu_and_work_size_per_cpu_multiple_nodes():
    res = Resources(
        nnodes=2,
        ncpus=4,
        mem_per_cpu="2gb",
        work_dir="scratch_local",
        work_size_per_cpu="5gb",
    )
    assert (
        PBS._translateSubmit(res, "gpu", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q gpu -j eo -e tmp/job{CFG.suffixes.qq_out} -l select=2:ncpus=2:mpiprocs=2:mem=4194304kb:scratch_local=10485760kb -l place=vscatter script.sh"
    )


def test_translate_submit_with_props():
    res = Resources(
        nnodes=1,
        ncpus=1,
        mem="1gb",
        props={"vnode": "my_node", "infiniband": "true"},
        work_dir="input_dir",
    )
    assert (
        PBS._translateSubmit(res, "queue", Path("tmp"), "script.sh", "job", [], {})
        == f"qsub -N job -q queue -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=1,mpiprocs=1,mem=1048576kb,vnode=my_node,infiniband=true script.sh"
    )


def test_translate_submit_with_props_and_env_vars():
    res = Resources(
        nnodes=1,
        ncpus=1,
        mem="1gb",
        props={"vnode": "my_node", "infiniband": "true"},
        work_dir="input_dir",
    )
    assert (
        PBS._translateSubmit(
            res,
            "queue",
            Path("tmp"),
            "script.sh",
            "job",
            [],
            {CFG.env_vars.guard: "true", CFG.env_vars.batch_system: "PBS"},
        )
        == f"qsub -N job -q queue -j eo -e tmp/job{CFG.suffixes.qq_out} -v \"{CFG.env_vars.guard}='true'\",\"{CFG.env_vars.batch_system}='PBS'\" -l ncpus=1,mpiprocs=1,mem=1048576kb,vnode=my_node,infiniband=true script.sh"
    )


def test_translate_submit_complex_case():
    res = Resources(
        nnodes=3,
        ncpus=6,
        mem="5gb",
        ngpus=3,
        walltime="1h30m",
        work_dir="scratch_local",
        work_size_per_cpu="2gb",
        props={"cl_cluster": "true"},
    )
    assert PBS._translateSubmit(
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
        f"-l select=3:ncpus=2:mpiprocs=2:mem=1747627kb:ngpus=1:scratch_local=4194304kb:cl_cluster=true "
        f"-l walltime=1:30:00 -l place=vscatter myscript.sh"
    )


def test_translate_submit_single_depend():
    res = Resources(nnodes=1, ncpus=1, mem="1gb", work_dir="input_dir")
    depend = [Depend(DependType.AFTER_START, ["123"])]
    cmd = PBS._translateSubmit(
        res, "queue", Path("tmp"), "script.sh", "job", depend, {}
    )
    expected = f"qsub -N job -q queue -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=1,mpiprocs=1,mem=1048576kb -W depend=after:123 script.sh"
    assert cmd == expected


def test_translate_submit_multiple_jobs_depend():
    res = Resources(nnodes=1, ncpus=1, mem="1gb", work_dir="input_dir")
    depend = [Depend(DependType.AFTER_SUCCESS, ["1", "2"])]
    cmd = PBS._translateSubmit(
        res, "queue", Path("tmp"), "script.sh", "job", depend, {}
    )
    expected = f"qsub -N job -q queue -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=1,mpiprocs=1,mem=1048576kb -W depend=afterok:1:2 script.sh"
    assert cmd == expected


def test_translate_submit_multiple_dependencies():
    res = Resources(nnodes=1, ncpus=1, mem="1gb", work_dir="input_dir")
    depend = [
        Depend(DependType.AFTER_SUCCESS, ["1"]),
        Depend(DependType.AFTER_FAILURE, ["2"]),
    ]
    cmd = PBS._translateSubmit(
        res, "queue", Path("tmp"), "script.sh", "job", depend, {}
    )
    expected = f"qsub -N job -q queue -j eo -e tmp/job{CFG.suffixes.qq_out} -l ncpus=1,mpiprocs=1,mem=1048576kb -W depend=afterok:1,afternotok:2 script.sh"
    assert cmd == expected


def test_translate_submit_complex_with_depend():
    res = Resources(
        nnodes=2,
        ncpus=4,
        mem="4gb",
        walltime="01:00:00",
        work_dir="scratch_local",
        work_size_per_cpu="2gb",
        props={"cl_cluster": "true"},
    )
    depend = [Depend(DependType.AFTER_COMPLETION, ["42", "43"])]
    cmd = PBS._translateSubmit(
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
        f"-l select=2:ncpus=2:mpiprocs=2:mem=2097152kb:scratch_local=4194304kb:cl_cluster=true "
        "-l walltime=01:00:00 -l place=vscatter -W depend=afterany:42:43 myscript.sh"
    )
    assert cmd == expected


def test_transform_resources_input_dir_warns_and_sets_work_dir():
    provided = Resources(work_dir="input_dir", work_size="10gb")
    with (
        patch("qq_lib.batch.pbs.pbs.PBSQueue") as mock_queue,
        patch.object(PBS, "_getDefaultServerResources", return_value=Resources()),
        patch.object(Resources, "mergeResources", return_value=provided),
        patch("qq_lib.batch.pbs.pbs.logger.warning") as mock_warning,
    ):
        mock_instance = MagicMock()
        mock_queue.return_value = mock_instance
        mock_instance.getDefaultResources.return_value = Resources()

        res = PBS.transformResources(
            "gpu", Resources(work_dir="input_dir", work_size="10gb")
        )

    assert res.work_dir == "input_dir"

    called_args = mock_warning.call_args[0]
    assert "Setting work-size is not supported" in called_args[0]
    assert "input_dir" in called_args[0]


def test_transform_resources_job_dir_warns_and_sets_work_dir():
    provided = Resources(work_dir="input_dir", work_size="10gb")
    with (
        patch("qq_lib.batch.pbs.pbs.PBSQueue") as mock_queue,
        patch.object(PBS, "_getDefaultServerResources", return_value=Resources()),
        patch.object(Resources, "mergeResources", return_value=provided),
        patch("qq_lib.batch.pbs.pbs.logger.warning") as mock_warning,
    ):
        mock_instance = MagicMock()
        mock_queue.return_value = mock_instance
        mock_instance.getDefaultResources.return_value = Resources()

        res = PBS.transformResources(
            "gpu", Resources(work_dir="job_dir", work_size="10gb")
        )

    assert res.work_dir == "input_dir"

    called_args = mock_warning.call_args[0]
    assert "Setting work-size is not supported" in called_args[0]
    assert "job_dir" in called_args[0]


def test_transform_resources_scratch_shm_warns_and_clears_work_size():
    provided = Resources(work_dir="scratch_shm", work_size="10gb")
    with (
        patch("qq_lib.batch.pbs.pbs.PBSQueue") as mock_queue,
        patch.object(PBS, "_getDefaultServerResources", return_value=Resources()),
        patch.object(Resources, "mergeResources", return_value=provided),
        patch("qq_lib.batch.pbs.pbs.logger.warning") as mock_warning,
    ):
        mock_instance = MagicMock()
        mock_queue.return_value = mock_instance
        mock_instance.getDefaultResources.return_value = Resources()

        res = PBS.transformResources(
            "gpu", Resources(work_dir="scratch_shm", work_size="10gb")
        )

    assert res.work_dir == "scratch_shm"
    assert res.work_size is None

    called_args = mock_warning.call_args[0]
    assert "Setting work-size is not supported" in called_args[0]
    assert "scratch_shm" in called_args[0]


def test_transform_resources_supported_scratch():
    for scratch in PBS.SUPPORTED_SCRATCHES:
        provided = Resources(work_dir=scratch, work_size="10gb")
        with (
            patch("qq_lib.batch.pbs.pbs.PBSQueue") as mock_queue,
            patch.object(PBS, "_getDefaultServerResources", return_value=Resources()),
            patch.object(Resources, "mergeResources", return_value=provided),
        ):
            mock_instance = MagicMock()
            mock_queue.return_value = mock_instance
            mock_instance.getDefaultResources.return_value = Resources()

            res = PBS.transformResources(
                "gpu", Resources(work_dir=scratch, work_size="10gb")
            )

        assert res.work_dir == scratch


def test_transform_resources_supported_scratch_unnormalized():
    for scratch in PBS.SUPPORTED_SCRATCHES:
        provided = Resources(
            work_dir=scratch.upper().replace("_", "-"), work_size="10gb"
        )
        with (
            patch("qq_lib.batch.pbs.pbs.PBSQueue") as mock_queue,
            patch.object(PBS, "_getDefaultServerResources", return_value=Resources()),
            patch.object(Resources, "mergeResources", return_value=provided),
        ):
            mock_instance = MagicMock()
            mock_queue.return_value = mock_instance
            mock_instance.getDefaultResources.return_value = Resources()

            res = PBS.transformResources(
                "gpu",
                Resources(work_dir=scratch.upper().replace("_", "-"), work_size="10gb"),
            )

        assert res.work_dir == scratch


def test_transform_resources_unknown_work_dir_raises():
    provided = Resources(work_dir="unknown_scratch")
    with (
        patch("qq_lib.batch.pbs.pbs.PBSQueue") as mock_queue,
        patch.object(PBS, "_getDefaultServerResources", return_value=Resources()),
        patch.object(Resources, "mergeResources", return_value=provided),
        pytest.raises(QQError, match="Unknown working directory type specified"),
    ):
        mock_instance = MagicMock()
        mock_queue.return_value = mock_instance
        mock_instance.getDefaultResources.return_value = Resources()

        PBS.transformResources("gpu", Resources(work_dir="unknown_scratch"))


def test_transform_resources_missing_work_dir_raises():
    provided = Resources(work_dir=None)
    with (
        patch("qq_lib.batch.pbs.pbs.PBSQueue") as mock_queue,
        patch.object(PBS, "_getDefaultServerResources", return_value=Resources()),
        patch.object(Resources, "mergeResources", return_value=provided),
        pytest.raises(
            QQError, match="Work-dir is not set after filling in default attributes"
        ),
    ):
        mock_instance = MagicMock()
        mock_queue.return_value = mock_instance
        mock_instance.getDefaultResources.return_value = Resources()

        PBS.transformResources("gpu", Resources())


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

        jobs = PBS._getBatchJobsUsingCommand("fake command - unused")

        assert len(jobs) == 3
        assert all(isinstance(job, PBSJob) for job in jobs)

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
            PBS._getBatchJobsUsingCommand("will not be used")


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
    result = PBS._translateDependencies(depend_list)
    assert result == expected


def test_collect_ams_env_vars(monkeypatch):
    from qq_lib.batch.pbs.pbs import PBS

    # mock environment with a mix of AMS and non-AMS vars
    env_vars = {
        "AMS_ACTIVE_MODULES": "mod1,mod2",
        "AMS_ROOT": "/opt/ams",
        "OTHER_VAR": "ignore_me",
        "AMS_BUNDLE_PATH": "/ams/bundle",
        "PATH": "/usr/bin",
    }
    monkeypatch.setattr(os, "environ", env_vars)

    result = PBS._collectAMSEnvVars()

    # assert that only AMS variables were collected
    expected = {
        "AMS_ACTIVE_MODULES": "mod1,mod2",
        "AMS_ROOT": "/opt/ams",
        "AMS_BUNDLE_PATH": "/ams/bundle",
    }
    assert result == expected


@patch("qq_lib.batch.pbs.pbs.subprocess.run")
def test_pbs_get_queues_returns_list(mock_run):
    mock_run.return_value = MagicMock(returncode=0, stdout="mock_stdout", stderr="")

    with (
        patch(
            "qq_lib.batch.pbs.pbs.parse_multi_pbs_dump_to_dictionaries",
            return_value=[({"key": "value"}, "queue1")],
        ) as mock_parse,
        patch(
            "qq_lib.batch.pbs.pbs.PBSQueue.fromDict", return_value="mock_queue"
        ) as mock_from_dict,
    ):
        result = PBS.getQueues()

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


@patch("qq_lib.batch.pbs.pbs.subprocess.run")
def test_pbs_get_queues_raises_on_failure(mock_run):
    mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error_message")

    with pytest.raises(QQError, match="error_message"):
        PBS.getQueues()


@patch("qq_lib.batch.pbs.pbs.subprocess.run")
def test_pbs_get_queues_multiple_queues(mock_run):
    mock_run.return_value = MagicMock(returncode=0, stdout="mock_stdout", stderr="")

    with (
        patch(
            "qq_lib.batch.pbs.pbs.parse_multi_pbs_dump_to_dictionaries",
            return_value=[
                ({"data1": "value1"}, "queue1"),
                ({"data2": "value2"}, "queue2"),
            ],
        ) as mock_parse,
        patch(
            "qq_lib.batch.pbs.pbs.PBSQueue.fromDict",
            side_effect=["queue_obj1", "queue_obj2"],
        ) as mock_from_dict,
    ):
        result = PBS.getQueues()

    mock_parse.assert_called_once_with("mock_stdout", "Queue")
    assert mock_from_dict.call_count == 2

    assert result == ["queue_obj1", "queue_obj2"]


@patch("qq_lib.batch.pbs.pbs.subprocess.run")
def test_pbs_get_nodes_returns_list(mock_run):
    mock_run.return_value = MagicMock(returncode=0, stdout="mock_stdout", stderr="")
    with patch(
        "qq_lib.batch.pbs.pbs.parse_multi_pbs_dump_to_dictionaries",
        return_value=[({"key": "value"}, "node1")],
    ) as mock_parse:
        result = PBS.getNodes()

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


@patch("qq_lib.batch.pbs.pbs.subprocess.run")
def test_pbs_get_nodes_raises_on_failure(mock_run):
    mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="error_message")
    with pytest.raises(QQError, match="error_message"):
        PBS.getNodes()


@patch("qq_lib.batch.pbs.pbs.subprocess.run")
def test_pbs_get_nodes_multiple_nodes(mock_run):
    mock_run.return_value = MagicMock(returncode=0, stdout="mock_stdout", stderr="")
    with patch(
        "qq_lib.batch.pbs.pbs.parse_multi_pbs_dump_to_dictionaries",
        return_value=[
            ({"data1": "value1"}, "node1"),
            ({"data2": "value2"}, "node2"),
        ],
    ) as mock_parse:
        result = PBS.getNodes()

    mock_parse.assert_called_once_with("mock_stdout", None)
    assert isinstance(result, list)
    assert len(result) == 2
    assert all(isinstance(n, PBSNode) for n in result)
    assert {n._name for n in result} == {"node1", "node2"}


def test_pbs_get_job_id_returns_value():
    with patch.dict(os.environ, {"PBS_JOBID": "12345.random.server.org"}):
        result = PBS.getJobId()
    assert result == "12345.random.server.org"


def test_pbs_get_job_id_returns_none_when_missing():
    with patch.dict(os.environ, {}, clear=True):
        result = PBS.getJobId()
    assert result is None


@pytest.mark.parametrize(
    "ids,expected_order",
    [
        (["3.server", "1.server", "2.server"], ["1.server", "2.server", "3.server"]),
        (["10.server", "2.server", "1.server"], ["1.server", "2.server", "10.server"]),
    ],
)
def test_pbs_sort_jobs_sorts_by_id_int(ids, expected_order):
    jobs = []
    for job_id in ids:
        job = PBSJob.__new__(PBSJob)
        job._job_id = job_id
        jobs.append(job)

    PBS.sortJobs(jobs)

    result = [job.getId() for job in jobs]
    assert result == expected_order


def test_pbs_sort_jobs_handles_none_values(monkeypatch):
    # jobs returning None from getIdInt should sort to the beginning
    job_valid = PBSJob.__new__(PBSJob)
    job_valid._job_id = "1.server"
    job_none = PBSJob.__new__(PBSJob)
    job_none._job_id = "abc"
    monkeypatch.setattr(job_none, "getIdInt", lambda: None)

    jobs = [job_valid, job_none]
    PBS.sortJobs(jobs)

    result = [job.getId() for job in jobs]
    assert result == ["abc", "1.server"]


def test_pbs_delete_remote_dir_deletes_local(tmp_path):
    test_dir = tmp_path / "to_delete"
    test_dir.mkdir()
    (test_dir / "file.txt").write_text("content")

    assert test_dir.exists()

    host = socket.gethostname()
    PBS.deleteRemoteDir(host, test_dir)

    assert not test_dir.exists()


def test_pbs_delete_remote_dir_raises_error_on_local_failure(tmp_path, monkeypatch):
    test_dir = tmp_path / "to_delete_fail"
    test_dir.mkdir()

    def mock_rmtree(_):
        raise PermissionError("access denied")

    monkeypatch.setattr(shutil, "rmtree", mock_rmtree)
    host = socket.gethostname()

    with pytest.raises(
        QQError, match=f"Could not delete directory '{test_dir}': access denied."
    ):
        PBS.deleteRemoteDir(host, test_dir)


@patch("qq_lib.batch.pbs.pbs.super")
def test_pbs_delete_remote_dir_calls_super_for_remote_host(mock_super):
    mock_super().deleteRemoteDir = patch(
        "qq_lib.batch.pbs.pbs.BatchInterface.deleteRemoteDir"
    ).start()

    host = "remote_host"
    directory = Path("/tmp/remotedir")

    PBS.deleteRemoteDir(host, directory)

    mock_super().deleteRemoteDir.assert_called_once_with(host, directory)


def test_pbs_get_supported_work_dir_types_returns_combined_list():
    expected = [
        "scratch_local",
        "scratch_ssd",
        "scratch_shared",
        "scratch_shm",
        "input_dir",
        "job_dir",
    ]
    assert PBS.getSupportedWorkDirTypes() == expected


def test_pbs_create_work_dir_on_scratch_creates_work_dir():
    job_id = "12345"
    fake_scratch = Path("/scratch/job_12345")
    inner_name = CFG.pbs_options.scratch_dir_inner
    expected_work_dir = (fake_scratch / inner_name).resolve()

    with (
        patch.object(
            PBS, "_getScratchDir", return_value=fake_scratch
        ) as get_scratch_mock,
        patch("qq_lib.batch.pbs.pbs.logger"),
        patch("pathlib.Path.mkdir") as mkdir_mock,
    ):
        result = PBS.createWorkDirOnScratch(job_id)

    get_scratch_mock.assert_called_once_with(job_id)

    assert result == expected_work_dir

    mkdir_mock.assert_called_once_with(exist_ok=True)
