# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.batch.slurm.job import SlurmJob
from qq_lib.batch.slurm.node import SlurmNode
from qq_lib.batch.slurm.slurm import Slurm
from qq_lib.core.error import QQError
from qq_lib.properties.depend import Depend, DependType
from qq_lib.properties.resources import Resources
from qq_lib.properties.size import Size


def test_slurm_env_name_returns_slurm():
    assert Slurm.envName() == "Slurm"


@patch(
    "qq_lib.batch.slurm.slurm.shutil.which",
    side_effect=lambda x: "/usr/bin/sbatch" if x == "sbatch" else None,
)
def test_slurm_is_available_returns_true_when_sbatch_present(
    mock_which,
):
    result = Slurm.isAvailable()
    assert result is True
    mock_which.assert_any_call("sbatch")


@patch(
    "qq_lib.batch.slurm.slurm.shutil.which",
    side_effect=lambda _: None,
)
def test_slurm_is_available_returns_false_when_sbatch_missing(mock_which):
    result = Slurm.isAvailable()
    assert result is False
    mock_which.assert_any_call("sbatch")


@patch(
    "qq_lib.batch.slurm.slurm.shutil.which",
    side_effect=lambda x: "/usr/bin/sbatch" if x == "sbatch" else "/usr/bin/it4ifree",
)
def test_slurm_is_available_returns_false_when_it4ifree_present(mock_which):
    result = Slurm.isAvailable()
    assert result is False
    mock_which.assert_any_call("sbatch")
    mock_which.assert_any_call("it4ifree")


@patch.dict("qq_lib.batch.slurm.slurm.os.environ", {"SLURM_JOB_ID": "12345"})
def test_slurm_get_job_id_returns_value_from_env():
    assert Slurm.getJobId() == "12345"


@patch.dict("qq_lib.batch.slurm.slurm.os.environ", {}, clear=True)
def test_slurm_get_job_id_returns_none_when_missing():
    assert Slurm.getJobId() is None


@patch("qq_lib.batch.slurm.slurm.subprocess.run")
@patch("qq_lib.batch.slurm.slurm.SlurmJob")
def test_slurm_get_batch_jobs_calls_slurmjob(mock_job, mock_run):
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "111\n222\n333\n"
    mock_run.return_value = mock_result

    jobs = Slurm._getBatchJobsUsingSqueueCommand("squeue -u user")

    mock_run.assert_called_once()
    assert len(jobs) == 3
    mock_job.assert_any_call("111")
    mock_job.assert_any_call("222")
    mock_job.assert_any_call("333")


@patch("qq_lib.batch.slurm.slurm.subprocess.run")
def test_slurm_get_batch_jobs_raises_on_error(mock_run):
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stderr = "error"
    mock_run.return_value = mock_result

    with pytest.raises(QQError):
        Slurm._getBatchJobsUsingSqueueCommand("squeue -u user")

    mock_run.assert_called_once()


@patch("qq_lib.batch.slurm.slurm.subprocess.run")
@patch("qq_lib.batch.slurm.slurm.SlurmJob")
def test_slurm_get_batch_jobs_skips_empty_lines(mock_job, mock_run):
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "111\n\n222\n"
    mock_run.return_value = mock_result

    jobs = Slurm._getBatchJobsUsingSqueueCommand("squeue -u user")

    assert len(jobs) == 2
    mock_job.assert_any_call("111")
    mock_job.assert_any_call("222")
    mock_run.assert_called_once()


@patch("qq_lib.batch.slurm.slurm.subprocess.run")
@patch("qq_lib.batch.slurm.slurm.SlurmJob.fromSacctString")
def test_slurm_get_batch_jobs_sacct_calls_fromsacctstring(mock_from_sacct, mock_run):
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "job1|info\njob2|info\n"
    mock_run.return_value = mock_result

    jobs = Slurm._getBatchJobsUsingSacctCommand("sacct -u user")

    mock_run.assert_called_once()
    assert len(jobs) == 2
    mock_from_sacct.assert_any_call("job1|info")
    mock_from_sacct.assert_any_call("job2|info")


@patch("qq_lib.batch.slurm.slurm.subprocess.run")
def test_slurm_get_batch_jobs_sacct_raises_on_error(mock_run):
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stderr = "error"
    mock_run.return_value = mock_result

    with pytest.raises(QQError):
        Slurm._getBatchJobsUsingSacctCommand("sacct -u user")

    mock_run.assert_called_once()


@patch("qq_lib.batch.slurm.slurm.subprocess.run")
@patch("qq_lib.batch.slurm.slurm.SlurmJob.fromSacctString")
def test_slurm_get_batch_jobs_sacct_skips_empty_lines(mock_from_sacct, mock_run):
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "job1|info\n\njob2|info\n"
    mock_run.return_value = mock_result

    jobs = Slurm._getBatchJobsUsingSacctCommand("sacct -u user")

    assert len(jobs) == 2
    mock_from_sacct.assert_any_call("job1|info")
    mock_from_sacct.assert_any_call("job2|info")
    mock_run.assert_called_once()


@patch("qq_lib.batch.slurm.slurm.Resources.mergeResources")
@patch("qq_lib.batch.slurm.slurm.Slurm._getDefaultResources")
@patch("qq_lib.batch.slurm.slurm.default_resources_from_dict")
@patch("qq_lib.batch.slurm.slurm.parse_slurm_dump_to_dictionary")
@patch("qq_lib.batch.slurm.slurm.subprocess.run")
def test_slurm_get_default_server_resources_merges_parsed_and_defaults(
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

    result = Slurm._getDefaultServerResources()

    mock_run.assert_called_once()
    mock_parse.assert_called_once_with("DefaultTime=2-00:00:00\nDefMemPerCPU=4G", "\n")
    mock_from_dict.assert_called_once_with(
        {"DefaultTime": "2-00:00:00", "DefMemPerCPU": "4G"}
    )
    mock_get_defaults.assert_called_once()
    mock_merge.assert_called_once_with(server_res, default_res)
    assert result is merged_res


@patch("qq_lib.batch.slurm.slurm.Resources.mergeResources")
@patch("qq_lib.batch.slurm.slurm.Slurm._getDefaultResources")
@patch("qq_lib.batch.slurm.slurm.default_resources_from_dict")
@patch("qq_lib.batch.slurm.slurm.parse_slurm_dump_to_dictionary")
@patch("qq_lib.batch.slurm.slurm.subprocess.run")
def test_slurm_get_default_server_resources_returns_empty_on_failure(
    mock_run, mock_parse, mock_from_dict, mock_get_defaults, mock_merge
):
    mock_run.return_value = MagicMock(returncode=1, stderr="err")

    result = Slurm._getDefaultServerResources()

    mock_run.assert_called_once()
    mock_parse.assert_not_called()
    mock_from_dict.assert_not_called()
    mock_get_defaults.assert_not_called()
    mock_merge.assert_not_called()
    assert isinstance(result, Resources)
    assert result == Resources()


def test_slurm_translate_dependencies_returns_none_for_empty_list():
    assert Slurm._translateDependencies([]) is None


def test_slurm_translate_dependencies_returns_single_dependency_string():
    depend = Depend(DependType.AFTER_START, ["123"])
    result = Slurm._translateDependencies([depend])
    assert result == "after:123"


def test_slurm_translate_dependencies_returns_multiple_dependency_string():
    depend1 = Depend(DependType.AFTER_SUCCESS, ["111", "222"])
    depend2 = Depend(DependType.AFTER_FAILURE, ["333"])
    result = Slurm._translateDependencies([depend1, depend2])
    assert result == "afterok:111:222,afternotok:333"


def test_slurm_translate_per_chunk_resources_two_nodes():
    res = Resources()
    res.nnodes = 2
    res.ncpus = 8
    res.mem = Size(32, "gb")
    res.ngpus = 4
    result = Slurm._translatePerChunkResources(res)
    assert "--ntasks-per-node=1" in result
    assert "--cpus-per-task=4" in result
    assert f"--mem={(res.mem // res.nnodes).toStrExactSlurm()}" in result
    assert "--gpus-per-node=2" in result


def test_slurm_translate_per_chunk_resources_two_nodes_per_node_resources():
    res = Resources()
    res.nnodes = 2
    res.ncpus_per_node = 8
    res.mem_per_node = Size(32, "gb")
    res.ngpus_per_node = 4
    result = Slurm._translatePerChunkResources(res)
    assert "--ntasks-per-node=1" in result
    assert "--cpus-per-task=8" in result
    assert f"--mem={res.mem_per_node.toStrExactSlurm()}" in result
    assert "--gpus-per-node=4" in result


def test_slurm_translate_per_chunk_resources_single_node():
    res = Resources()
    res.nnodes = 1
    res.ncpus = 4
    res.mem = Size(16, "gb")
    res.ngpus = 2
    result = Slurm._translatePerChunkResources(res)
    assert "--ntasks-per-node=1" in result
    assert "--cpus-per-task=4" in result
    assert f"--mem={res.mem.toStrExactSlurm()}" in result
    assert "--gpus-per-node=2" in result


def test_slurm_translate_per_chunk_resources_single_node_per_node_resources():
    res = Resources()
    res.nnodes = 1
    res.ncpus_per_node = 4
    res.mem_per_node = Size(16, "gb")
    res.ngpus_per_node = 2
    result = Slurm._translatePerChunkResources(res)
    assert "--ntasks-per-node=1" in result
    assert "--cpus-per-task=4" in result
    assert f"--mem={res.mem_per_node.toStrExactSlurm()}" in result
    assert "--gpus-per-node=2" in result


def test_slurm_translate_per_chunk_resources_multiple_nodes():
    res = Resources()
    res.nnodes = 5
    res.ncpus = 10
    res.mem = Size(50, "gb")
    res.ngpus = 5
    result = Slurm._translatePerChunkResources(res)
    assert "--ntasks-per-node=1" in result
    assert "--cpus-per-task=2" in result
    assert f"--mem={(res.mem // res.nnodes).toStrExactSlurm()}" in result
    assert "--gpus-per-node=1" in result


def test_slurm_translate_per_chunk_resources_uses_mem_per_cpu():
    res = Resources()
    res.nnodes = 2
    res.ncpus = 8
    res.mem = None
    res.mem_per_cpu = Size(4, "gb")
    res.ngpus = 0
    result = Slurm._translatePerChunkResources(res)
    assert f"--mem-per-cpu={res.mem_per_cpu.toStrExactSlurm()}" in result


def test_slurm_translate_per_chunk_resources_raises_when_mem_missing():
    res = Resources()
    res.nnodes = 1
    res.mem = None
    res.mem_per_cpu = None
    with pytest.raises(
        QQError,
        match="None of the attributes 'mem', 'mem-per-node', or 'mem-per-cpu' is defined.",
    ):
        Slurm._translatePerChunkResources(res)


@pytest.mark.parametrize("nnodes", [None, 0])
def test_slurm_translate_per_chunk_resources_invalid_nnodes(nnodes):
    res = Resources()
    res.nnodes = nnodes
    res.mem = Size(16, "gb")
    with pytest.raises(QQError, match="Attribute 'nnodes'"):
        Slurm._translatePerChunkResources(res)


def test_slurm_translate_per_chunk_resources_invalid_divisibility_cpu():
    res = Resources()
    res.nnodes = 3
    res.ncpus = 10
    res.mem = Size(30, "gb")
    with pytest.raises(QQError, match="must be divisible by 'nnodes'"):
        Slurm._translatePerChunkResources(res)


def test_slurm_translate_per_chunk_resources_invalid_divisibility_gpu():
    res = Resources()
    res.nnodes = 3
    res.ncpus = 12
    res.ngpus = 7
    res.mem = Size(30, "gb")
    with pytest.raises(QQError, match="must be divisible by 'nnodes'"):
        Slurm._translatePerChunkResources(res)


def test_slurm_translate_env_vars_returns_comma_separated_string():
    env = {"VAR1": "value1", "VAR2": "value2"}
    result = Slurm._translateEnvVars(env)
    assert result == 'VAR1="value1",VAR2="value2"'


def test_slurm_translate_env_vars_single_variable():
    env = {"VAR": "123"}
    result = Slurm._translateEnvVars(env)
    assert result == 'VAR="123"'


def test_slurm_translate_env_vars_empty_dict_returns_empty_string():
    result = Slurm._translateEnvVars({})
    assert result == ""


def test_slurm_translate_submit_basic_command():
    res = Resources()
    res.nnodes = 2
    res.ncpus = 8
    res.mem = Size(32, "gb")
    res.ngpus = 4
    res.props = {}
    res.walltime = "2-00:00:00"

    queue = "gpu"
    input_dir = Path("/tmp")
    script = "run.sh"
    job_name = "job1"
    depend = []
    env_vars = {}
    account = None

    command = Slurm._translateSubmit(
        res, queue, input_dir, script, job_name, depend, env_vars, account
    )

    assert command.startswith("sbatch")
    assert f"-J {job_name}" in command
    assert f"-p {queue}" in command
    assert f"-e {input_dir / (job_name + '.qqout')}" in command
    assert f"-o {input_dir / (job_name + '.qqout')}" in command
    assert f"--nodes {res.nnodes}" in command
    assert "--ntasks-per-node=1" in command
    assert f"--cpus-per-task={res.ncpus // res.nnodes}" in command
    assert f"--mem={(res.mem // res.nnodes).toStrExactSlurm()}" in command
    assert f"--gpus-per-node={res.ngpus // res.nnodes}" in command
    assert f"--time={res.walltime}" in command
    assert command.endswith(script)


def test_slurm_translate_submit_basic_command_with_per_node_properties():
    res = Resources()
    res.nnodes = 2
    res.ncpus_per_node = 32
    res.mem_per_node = Size(32, "gb")
    res.ngpus_per_node = 4
    res.props = {}
    res.walltime = "2-00:00:00"

    queue = "gpu"
    input_dir = Path("/tmp")
    script = "run.sh"
    job_name = "job1"
    depend = []
    env_vars = {}
    account = None

    command = Slurm._translateSubmit(
        res, queue, input_dir, script, job_name, depend, env_vars, account
    )

    assert command.startswith("sbatch")
    assert f"-J {job_name}" in command
    assert f"-p {queue}" in command
    assert f"-e {input_dir / (job_name + '.qqout')}" in command
    assert f"-o {input_dir / (job_name + '.qqout')}" in command
    assert f"--nodes {res.nnodes}" in command
    assert "--ntasks-per-node=1" in command
    assert f"--cpus-per-task={res.ncpus_per_node}" in command
    assert f"--mem={res.mem_per_node.toStrExactSlurm()}" in command
    assert f"--gpus-per-node={res.ngpus_per_node}" in command
    assert f"--time={res.walltime}" in command
    assert command.endswith(script)


def test_slurm_translate_submit_with_account_and_env_vars():
    res = Resources()
    res.nnodes = 1
    res.ncpus = 4
    res.mem = Size(16, "gb")
    res.props = {}
    res.walltime = None

    queue = "main"
    input_dir = Path("/work")
    script = "train.sh"
    job_name = "jobX"
    depend = []
    env_vars = {"VAR1": "A", "VAR2": "B"}
    account = "project123"

    command = Slurm._translateSubmit(
        res, queue, input_dir, script, job_name, depend, env_vars, account
    )

    assert "--account project123" in command
    assert '--export ALL,VAR1="A",VAR2="B"' in command
    assert command.endswith(script)


def test_slurm_translate_submit_with_dependencies():
    res = Resources()
    res.nnodes = 1
    res.ncpus = 2
    res.mem = Size(8, "gb")
    res.props = {}
    queue = "short"
    input_dir = Path("/data")
    script = "job.sh"
    job_name = "depjob"
    depend = [Depend(DependType.AFTER_SUCCESS, ["111", "222"])]
    env_vars = {}
    account = None

    command = Slurm._translateSubmit(
        res, queue, input_dir, script, job_name, depend, env_vars, account
    )

    assert "--dependency=afterok:111:222" in command
    assert command.endswith(script)


def test_slurm_translate_submit_with_props_true_only():
    res = Resources()
    res.nnodes = 1
    res.ncpus = 4
    res.mem = Size(8, "gb")
    res.props = {"gpu": "true", "ssd": "true"}
    queue = "long"
    input_dir = Path("/scratch")
    script = "analyze.sh"
    job_name = "job2"
    depend = []
    env_vars = {}
    account = None

    command = Slurm._translateSubmit(
        res, queue, input_dir, script, job_name, depend, env_vars, account
    )

    assert '--constraint="gpu&ssd"' in command
    assert command.endswith(script)


def test_slurm_translate_submit_raises_on_invalid_prop_value():
    res = Resources()
    res.nnodes = 1
    res.ncpus = 2
    res.mem = Size(4, "gb")
    res.props = {"ssd": "false"}
    queue = "gpu"
    input_dir = Path("/tmp")
    script = "fail.sh"
    job_name = "bad"
    depend = []
    env_vars = {}
    account = None

    with pytest.raises(
        QQError, match="Slurm only supports properties with a value of 'true'"
    ):
        Slurm._translateSubmit(
            res, queue, input_dir, script, job_name, depend, env_vars, account
        )


def test_slurm_translate_kill_returns_correct_command():
    job_id = "12345"
    result = Slurm._translateKill(job_id)
    assert result == f"scancel {job_id}"


def test_slurm_translate_kill_force_returns_correct_command():
    job_id = "67890"
    result = Slurm._translateKillForce(job_id)
    assert result == f"scancel --signal=KILL {job_id}"


@patch("qq_lib.batch.slurm.slurm.BatchInterface.isShared", return_value=True)
def test_slurm_is_shared_delegates_to_interface(mock_is_shared):
    directory = Path("/tmp/testdir")
    result = Slurm.isShared(directory)
    mock_is_shared.assert_called_once_with(directory)
    assert result is True


@patch("qq_lib.batch.slurm.slurm.BatchInterface.resubmit")
def test_slurm_resubmit_delegates_to_interface(mock_resubmit):
    Slurm.resubmit(
        input_machine="machine1",
        input_dir="/work/job",
        command_line=["-q gpu", "--account fake-account"],
    )
    mock_resubmit.assert_called_once_with(
        input_machine="machine1",
        input_dir="/work/job",
        command_line=["-q gpu", "--account fake-account"],
    )


@patch("qq_lib.batch.slurm.slurm.PBS.readRemoteFile", return_value="content")
def test_slurm_read_remote_file_delegates(mock_read):
    result = Slurm.readRemoteFile("host1", Path("/tmp/file.txt"))
    mock_read.assert_called_once_with("host1", Path("/tmp/file.txt"))
    assert result == "content"


@patch("qq_lib.batch.slurm.slurm.PBS.writeRemoteFile")
def test_slurm_write_remote_file_delegates(mock_write):
    Slurm.writeRemoteFile("host2", Path("/tmp/file.txt"), "data")
    mock_write.assert_called_once_with("host2", Path("/tmp/file.txt"), "data")


@patch("qq_lib.batch.slurm.slurm.PBS.makeRemoteDir")
def test_slurm_make_remote_dir_delegates(mock_make):
    Slurm.makeRemoteDir("host3", Path("/tmp/dir"))
    mock_make.assert_called_once_with("host3", Path("/tmp/dir"))


@patch(
    "qq_lib.batch.slurm.slurm.PBS.listRemoteDir",
    return_value=[Path("/tmp/a"), Path("/tmp/b")],
)
def test_slurm_list_remote_dir_delegates(mock_list):
    result = Slurm.listRemoteDir("host4", Path("/tmp"))
    mock_list.assert_called_once_with("host4", Path("/tmp"))
    assert result == [Path("/tmp/a"), Path("/tmp/b")]


@patch("qq_lib.batch.slurm.slurm.PBS.moveRemoteFiles")
def test_slurm_move_remote_files_delegates(mock_move):
    Slurm.moveRemoteFiles("host5", [Path("/tmp/a")], [Path("/tmp/b")])
    mock_move.assert_called_once_with("host5", [Path("/tmp/a")], [Path("/tmp/b")])


@patch("qq_lib.batch.slurm.slurm.PBS.syncWithExclusions")
def test_slurm_sync_with_exclusions_delegates(mock_sync):
    Slurm.syncWithExclusions(
        Path("/src"), Path("/dest"), "src_host", "dest_host", [Path("ignore.txt")]
    )
    mock_sync.assert_called_once_with(
        Path("/src"), Path("/dest"), "src_host", "dest_host", [Path("ignore.txt")]
    )


@patch("qq_lib.batch.slurm.slurm.PBS.syncSelected")
def test_slurm_sync_selected_delegates(mock_sync):
    Slurm.syncSelected(
        Path("/src"), Path("/dest"), "src_host", "dest_host", [Path("include.txt")]
    )
    mock_sync.assert_called_once_with(
        Path("/src"), Path("/dest"), "src_host", "dest_host", [Path("include.txt")]
    )


@patch("qq_lib.batch.slurm.slurm.Slurm._getBatchJobsUsingSqueueCommand")
@patch("qq_lib.batch.slurm.slurm.Slurm._getBatchJobsUsingSacctCommand")
def test_slurm_get_unfinished_batch_jobs(mock_sacct, mock_squeue):
    mock_sacct_job = MagicMock()
    mock_sacct_job.getId.return_value = "2"
    mock_squeue_job = MagicMock()
    mock_squeue_job.getId.return_value = "1"
    mock_sacct.return_value = [mock_sacct_job, mock_squeue_job]
    mock_squeue.return_value = [mock_squeue_job]

    result = Slurm.getBatchJobs("user2")

    mock_sacct.assert_called_once()
    mock_squeue.assert_called_once()
    assert len(result) == 2
    assert set(result) == {mock_squeue_job, mock_sacct_job}


@patch("qq_lib.batch.slurm.slurm.Slurm._getBatchJobsUsingSacctCommand")
@patch("qq_lib.batch.slurm.slurm.Slurm._getBatchJobsUsingSqueueCommand")
def test_slurm_get_batch_jobs(mock_squeue, mock_sacct):
    mock_sacct_job = MagicMock()
    mock_sacct_job.getId.return_value = "2"
    mock_squeue_job = MagicMock()
    mock_squeue_job.getId.return_value = "1"
    mock_sacct.return_value = [mock_sacct_job, mock_squeue_job]
    mock_squeue.return_value = [mock_squeue_job]

    result = Slurm.getBatchJobs("user2")

    mock_sacct.assert_called_once()
    mock_squeue.assert_called_once()
    assert len(result) == 2
    assert set(result) == {mock_squeue_job, mock_sacct_job}


@patch("qq_lib.batch.slurm.slurm.Slurm._getBatchJobsUsingSqueueCommand")
@patch("qq_lib.batch.slurm.slurm.Slurm._getBatchJobsUsingSacctCommand")
def test_slurm_get_all_unfinished_batch_jobs(mock_sacct, mock_squeue):
    mock_sacct_job = MagicMock()
    mock_sacct_job.getId.return_value = "5"
    mock_squeue_job = MagicMock()
    mock_squeue_job.getId.return_value = "2"
    mock_sacct.return_value = [mock_sacct_job]
    mock_squeue.return_value = [mock_squeue_job, mock_sacct_job]

    result = Slurm.getAllBatchJobs()

    mock_sacct.assert_called_once()
    mock_squeue.assert_called_once()
    assert len(result) == 2
    assert set(result) == {mock_squeue_job, mock_sacct_job}


@patch("qq_lib.batch.slurm.slurm.Slurm._getBatchJobsUsingSacctCommand")
@patch("qq_lib.batch.slurm.slurm.Slurm._getBatchJobsUsingSqueueCommand")
def test_slurm_get_all_batch_jobs(mock_squeue, mock_sacct):
    mock_sacct_job = MagicMock()
    mock_sacct_job.getId.return_value = "5"
    mock_squeue_job = MagicMock()
    mock_squeue_job.getId.return_value = "2"
    mock_sacct.return_value = [mock_sacct_job]
    mock_squeue.return_value = [mock_squeue_job, mock_sacct_job]

    result = Slurm.getAllBatchJobs()

    mock_sacct.assert_called_once()
    mock_squeue.assert_called_once()
    assert len(result) == 2
    assert set(result) == {mock_squeue_job, mock_sacct_job}


@patch("qq_lib.batch.slurm.slurm.subprocess.run")
@patch("qq_lib.batch.slurm.slurm.Slurm._translateKill", return_value="scancel 123")
def test_slurm_job_kill_runs_successfully(mock_translate, mock_run):
    mock_run.return_value = MagicMock(returncode=0)
    Slurm.jobKill("123")
    mock_translate.assert_called_once_with("123")
    mock_run.assert_called_once()


@patch("qq_lib.batch.slurm.slurm.subprocess.run")
@patch("qq_lib.batch.slurm.slurm.Slurm._translateKill", return_value="scancel 999")
def test_slurm_job_kill_raises_on_error(mock_translate, mock_run):
    mock_run.return_value = MagicMock(returncode=1, stderr="error")
    with pytest.raises(QQError, match="Failed to kill job"):
        Slurm.jobKill("999")
    mock_translate.assert_called_once()
    mock_run.assert_called_once()


@patch("qq_lib.batch.slurm.slurm.subprocess.run")
@patch(
    "qq_lib.batch.slurm.slurm.Slurm._translateKillForce",
    return_value="scancel --signal=KILL 123",
)
def test_slurm_job_kill_force_runs_successfully(mock_translate, mock_run):
    mock_run.return_value = MagicMock(returncode=0)
    Slurm.jobKillForce("123")
    mock_translate.assert_called_once_with("123")
    mock_run.assert_called_once()


@patch("qq_lib.batch.slurm.slurm.subprocess.run")
@patch(
    "qq_lib.batch.slurm.slurm.Slurm._translateKillForce",
    return_value="scancel --signal=KILL 999",
)
def test_slurm_job_kill_force_raises_on_error(mock_translate, mock_run):
    mock_run.return_value = MagicMock(returncode=1, stderr="fail")
    with pytest.raises(QQError, match="Failed to kill job"):
        Slurm.jobKillForce("999")
    mock_translate.assert_called_once()
    mock_run.assert_called_once()


@patch("qq_lib.batch.slurm.slurm.BatchInterface.navigateToDestination")
def test_slurm_navigate_to_destination_delegates(mock_nav):
    Slurm.navigateToDestination("host1", Path("/data"))
    mock_nav.assert_called_once_with("host1", Path("/data"))


@patch("qq_lib.batch.slurm.slurm.SlurmJob")
def test_slurm_get_batch_job_creates_slurmjob(mock_job):
    Slurm.getBatchJob("1234")
    mock_job.assert_called_once_with("1234")


@patch("qq_lib.batch.slurm.slurm.subprocess.run")
@patch("qq_lib.batch.slurm.slurm.Slurm._translateSubmit", return_value="sbatch cmd")
@patch("qq_lib.batch.slurm.slurm.PBS._sharedGuard")
def test_slurm_job_submit_success(mock_guard, mock_translate, mock_run):
    res = Resources()
    script = Path("/tmp/job.sh")
    mock_run.return_value = MagicMock(
        returncode=0, stdout="Submitted batch job 56789\n"
    )

    result = Slurm.jobSubmit(res, "qgpu", script, "job1", [], {}, "acc")

    mock_guard.assert_called_once_with(res, {})
    mock_translate.assert_called_once()
    mock_run.assert_called_once()
    assert result == "56789"


@patch("qq_lib.batch.slurm.slurm.subprocess.run")
@patch("qq_lib.batch.slurm.slurm.Slurm._translateSubmit", return_value="sbatch fail")
@patch("qq_lib.batch.slurm.slurm.PBS._sharedGuard")
def test_slurm_job_submit_raises_on_error(mock_guard, mock_translate, mock_run):
    res = Resources()
    script = Path("/tmp/fail.sh")
    mock_run.return_value = MagicMock(returncode=1, stderr="error text")

    with pytest.raises(QQError, match="Failed to submit script"):
        Slurm.jobSubmit(res, "qgpu", script, "fail_job", [], {}, None)

    mock_guard.assert_called_once_with(res, {})
    mock_translate.assert_called_once()
    mock_run.assert_called_once()


@pytest.mark.parametrize(
    "ids,expected_order",
    [
        (["2", "1", "3"], ["1", "2", "3"]),
        (["10_1", "2", "10_0"], ["2", "10_0", "10_1"]),
        (["1_2_3", "1_1_9", "1_10_0"], ["1_1_9", "1_2_3", "1_10_0"]),
    ],
)
def test_qq_slurm_sort_jobs_sorts_by_ids_for_sorting(ids, expected_order):
    jobs = []
    for job_id in ids:
        job = SlurmJob.__new__(SlurmJob)
        job._job_id = job_id
        jobs.append(job)

    Slurm.sortJobs(jobs)

    result = [job.getId() for job in jobs]
    assert result == expected_order


def test_qq_slurm_sort_jobs_handles_zero_sort_keys():
    job_valid = SlurmJob.__new__(SlurmJob)
    job_valid._job_id = "1"
    job_invalid = SlurmJob.__new__(SlurmJob)
    job_invalid._job_id = "abc"

    jobs = [job_valid, job_invalid]
    Slurm.sortJobs(jobs)

    result = [job.getId() for job in jobs]
    assert result == ["abc", "1"]


@patch("qq_lib.batch.slurm.slurm.SlurmQueue.fromDict")
@patch("qq_lib.batch.slurm.slurm.parse_slurm_dump_to_dictionary")
@patch("qq_lib.batch.slurm.slurm.subprocess.run")
def test_slurm_get_queues(mock_run, mock_parse, mock_fromdict):
    mock_run.return_value = MagicMock(
        returncode=0,
        stdout="PartitionName=default AllowGroups=ALL\nPartitionName=cpu AllowGroups=ALL",
        stderr="",
    )
    mock_parse.side_effect = [
        {"PartitionName": "default", "AllowGroups": "ALL"},
        {"PartitionName": "cpu", "AllowGroups": "ALL"},
    ]
    mock_fromdict.side_effect = [
        MagicMock(
            _name="default", _info={"PartitionName": "default", "AllowGroups": "ALL"}
        ),
        MagicMock(_name="cpu", _info={"PartitionName": "cpu", "AllowGroups": "ALL"}),
    ]
    result = Slurm.getQueues()
    assert len(result) == 2
    assert result[0]._name == "default"
    assert result[1]._name == "cpu"
    mock_run.assert_called_once_with(
        ["bash"],
        input="scontrol show partition -o",
        text=True,
        check=False,
        capture_output=True,
        errors="replace",
    )
    assert mock_parse.call_count == 2
    assert mock_fromdict.call_count == 2


@patch("qq_lib.batch.slurm.slurm.subprocess.run")
def test_slurm_get_queues_scontrol_fails(mock_run):
    mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="some error")
    with pytest.raises(QQError, match="Could not retrieve information about queues"):
        Slurm.getQueues()


@patch("qq_lib.batch.slurm.slurm.subprocess.run")
@patch("qq_lib.batch.slurm.slurm.parse_slurm_dump_to_dictionary")
@patch("qq_lib.batch.slurm.slurm.SlurmNode.fromDict")
def test_slurm_get_nodes_success(mock_from_dict, mock_parser, mock_run):
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "NodeName=node1 Arch=x86_64\nNodeName=node2 Arch=x86_64"
    mock_run.return_value = mock_result

    mock_parser.side_effect = [
        {"NodeName": "node1", "Arch": "x86_64"},
        {"NodeName": "node2", "Arch": "x86_64"},
    ]

    mock_node1 = MagicMock(spec=SlurmNode)
    mock_node2 = MagicMock(spec=SlurmNode)
    mock_from_dict.side_effect = [mock_node1, mock_node2]

    result = Slurm.getNodes()

    mock_run.assert_called_once_with(
        ["bash"],
        input="scontrol show node -o",
        text=True,
        check=False,
        capture_output=True,
        errors="replace",
    )

    assert mock_parser.call_count == 2
    mock_from_dict.assert_any_call("node1", {"NodeName": "node1", "Arch": "x86_64"})
    mock_from_dict.assert_any_call("node2", {"NodeName": "node2", "Arch": "x86_64"})

    assert result == [mock_node1, mock_node2]


@patch("qq_lib.batch.slurm.slurm.subprocess.run")
def test_slurm_get_nodes_failure_raises_qqerror(mock_run):
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stderr = "some error"
    mock_run.return_value = mock_result

    with pytest.raises(
        QQError, match="Could not retrieve information about nodes: some error."
    ):
        Slurm.getNodes()


@patch("qq_lib.batch.slurm.slurm.PBS.deleteRemoteDir")
def test_slurm_delete_remote_dir_delegates(mock_make):
    Slurm.deleteRemoteDir("host3", Path("/tmp/dir"))
    mock_make.assert_called_once_with("host3", Path("/tmp/dir"))
