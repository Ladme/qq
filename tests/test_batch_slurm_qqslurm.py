# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.batch.slurm.qqslurm import QQSlurm
from qq_lib.core.error import QQError
from qq_lib.properties.depend import Depend, DependType
from qq_lib.properties.resources import QQResources
from qq_lib.properties.size import Size


def test_qqslurm_env_name_returns_slurm():
    assert QQSlurm.envName() == "Slurm"


@patch(
    "qq_lib.batch.slurm.qqslurm.shutil.which",
    side_effect=lambda x: "/usr/bin/sbatch" if x == "sbatch" else None,
)
def test_qqslurm_is_available_returns_true_when_sbatch_present(
    mock_which,
):
    result = QQSlurm.isAvailable()
    assert result is True
    mock_which.assert_any_call("sbatch")


@patch(
    "qq_lib.batch.slurm.qqslurm.shutil.which",
    side_effect=lambda _: None,
)
def test_qqslurm_is_available_returns_false_when_sbatch_missing(mock_which):
    result = QQSlurm.isAvailable()
    assert result is False
    mock_which.assert_any_call("sbatch")


@patch(
    "qq_lib.batch.slurm.qqslurm.shutil.which",
    side_effect=lambda x: "/usr/bin/sbatch" if x == "sbatch" else "/usr/bin/it4ifree",
)
def test_qqslurm_is_available_returns_false_when_it4ifree_present(mock_which):
    result = QQSlurm.isAvailable()
    assert result is False
    mock_which.assert_any_call("sbatch")
    mock_which.assert_any_call("it4ifree")


@patch.dict("qq_lib.batch.slurm.qqslurm.os.environ", {"SLURM_JOB_ID": "12345"})
def test_qqslurm_get_job_id_returns_value_from_env():
    assert QQSlurm.getJobId() == "12345"


@patch.dict("qq_lib.batch.slurm.qqslurm.os.environ", {}, clear=True)
def test_qqslurm_get_job_id_returns_none_when_missing():
    assert QQSlurm.getJobId() is None


@patch("qq_lib.batch.slurm.qqslurm.subprocess.run")
@patch("qq_lib.batch.slurm.qqslurm.SlurmJob")
def test_qqslurm_get_batch_jobs_calls_slurmjob(mock_job, mock_run):
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "111\n222\n333\n"
    mock_run.return_value = mock_result

    jobs = QQSlurm._getBatchJobsUsingSqueueCommand("squeue -u user")

    mock_run.assert_called_once()
    assert len(jobs) == 3
    mock_job.assert_any_call("111")
    mock_job.assert_any_call("222")
    mock_job.assert_any_call("333")


@patch("qq_lib.batch.slurm.qqslurm.subprocess.run")
def test_qqslurm_get_batch_jobs_raises_on_error(mock_run):
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stderr = "error"
    mock_run.return_value = mock_result

    with pytest.raises(QQError):
        QQSlurm._getBatchJobsUsingSqueueCommand("squeue -u user")

    mock_run.assert_called_once()


@patch("qq_lib.batch.slurm.qqslurm.subprocess.run")
@patch("qq_lib.batch.slurm.qqslurm.SlurmJob")
def test_qqslurm_get_batch_jobs_skips_empty_lines(mock_job, mock_run):
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "111\n\n222\n"
    mock_run.return_value = mock_result

    jobs = QQSlurm._getBatchJobsUsingSqueueCommand("squeue -u user")

    assert len(jobs) == 2
    mock_job.assert_any_call("111")
    mock_job.assert_any_call("222")
    mock_run.assert_called_once()


@patch("qq_lib.batch.slurm.qqslurm.subprocess.run")
@patch("qq_lib.batch.slurm.qqslurm.SlurmJob.fromSacctString")
def test_qqslurm_get_batch_jobs_sacct_calls_fromsacctstring(mock_from_sacct, mock_run):
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "job1|info\njob2|info\n"
    mock_run.return_value = mock_result

    jobs = QQSlurm._getBatchJobsUsingSacctCommand("sacct -u user")

    mock_run.assert_called_once()
    assert len(jobs) == 2
    mock_from_sacct.assert_any_call("job1|info")
    mock_from_sacct.assert_any_call("job2|info")


@patch("qq_lib.batch.slurm.qqslurm.subprocess.run")
def test_qqslurm_get_batch_jobs_sacct_raises_on_error(mock_run):
    mock_result = MagicMock()
    mock_result.returncode = 1
    mock_result.stderr = "error"
    mock_run.return_value = mock_result

    with pytest.raises(QQError):
        QQSlurm._getBatchJobsUsingSacctCommand("sacct -u user")

    mock_run.assert_called_once()


@patch("qq_lib.batch.slurm.qqslurm.subprocess.run")
@patch("qq_lib.batch.slurm.qqslurm.SlurmJob.fromSacctString")
def test_qqslurm_get_batch_jobs_sacct_skips_empty_lines(mock_from_sacct, mock_run):
    mock_result = MagicMock()
    mock_result.returncode = 0
    mock_result.stdout = "job1|info\n\njob2|info\n"
    mock_run.return_value = mock_result

    jobs = QQSlurm._getBatchJobsUsingSacctCommand("sacct -u user")

    assert len(jobs) == 2
    mock_from_sacct.assert_any_call("job1|info")
    mock_from_sacct.assert_any_call("job2|info")
    mock_run.assert_called_once()


@patch("qq_lib.batch.slurm.qqslurm.QQResources.mergeResources")
@patch("qq_lib.batch.slurm.qqslurm.QQSlurm._getDefaultResources")
@patch("qq_lib.batch.slurm.qqslurm.default_resources_from_dict")
@patch("qq_lib.batch.slurm.qqslurm.parse_slurm_dump_to_dictionary")
@patch("qq_lib.batch.slurm.qqslurm.subprocess.run")
def test_qqslurm_get_default_server_resources_merges_parsed_and_defaults(
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

    result = QQSlurm._getDefaultServerResources()

    mock_run.assert_called_once()
    mock_parse.assert_called_once_with("DefaultTime=2-00:00:00\nDefMemPerCPU=4G", "\n")
    mock_from_dict.assert_called_once_with(
        {"DefaultTime": "2-00:00:00", "DefMemPerCPU": "4G"}
    )
    mock_get_defaults.assert_called_once()
    mock_merge.assert_called_once_with(server_res, default_res)
    assert result is merged_res


@patch("qq_lib.batch.slurm.qqslurm.QQResources.mergeResources")
@patch("qq_lib.batch.slurm.qqslurm.QQSlurm._getDefaultResources")
@patch("qq_lib.batch.slurm.qqslurm.default_resources_from_dict")
@patch("qq_lib.batch.slurm.qqslurm.parse_slurm_dump_to_dictionary")
@patch("qq_lib.batch.slurm.qqslurm.subprocess.run")
def test_qqslurm_get_default_server_resources_returns_empty_on_failure(
    mock_run, mock_parse, mock_from_dict, mock_get_defaults, mock_merge
):
    mock_run.return_value = MagicMock(returncode=1, stderr="err")

    result = QQSlurm._getDefaultServerResources()

    mock_run.assert_called_once()
    mock_parse.assert_not_called()
    mock_from_dict.assert_not_called()
    mock_get_defaults.assert_not_called()
    mock_merge.assert_not_called()
    assert isinstance(result, QQResources)
    assert result == QQResources()


def test_qqslurm_translate_dependencies_returns_none_for_empty_list():
    assert QQSlurm._translateDependencies([]) is None


def test_qqslurm_translate_dependencies_returns_single_dependency_string():
    depend = Depend(DependType.AFTER_START, ["123"])
    result = QQSlurm._translateDependencies([depend])
    assert result == "after:123"


def test_qqslurm_translate_dependencies_returns_multiple_dependency_string():
    depend1 = Depend(DependType.AFTER_SUCCESS, ["111", "222"])
    depend2 = Depend(DependType.AFTER_FAILURE, ["333"])
    result = QQSlurm._translateDependencies([depend1, depend2])
    assert result == "afterok:111:222,afternotok:333"


def test_qqslurm_translate_per_chunk_resources_two_nodes():
    res = QQResources()
    res.nnodes = 2
    res.ncpus = 8
    res.mem = Size(32, "gb")
    res.ngpus = 4
    result = QQSlurm._translatePerChunkResources(res)
    assert "--mincpus=4" in result
    assert f"--mem={(res.mem // res.nnodes).toStrExactSlurm()}" in result
    assert "--gpus-per-node=2" in result


def test_qqslurm_translate_per_chunk_resources_single_node():
    res = QQResources()
    res.nnodes = 1
    res.ncpus = 4
    res.mem = Size(16, "gb")
    res.ngpus = 2
    result = QQSlurm._translatePerChunkResources(res)
    assert "--mincpus=4" in result
    assert f"--mem={res.mem.toStrExactSlurm()}" in result
    assert "--gpus-per-node=2" in result


def test_qqslurm_translate_per_chunk_resources_multiple_nodes():
    res = QQResources()
    res.nnodes = 5
    res.ncpus = 10
    res.mem = Size(50, "gb")
    res.ngpus = 5
    result = QQSlurm._translatePerChunkResources(res)
    assert "--mincpus=2" in result
    assert f"--mem={(res.mem // res.nnodes).toStrExactSlurm()}" in result
    assert "--gpus-per-node=1" in result


def test_qqslurm_translate_per_chunk_resources_uses_mem_per_cpu():
    res = QQResources()
    res.nnodes = 2
    res.ncpus = 8
    res.mem = None
    res.mem_per_cpu = Size(4, "gb")
    res.ngpus = 0
    result = QQSlurm._translatePerChunkResources(res)
    assert f"--mem-per-cpu={res.mem_per_cpu.toStrExactSlurm()}" in result


def test_qqslurm_translate_per_chunk_resources_raises_when_mem_missing():
    res = QQResources()
    res.nnodes = 1
    res.mem = None
    res.mem_per_cpu = None
    with pytest.raises(
        QQError, match="Attribute 'mem' and attribute 'mem-per-cpu' are not defined."
    ):
        QQSlurm._translatePerChunkResources(res)


@pytest.mark.parametrize("nnodes", [None, 0])
def test_qqslurm_translate_per_chunk_resources_invalid_nnodes(nnodes):
    res = QQResources()
    res.nnodes = nnodes
    res.mem = Size(16, "gb")
    with pytest.raises(QQError, match="Attribute 'nnodes'"):
        QQSlurm._translatePerChunkResources(res)


def test_qqslurm_translate_per_chunk_resources_invalid_divisibility_cpu():
    res = QQResources()
    res.nnodes = 3
    res.ncpus = 10
    res.mem = Size(30, "gb")
    with pytest.raises(QQError, match="must be divisible by 'nnodes'"):
        QQSlurm._translatePerChunkResources(res)


def test_qqslurm_translate_per_chunk_resources_invalid_divisibility_gpu():
    res = QQResources()
    res.nnodes = 3
    res.ncpus = 12
    res.ngpus = 7
    res.mem = Size(30, "gb")
    with pytest.raises(QQError, match="must be divisible by 'nnodes'"):
        QQSlurm._translatePerChunkResources(res)


def test_qqslurm_translate_env_vars_returns_comma_separated_string():
    env = {"VAR1": "value1", "VAR2": "value2"}
    result = QQSlurm._translateEnvVars(env)
    assert result == 'VAR1="value1",VAR2="value2"'


def test_qqslurm_translate_env_vars_single_variable():
    env = {"VAR": "123"}
    result = QQSlurm._translateEnvVars(env)
    assert result == 'VAR="123"'


def test_qqslurm_translate_env_vars_empty_dict_returns_empty_string():
    result = QQSlurm._translateEnvVars({})
    assert result == ""


def test_qqslurm_translate_submit_basic_command():
    res = QQResources()
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

    command = QQSlurm._translateSubmit(
        res, queue, input_dir, script, job_name, depend, env_vars, account
    )

    assert command.startswith("sbatch")
    assert f"-J {job_name}" in command
    assert f"-p {queue}" in command
    assert f"-e {input_dir / (job_name + '.qqout')}" in command
    assert f"-o {input_dir / (job_name + '.qqout')}" in command
    assert f"--mincpus={res.ncpus // res.nnodes}" in command
    assert f"--mem={(res.mem // res.nnodes).toStrExactSlurm()}" in command
    assert f"--gpus-per-node={res.ngpus // res.nnodes}" in command
    assert f"--time={res.walltime}" in command
    assert command.endswith(script)


def test_qqslurm_translate_submit_with_account_and_env_vars():
    res = QQResources()
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

    command = QQSlurm._translateSubmit(
        res, queue, input_dir, script, job_name, depend, env_vars, account
    )

    assert "--account project123" in command
    assert '--export ALL,VAR1="A",VAR2="B"' in command
    assert command.endswith(script)


def test_qqslurm_translate_submit_with_dependencies():
    res = QQResources()
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

    command = QQSlurm._translateSubmit(
        res, queue, input_dir, script, job_name, depend, env_vars, account
    )

    assert "--dependency=afterok:111:222" in command
    assert command.endswith(script)


def test_qqslurm_translate_submit_with_props_true_only():
    res = QQResources()
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

    command = QQSlurm._translateSubmit(
        res, queue, input_dir, script, job_name, depend, env_vars, account
    )

    assert '--constraint="gpu&ssd"' in command
    assert command.endswith(script)


def test_qqslurm_translate_submit_raises_on_invalid_prop_value():
    res = QQResources()
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
        QQSlurm._translateSubmit(
            res, queue, input_dir, script, job_name, depend, env_vars, account
        )


def test_qqslurm_translate_kill_returns_correct_command():
    job_id = "12345"
    result = QQSlurm._translateKill(job_id)
    assert result == f"scancel {job_id}"


def test_qqslurm_translate_kill_force_returns_correct_command():
    job_id = "67890"
    result = QQSlurm._translateKillForce(job_id)
    assert result == f"scancel --signal=KILL {job_id}"


@patch("qq_lib.batch.slurm.qqslurm.QQBatchInterface.isShared", return_value=True)
def test_qqslurm_is_shared_delegates_to_interface(mock_is_shared):
    directory = Path("/tmp/testdir")
    result = QQSlurm.isShared(directory)
    mock_is_shared.assert_called_once_with(directory)
    assert result is True


@patch("qq_lib.batch.slurm.qqslurm.QQBatchInterface.resubmit")
def test_qqslurm_resubmit_delegates_to_interface(mock_resubmit):
    QQSlurm.resubmit("machine1", "/work/job", ["-q gpu", "--account fake-account"])
    mock_resubmit.assert_called_once_with(
        input_machine="machine1",
        input_dir="/work/job",
        command_line=["-q gpu", "--account fake-account"],
    )


@patch("qq_lib.batch.slurm.qqslurm.QQPBS.readRemoteFile", return_value="content")
def test_qqslurm_read_remote_file_delegates(mock_read):
    result = QQSlurm.readRemoteFile("host1", Path("/tmp/file.txt"))
    mock_read.assert_called_once_with("host1", Path("/tmp/file.txt"))
    assert result == "content"


@patch("qq_lib.batch.slurm.qqslurm.QQPBS.writeRemoteFile")
def test_qqslurm_write_remote_file_delegates(mock_write):
    QQSlurm.writeRemoteFile("host2", Path("/tmp/file.txt"), "data")
    mock_write.assert_called_once_with("host2", Path("/tmp/file.txt"), "data")


@patch("qq_lib.batch.slurm.qqslurm.QQPBS.makeRemoteDir")
def test_qqslurm_make_remote_dir_delegates(mock_make):
    QQSlurm.makeRemoteDir("host3", Path("/tmp/dir"))
    mock_make.assert_called_once_with("host3", Path("/tmp/dir"))


@patch(
    "qq_lib.batch.slurm.qqslurm.QQPBS.listRemoteDir",
    return_value=[Path("/tmp/a"), Path("/tmp/b")],
)
def test_qqslurm_list_remote_dir_delegates(mock_list):
    result = QQSlurm.listRemoteDir("host4", Path("/tmp"))
    mock_list.assert_called_once_with("host4", Path("/tmp"))
    assert result == [Path("/tmp/a"), Path("/tmp/b")]


@patch("qq_lib.batch.slurm.qqslurm.QQPBS.moveRemoteFiles")
def test_qqslurm_move_remote_files_delegates(mock_move):
    QQSlurm.moveRemoteFiles("host5", [Path("/tmp/a")], [Path("/tmp/b")])
    mock_move.assert_called_once_with("host5", [Path("/tmp/a")], [Path("/tmp/b")])


@patch("qq_lib.batch.slurm.qqslurm.QQPBS.syncWithExclusions")
def test_qqslurm_sync_with_exclusions_delegates(mock_sync):
    QQSlurm.syncWithExclusions(
        Path("/src"), Path("/dest"), "src_host", "dest_host", [Path("ignore.txt")]
    )
    mock_sync.assert_called_once_with(
        Path("/src"), Path("/dest"), "src_host", "dest_host", [Path("ignore.txt")]
    )


@patch("qq_lib.batch.slurm.qqslurm.QQPBS.syncSelected")
def test_qqslurm_sync_selected_delegates(mock_sync):
    QQSlurm.syncSelected(
        Path("/src"), Path("/dest"), "src_host", "dest_host", [Path("include.txt")]
    )
    mock_sync.assert_called_once_with(
        Path("/src"), Path("/dest"), "src_host", "dest_host", [Path("include.txt")]
    )


@patch("qq_lib.batch.slurm.qqslurm.QQSlurm._getBatchJobsUsingSqueueCommand")
def test_qqslurm_get_unfinished_batch_jobs_returns_sorted(mock_squeue):
    mock_job1 = MagicMock()
    mock_job2 = MagicMock()
    mock_job1.getId.return_value = "2"
    mock_job2.getId.return_value = "1"
    mock_squeue.return_value = [mock_job1, mock_job2]

    result = QQSlurm.getUnfinishedBatchJobs("user1")

    mock_squeue.assert_called_once_with('squeue -u user1 -t PENDING,RUNNING -h -o "%i"')
    assert result == [mock_job2, mock_job1]


@patch("qq_lib.batch.slurm.qqslurm.QQSlurm._getBatchJobsUsingSacctCommand")
@patch("qq_lib.batch.slurm.qqslurm.QQSlurm._getBatchJobsUsingSqueueCommand")
def test_qqslurm_get_batch_jobs_combines_and_sorts(mock_squeue, mock_sacct):
    mock_sacct_job = MagicMock()
    mock_sacct_job.getId.return_value = "2"
    mock_squeue_job = MagicMock()
    mock_squeue_job.getId.return_value = "1"
    mock_sacct.return_value = [mock_sacct_job]
    mock_squeue.return_value = [mock_squeue_job]

    result = QQSlurm.getBatchJobs("user2")

    mock_sacct.assert_called_once()
    mock_squeue.assert_called_once()
    assert result == [mock_squeue_job, mock_sacct_job]


@patch("qq_lib.batch.slurm.qqslurm.QQSlurm._getBatchJobsUsingSqueueCommand")
def test_qqslurm_get_all_unfinished_batch_jobs_returns_sorted(mock_squeue):
    mock_job1 = MagicMock()
    mock_job2 = MagicMock()
    mock_job1.getId.return_value = "3"
    mock_job2.getId.return_value = "1"
    mock_squeue.return_value = [mock_job1, mock_job2]

    result = QQSlurm.getAllUnfinishedBatchJobs()

    mock_squeue.assert_called_once_with('squeue -t PENDING,RUNNING -h -o "%i"')
    assert result == [mock_job2, mock_job1]


@patch("qq_lib.batch.slurm.qqslurm.QQSlurm._getBatchJobsUsingSacctCommand")
@patch("qq_lib.batch.slurm.qqslurm.QQSlurm._getBatchJobsUsingSqueueCommand")
def test_qqslurm_get_all_batch_jobs_combines_and_sorts(mock_squeue, mock_sacct):
    mock_sacct_job = MagicMock()
    mock_sacct_job.getId.return_value = "5"
    mock_squeue_job = MagicMock()
    mock_squeue_job.getId.return_value = "2"
    mock_sacct.return_value = [mock_sacct_job]
    mock_squeue.return_value = [mock_squeue_job]

    result = QQSlurm.getAllBatchJobs()

    mock_sacct.assert_called_once()
    mock_squeue.assert_called_once()
    assert result == [mock_squeue_job, mock_sacct_job]


@patch("qq_lib.batch.slurm.qqslurm.subprocess.run")
@patch("qq_lib.batch.slurm.qqslurm.QQSlurm._translateKill", return_value="scancel 123")
def test_qqslurm_job_kill_runs_successfully(mock_translate, mock_run):
    mock_run.return_value = MagicMock(returncode=0)
    QQSlurm.jobKill("123")
    mock_translate.assert_called_once_with("123")
    mock_run.assert_called_once()


@patch("qq_lib.batch.slurm.qqslurm.subprocess.run")
@patch("qq_lib.batch.slurm.qqslurm.QQSlurm._translateKill", return_value="scancel 999")
def test_qqslurm_job_kill_raises_on_error(mock_translate, mock_run):
    mock_run.return_value = MagicMock(returncode=1, stderr="error")
    with pytest.raises(QQError, match="Failed to kill job"):
        QQSlurm.jobKill("999")
    mock_translate.assert_called_once()
    mock_run.assert_called_once()


@patch("qq_lib.batch.slurm.qqslurm.subprocess.run")
@patch(
    "qq_lib.batch.slurm.qqslurm.QQSlurm._translateKillForce",
    return_value="scancel --signal=KILL 123",
)
def test_qqslurm_job_kill_force_runs_successfully(mock_translate, mock_run):
    mock_run.return_value = MagicMock(returncode=0)
    QQSlurm.jobKillForce("123")
    mock_translate.assert_called_once_with("123")
    mock_run.assert_called_once()


@patch("qq_lib.batch.slurm.qqslurm.subprocess.run")
@patch(
    "qq_lib.batch.slurm.qqslurm.QQSlurm._translateKillForce",
    return_value="scancel --signal=KILL 999",
)
def test_qqslurm_job_kill_force_raises_on_error(mock_translate, mock_run):
    mock_run.return_value = MagicMock(returncode=1, stderr="fail")
    with pytest.raises(QQError, match="Failed to kill job"):
        QQSlurm.jobKillForce("999")
    mock_translate.assert_called_once()
    mock_run.assert_called_once()


@patch("qq_lib.batch.slurm.qqslurm.QQBatchInterface.navigateToDestination")
def test_qqslurm_navigate_to_destination_delegates(mock_nav):
    QQSlurm.navigateToDestination("host1", Path("/data"))
    mock_nav.assert_called_once_with("host1", Path("/data"))


@patch("qq_lib.batch.slurm.qqslurm.SlurmJob")
def test_qqslurm_get_batch_job_creates_slurmjob(mock_job):
    QQSlurm.getBatchJob("1234")
    mock_job.assert_called_once_with("1234")


@patch("qq_lib.batch.slurm.qqslurm.subprocess.run")
@patch("qq_lib.batch.slurm.qqslurm.QQSlurm._translateSubmit", return_value="sbatch cmd")
@patch("qq_lib.batch.slurm.qqslurm.QQPBS._sharedGuard")
def test_qqslurm_job_submit_success(mock_guard, mock_translate, mock_run):
    res = QQResources()
    script = Path("/tmp/job.sh")
    mock_run.return_value = MagicMock(
        returncode=0, stdout="Submitted batch job 56789\n"
    )

    result = QQSlurm.jobSubmit(res, "qgpu", script, "job1", [], {}, "acc")

    mock_guard.assert_called_once_with(res, {})
    mock_translate.assert_called_once()
    mock_run.assert_called_once()
    assert result == "56789"


@patch("qq_lib.batch.slurm.qqslurm.subprocess.run")
@patch(
    "qq_lib.batch.slurm.qqslurm.QQSlurm._translateSubmit", return_value="sbatch fail"
)
@patch("qq_lib.batch.slurm.qqslurm.QQPBS._sharedGuard")
def test_qqslurm_job_submit_raises_on_error(mock_guard, mock_translate, mock_run):
    res = QQResources()
    script = Path("/tmp/fail.sh")
    mock_run.return_value = MagicMock(returncode=1, stderr="error text")

    with pytest.raises(QQError, match="Failed to submit script"):
        QQSlurm.jobSubmit(res, "qgpu", script, "fail_job", [], {}, None)

    mock_guard.assert_called_once_with(res, {})
    mock_translate.assert_called_once()
    mock_run.assert_called_once()
