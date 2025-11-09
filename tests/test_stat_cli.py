# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from unittest.mock import patch

import pytest
from click.testing import CliRunner

from qq_lib.batch.interface import BatchMeta
from qq_lib.batch.pbs import PBS, PBSJob
from qq_lib.batch.pbs.common import parse_multi_pbs_dump_to_dictionaries
from qq_lib.jobs.presenter import QQJobsPresenter
from qq_lib.stat import stat


@pytest.fixture
def sample_pbs_dump():
    return """
Job Id: 123456.fake-cluster.example.com
    Job_Name = example_job_1
    Job_Owner = user1@EXAMPLE
    resources_used.cpupercent = 75
    resources_used.cput = 01:23:45
    resources_used.mem = 51200kb
    resources_used.ncpus = 4
    resources_used.vmem = 51200kb
    resources_used.walltime = 01:00:00
    job_state = R
    queue = gpu
    server = fake-cluster.example.com
    ctime = Sun Sep 21 00:00:00 2025
    mtime = Sun Sep 21 01:00:00 2025
    Resource_List.ncpus = 4
    Resource_List.ngpus = 1
    Resource_List.nodect = 1
    Resource_List.walltime = 02:00:00
    exec_host = nodeA/4*4
    exec_vnode = (nodeA:ncpus=4:ngpus=1:mem=4096mb)
    Output_Path = /fake/path/job_123456.log
    stime = Sun Sep 21 00:00:00 2025
    jobdir = /fake/home/user1

Job Id: 654321.fake-cluster.example.com
    Job_Name = example_job_2
    Job_Owner = user2@EXAMPLE
    resources_used.cpupercent = 150
    resources_used.cput = 02:34:56
    resources_used.mem = 102400kb
    resources_used.ncpus = 8
    resources_used.vmem = 102400kb
    resources_used.walltime = 02:00:00
    job_state = Q
    queue = batch
    server = fake-cluster.example.com
    ctime = Sun Sep 21 00:00:00 2025
    mtime = Sun Sep 21 01:00:00 2025
    Resource_List.ncpus = 8
    Resource_List.ngpus = 0
    Resource_List.nodect = 2
    Resource_List.walltime = 04:00:00
    exec_host = nodeB/8*8
    exec_vnode = (nodeB:ncpus=8:mem=8192mb)
    Output_Path = /fake/path/job_654321.log
    jobdir = /fake/home/user2
""".strip()


@pytest.fixture
def parsed_jobs(sample_pbs_dump):
    jobs = []
    for data, job_id in parse_multi_pbs_dump_to_dictionaries(sample_pbs_dump, "Job Id"):
        jobs.append(PBSJob.fromDict(job_id, data))
    return jobs


def test_stat_command_unfinished_shows_jobs(parsed_jobs):
    runner = CliRunner()

    with (
        patch.object(BatchMeta, "fromEnvVarOrGuess", return_value=PBS),
        patch.object(PBS, "getAllUnfinishedBatchJobs", return_value=parsed_jobs),
        patch.object(
            PBS,
            "getAllBatchJobs",
            side_effect=Exception("getAllBatchJobs should not be called"),
        ),
        patch.object(PBS, "sortJobs") as mock_sort,
    ):
        result = runner.invoke(stat, [], catch_exceptions=False)

        assert result.exit_code == 0
        mock_sort.assert_called_once()
        output = result.output

        for job in parsed_jobs:
            assert QQJobsPresenter._shortenJobId(job.getId()) in output
            assert job.getName() in output
            assert job.getUser() in output


def test_stat_command_all_flag_shows_all_jobs(parsed_jobs):
    runner = CliRunner()

    with (
        patch.object(BatchMeta, "fromEnvVarOrGuess", return_value=PBS),
        patch.object(PBS, "getAllBatchJobs", return_value=parsed_jobs),
        patch.object(
            PBS,
            "getAllUnfinishedBatchJobs",
            side_effect=Exception("getAllUnfinishedBatchJobs should not be called"),
        ),
        patch.object(PBS, "sortJobs") as mock_sort,
    ):
        result = runner.invoke(stat, ["--all"], catch_exceptions=False)

        assert result.exit_code == 0
        mock_sort.assert_called_once()
        output = result.output

        for job in parsed_jobs:
            assert QQJobsPresenter._shortenJobId(job.getId()) in output
            assert job.getName() in output
            assert job.getUser() in output


def test_stat_command_yaml_flag_outputs_yaml(parsed_jobs):
    runner = CliRunner()

    with (
        patch.object(BatchMeta, "fromEnvVarOrGuess", return_value=PBS),
        patch.object(PBS, "getAllUnfinishedBatchJobs", return_value=parsed_jobs),
        patch.object(
            PBS,
            "getAllBatchJobs",
            side_effect=Exception("getAllBatchJobs should not be called"),
        ),
        patch.object(PBS, "sortJobs") as mock_sort,
    ):
        result = runner.invoke(stat, ["--yaml"], catch_exceptions=False)

        assert result.exit_code == 0
        mock_sort.assert_called_once()
        output = result.output

        for job in parsed_jobs:
            yaml_repr = job.toYaml()
            assert yaml_repr.strip() in output


def test_stat_command_no_jobs():
    runner = CliRunner()

    with (
        patch.object(BatchMeta, "fromEnvVarOrGuess", return_value=PBS),
        patch.object(PBS, "getAllUnfinishedBatchJobs", return_value=[]),
        patch.object(
            PBS,
            "getAllBatchJobs",
            side_effect=Exception("getAllBatchJobs should not be called"),
        ),
        patch.object(PBS, "sortJobs") as mock_sort,
    ):
        result = runner.invoke(stat, [], catch_exceptions=False)

        assert result.exit_code == 0
        assert "No jobs found." in result.output
        mock_sort.assert_not_called()
