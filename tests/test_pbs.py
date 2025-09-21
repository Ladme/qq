# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

# ruff: noqa: W291

from qq_lib.pbs import _parse_pbs_dump_to_dictionary


def test_parse_pbs_dump_empty_string():
    text = ""
    result = _parse_pbs_dump_to_dictionary(text)
    assert result == {}


def test_parse_pbs_dump_real_file():
    text = """
Job Id: 123456.fake-cluster.example.com
    Job_Name = example_job
    Job_Owner = user@EXAMPLE
    resources_used.cpupercent = 100
    resources_used.cput = 01:23:45
    resources_used.diag_messages = '{}'
    resources_used.mem = 102400kb
    resources_used.ncpus = 8
    resources_used.vmem = 102400kb
    resources_used.walltime = 02:00:00
    job_state = R
    queue = gpu
    server = fake-cluster.example.com
    ctime = Sun Sep 21 00:00:00 2025
    depend = afterany:123455.fake-cluster.example.com@fake-cluster.example.com
    Error_Path = /fake/path/job_123456.log
    exec_host = node1/8*8
    exec_host2 = node1.example.com:15002/8*8
    exec_vnode = (node1:ncpus=8:ngpus=1:mem=8192mb:scratch_local=8192mb)
    group_list = examplegroup
    Hold_Types = n
    Join_Path = oe
    Mail_Points = n
    mtime = Sun Sep 21 02:00:00 2025
    Output_Path = /fake/path/job_123456.log
    qtime = Sun Sep 21 00:00:00 2025
    Rerunable = False
    Resource_List.mem = 8gb
    Resource_List.mpiprocs = 8
    Resource_List.ncpus = 8
    Resource_List.ngpus = 1
    Resource_List.nodect = 1
    Resource_List.place = free
    Resource_List.scratch_local = 8gb
    Resource_List.select = 1:ncpus=8:ngpus=1:mpiprocs=8:mem=8gb:scratch_local=8
        gb:cl_two=true:ompthreads=1:node_owner=everybody
    Resource_List.walltime = 24:00:00
    stime = Sun Sep 21 00:00:00 2025
    session_id = 123456
    jobdir = /fake/home/user
    substate = 42
    Variable_List = QQ_DEBUG=true,QQ_ENV_SET=true,
        AMS_SITE_SUPPORT=linuxsupport@example.com,PBS_O_LOGNAME=user,
        PBS_O_QUEUE=gpu,PBS_O_HOST=host.example.com,
        SCRATCHDIR=/scratch/user/job_123456,
        SCRATCH=/scratch/user/job_123456,
        SINGULARITY_TMPDIR=/scratch/user/job_123456,
        SINGULARITY_CACHEDIR=/scratch/user/job_123456
    etime = Sun Sep 21 00:00:00 2025
    umask = 77
    run_count = 1
    eligible_time = 00:00:00
    project = _pbs_project_default
    Submit_Host = host.example.com
    credential_id = user@EXAMPLE
    credential_validity = Mon Sep 22 06:38:19 2025
"""

    result = _parse_pbs_dump_to_dictionary(text)

    assert isinstance(result, dict)
    assert result["Job_Name"].strip() == "example_job"
    assert result["job_state"].strip() == "R"
    assert (
        result["Resource_List.select"].strip()
        == "1:ncpus=8:ngpus=1:mpiprocs=8:mem=8gb:scratch_local=8gb:cl_two=true:ompthreads=1:node_owner=everybody"
    )
    assert result["jobdir"].strip() == "/fake/home/user"
    assert result["resources_used.cpupercent"].strip() == "100"
    assert "QQ_DEBUG=true,QQ_ENV_SET=true," in result["Variable_List"]
    assert "SINGULARITY_CACHEDIR=/scratch/user/job_123456" in result["Variable_List"]


def test_parse_pbs_dump_nonsense_input():
    text = """
This is not a key-value
Just some random text
= = =
Another line without equal
KEY = 
=VALUE
NORMAL = OK
CONTINUATION
"""
    result = _parse_pbs_dump_to_dictionary(text)

    assert result.get("NORMAL") == "OKCONTINUATION"

    assert "This is not a key-value" not in result
    assert "KEY" not in result
