# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import pytest

from qq_lib.batch.pbs.common import (
    parseMultiPBSDumpToDictionaries,
    parsePBSDumpToDictionary,
)
from qq_lib.core.error import QQError


def test_parse_pbs_dump_to_dictionary_queue():
    pbs_dump = """Queue: gpu
    queue_type = Execution
    Priority = 75
    total_jobs = 367
    state_count = Transit:0 Queued:235 Held:0 Waiting:0 Running:132 Exiting:0 Begun:0
    max_queued = [u:PBS_GENERIC=2000]
    resources_max.ngpus = 99
    resources_max.walltime = 24:00:00
    resources_min.mem = 50mb
    resources_min.ngpus = 1
    resources_default.ngpus = 1
    comment = Queue for jobs computed on GPU
    default_chunk.queue_list = q_gpu
    resources_assigned.mem = 1056gb
    resources_assigned.mpiprocs = 1056
    resources_assigned.ncpus = 1056
    resources_assigned.nodect = 132
    kill_delay = 120
    max_run_res.ncpus = [u:PBS_GENERIC=500]
    backfill_depth = 10
    enabled = True
    started = True
    """

    expected = {
        "queue_type": "Execution",
        "Priority": "75",
        "total_jobs": "367",
        "state_count": "Transit:0 Queued:235 Held:0 Waiting:0 Running:132 Exiting:0 Begun:0",
        "max_queued": "[u:PBS_GENERIC=2000]",
        "resources_max.ngpus": "99",
        "resources_max.walltime": "24:00:00",
        "resources_min.mem": "50mb",
        "resources_min.ngpus": "1",
        "resources_default.ngpus": "1",
        "comment": "Queue for jobs computed on GPU",
        "default_chunk.queue_list": "q_gpu",
        "resources_assigned.mem": "1056gb",
        "resources_assigned.mpiprocs": "1056",
        "resources_assigned.ncpus": "1056",
        "resources_assigned.nodect": "132",
        "kill_delay": "120",
        "max_run_res.ncpus": "[u:PBS_GENERIC=500]",
        "backfill_depth": "10",
        "enabled": "True",
        "started": "True",
    }

    result = parsePBSDumpToDictionary(pbs_dump)
    assert result == expected


def test_parse_pbs_dump_to_dictionary_job():
    pbs_dump = """Job Id: 123456.fake-cluster.example.com
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
    Resource_List.select = 1:ncpus=8:ngpus=1:mpiprocs=8:mem=8gb:scratch_local=8gb:cl_two=true:ompthreads=1:node_owner=everybody
    Resource_List.walltime = 24:00:00
    stime = Sun Sep 21 00:00:00 2025
    session_id = 123456
    jobdir = /fake/home/user
    substate = 42
    Variable_List = QQ_DEBUG=true,QQ_ENV_SET=true,AMS_SITE_SUPPORT=linuxsupport@example.com,PBS_O_LOGNAME=user,PBS_O_QUEUE=gpu,PBS_O_HOST=host.example.com,SCRATCHDIR=/scratch/user/job_123456,SCRATCH=/scratch/user/job_123456,SINGULARITY_TMPDIR=/scratch/user/job_123456,SINGULARITY_CACHEDIR=/scratch/user/job_123456
    etime = Sun Sep 21 00:00:00 2025
    umask = 77
    run_count = 1
    eligible_time = 00:00:00
    project = _pbs_project_default
    Submit_Host = host.example.com
    credential_id = user@EXAMPLE
    credential_validity = Mon Sep 22 06:38:19 2025
    """

    expected = {
        "Job_Name": "example_job",
        "Job_Owner": "user@EXAMPLE",
        "resources_used.cpupercent": "100",
        "resources_used.cput": "01:23:45",
        "resources_used.diag_messages": "'{}'",
        "resources_used.mem": "102400kb",
        "resources_used.ncpus": "8",
        "resources_used.vmem": "102400kb",
        "resources_used.walltime": "02:00:00",
        "job_state": "R",
        "queue": "gpu",
        "server": "fake-cluster.example.com",
        "ctime": "Sun Sep 21 00:00:00 2025",
        "depend": "afterany:123455.fake-cluster.example.com@fake-cluster.example.com",
        "Error_Path": "/fake/path/job_123456.log",
        "exec_host": "node1/8*8",
        "exec_host2": "node1.example.com:15002/8*8",
        "exec_vnode": "(node1:ncpus=8:ngpus=1:mem=8192mb:scratch_local=8192mb)",
        "group_list": "examplegroup",
        "Hold_Types": "n",
        "Join_Path": "oe",
        "Mail_Points": "n",
        "mtime": "Sun Sep 21 02:00:00 2025",
        "Output_Path": "/fake/path/job_123456.log",
        "qtime": "Sun Sep 21 00:00:00 2025",
        "Rerunable": "False",
        "Resource_List.mem": "8gb",
        "Resource_List.mpiprocs": "8",
        "Resource_List.ncpus": "8",
        "Resource_List.ngpus": "1",
        "Resource_List.nodect": "1",
        "Resource_List.place": "free",
        "Resource_List.scratch_local": "8gb",
        "Resource_List.select": "1:ncpus=8:ngpus=1:mpiprocs=8:mem=8gb:scratch_local=8gb:cl_two=true:ompthreads=1:node_owner=everybody",
        "Resource_List.walltime": "24:00:00",
        "stime": "Sun Sep 21 00:00:00 2025",
        "session_id": "123456",
        "jobdir": "/fake/home/user",
        "substate": "42",
        "Variable_List": "QQ_DEBUG=true,QQ_ENV_SET=true,AMS_SITE_SUPPORT=linuxsupport@example.com,PBS_O_LOGNAME=user,PBS_O_QUEUE=gpu,PBS_O_HOST=host.example.com,SCRATCHDIR=/scratch/user/job_123456,SCRATCH=/scratch/user/job_123456,SINGULARITY_TMPDIR=/scratch/user/job_123456,SINGULARITY_CACHEDIR=/scratch/user/job_123456",
        "etime": "Sun Sep 21 00:00:00 2025",
        "umask": "77",
        "run_count": "1",
        "eligible_time": "00:00:00",
        "project": "_pbs_project_default",
        "Submit_Host": "host.example.com",
        "credential_id": "user@EXAMPLE",
        "credential_validity": "Mon Sep 22 06:38:19 2025",
    }

    result = parsePBSDumpToDictionary(pbs_dump)
    assert result == expected


def test_parse_multi_pbs_dump_to_dictionaries_queues():
    pbs_dump = """Queue: test4h
    queue_type = Execution
    Priority = 20
    total_jobs = 0
    state_count = Transit:0 Queued:0 Held:0 Waiting:0 Running:0 Exiting:0 Begun:0
    max_queued = [u:PBS_GENERIC=4000]
    from_route_only = True
    resources_max.walltime = 04:00:00
    comment = desktop computers; only dedicated users can submit on their desktops
    default_chunk.queue_list = q_test4h
    kill_delay = 120
    max_run = [u:PBS_GENERIC=2000]
    max_run_res.ncpus = [u:PBS_GENERIC=1000]
    backfill_depth = 10
    enabled = True
    started = True

Queue: maintenance
    queue_type = Execution
    Priority = 200
    total_jobs = 0
    state_count = Transit:0 Queued:0 Held:0 Waiting:0 Running:0 Exiting:0 Begun:0
    acl_user_enable = True
    acl_users = user1,user2
    resources_default.walltime = 720:00:00
    comment = Special queue marking machines in maintenance
    kill_delay = 120
    hasnodes = True
    enabled = True
    started = True

Queue: gpu
    queue_type = Execution
    Priority = 75
    total_jobs = 367
    state_count = Transit:0 Queued:235 Held:0 Waiting:0 Running:132 Exiting:0 Begun:0
    max_queued = [u:PBS_GENERIC=2000]
    resources_max.ngpus = 99
    resources_max.walltime = 24:00:00
    resources_min.mem = 50mb
    resources_min.ngpus = 1
    resources_default.ngpus = 1
    comment = Queue for jobs computed on GPU
    default_chunk.queue_list = q_gpu
    resources_assigned.mem = 1056gb
    resources_assigned.mpiprocs = 1056
    resources_assigned.ncpus = 1056
    resources_assigned.nodect = 132
    kill_delay = 120
    max_run_res.ncpus = [u:PBS_GENERIC=500]
    backfill_depth = 10
    enabled = True
    started = True
    """

    result = parseMultiPBSDumpToDictionaries(pbs_dump, "Queue")

    assert len(result) == 3
    queue_names = [name for _, name in result]
    assert queue_names == ["test4h", "maintenance", "gpu"]

    for data_dict, name in result:
        assert isinstance(data_dict, dict)
        assert "queue_type" in data_dict
        assert data_dict["enabled"] == "True"
        assert data_dict["started"] == "True"
        assert isinstance(name, str)


def test_parse_multi_pbs_dump_to_dictionaries_jobs():
    pbs_dump = """Job Id: 101.fake-cluster.example.com
    Job_Name = job_one
    Job_Owner = user1@EXAMPLE
    job_state = R
    queue = gpu
    ctime = Sun Sep 21 00:00:00 2025
    Resource_List.ncpus = 8
    Resource_List.ngpus = 1
    Resource_List.mem = 8gb
    Resource_List.walltime = 24:00:00
    started = True

Job Id: 102.fake-cluster.example.com
    Job_Name = job_two
    Job_Owner = user2@EXAMPLE
    job_state = Q
    queue = cpu
    ctime = Sun Sep 21 01:00:00 2025
    Resource_List.ncpus = 16
    Resource_List.ngpus = 0
    Resource_List.mem = 16gb
    Resource_List.walltime = 12:00:00
    started = False

Job Id: 103.fake-cluster.example.com
    Job_Name = job_three
    Job_Owner = user3@EXAMPLE
    job_state = H
    queue = maintenance
    ctime = Sun Sep 21 02:00:00 2025
    Resource_List.ncpus = 4
    Resource_List.ngpus = 0
    Resource_List.mem = 4gb
    Resource_List.walltime = 06:00:00
    started = False
    """

    result = parseMultiPBSDumpToDictionaries(pbs_dump, "Job Id")

    assert len(result) == 3

    job_names = [name for _, name in result]
    assert job_names == [
        "101.fake-cluster.example.com",
        "102.fake-cluster.example.com",
        "103.fake-cluster.example.com",
    ]

    for data_dict, name in result:
        assert isinstance(data_dict, dict)
        assert "Job_Name" in data_dict
        assert "job_state" in data_dict
        assert "queue" in data_dict
        assert isinstance(name, str)


def test_parse_pbs_dump_to_dictionary_node():
    pbs_dump = """zero21
        Mom = zero21.cluster.local
        ntype = PBS
        state = job-busy
        state_aux = free
        pcpus = 32
        Priority = 80
        resources_available.arch = linux
        resources_available.cluster = zero
        resources_available.cpu_vendor = amd
        resources_available.cuda_version = 13.0
        resources_available.gpu_mem = 10240mb
        resources_available.mem = 128673mb
        resources_available.ncpus = 32
        resources_available.ngpus = 4
        resources_available.os = debian12
        resources_available.osfamily = debian
        resources_available.queue_list = q_gpu
        resources_available.scratch_local = 870000000kb
        resources_available.scratch_ssd = 836000000kb
        resources_available.scratch_shared = 0kb
        resources_available.singularity = True
        resources_assigned.mem = 33554432kb
        resources_assigned.ncpus = 32
        resources_assigned.ngpus = 4
        resv_enable = True
        sharing = default_shared
        license = l
    """

    expected = {
        "Mom": "zero21.cluster.local",
        "ntype": "PBS",
        "state": "job-busy",
        "state_aux": "free",
        "pcpus": "32",
        "Priority": "80",
        "resources_available.arch": "linux",
        "resources_available.cluster": "zero",
        "resources_available.cpu_vendor": "amd",
        "resources_available.cuda_version": "13.0",
        "resources_available.gpu_mem": "10240mb",
        "resources_available.mem": "128673mb",
        "resources_available.ncpus": "32",
        "resources_available.ngpus": "4",
        "resources_available.os": "debian12",
        "resources_available.osfamily": "debian",
        "resources_available.queue_list": "q_gpu",
        "resources_available.scratch_local": "870000000kb",
        "resources_available.scratch_ssd": "836000000kb",
        "resources_available.scratch_shared": "0kb",
        "resources_available.singularity": "True",
        "resources_assigned.mem": "33554432kb",
        "resources_assigned.ncpus": "32",
        "resources_assigned.ngpus": "4",
        "resv_enable": "True",
        "sharing": "default_shared",
        "license": "l",
    }

    result = parsePBSDumpToDictionary(pbs_dump)
    assert result == expected


def test_parse_multi_pbs_dump_to_dictionaries_nodes():
    pbs_dump = """zero1
     Mom = zero1.cluster.local
     ntype = PBS
     state = free
     pcpus = 32
     resources_available.arch = linux
     resources_available.os = debian12
     resources_available.ncpus = 32
     resources_available.ngpus = 2
     resources_available.mem = 128000mb

zero2
     Mom = zero2.cluster.local
     ntype = PBS
     state = job-busy
     pcpus = 64
     resources_available.arch = linux
     resources_available.os = debian12
     resources_available.ncpus = 64
     resources_available.ngpus = 4
     resources_available.mem = 256000mb

zero3
     Mom = zero3.cluster.local
     ntype = PBS
     state = down
     pcpus = 16
     resources_available.arch = linux
     resources_available.os = debian12
     resources_available.ncpus = 16
     resources_available.ngpus = 0
     resources_available.mem = 64000mb
    """

    result = parseMultiPBSDumpToDictionaries(pbs_dump, None)

    assert len(result) == 3

    node_names = [name for _, name in result]
    assert node_names == ["zero1", "zero2", "zero3"]

    for data_dict, name in result:
        assert isinstance(data_dict, dict)
        assert "state" in data_dict
        assert "resources_available.os" in data_dict
        assert "resources_available.ncpus" in data_dict
        assert isinstance(name, str)


@pytest.mark.parametrize("content", ["", "    ", "\t", "\n\n\n"])
def test_parse_multi_pbs_dump_to_dictionaries_empty(content):
    assert parseMultiPBSDumpToDictionaries(content, "Queue") == []


def test_parse_multi_pbs_dump_to_dictionaries_invalid_format_raises_error():
    invalid_dump = "Invalid text without queue name line"
    with pytest.raises(QQError, match="Invalid PBS dump format"):
        parseMultiPBSDumpToDictionaries(invalid_dump, "Job Id")
