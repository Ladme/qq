# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

# ruff: noqa: W291


from datetime import datetime, timedelta
from pathlib import Path

import pytest

from qq_lib.batch.pbs.common import parsePBSDumpToDictionary
from qq_lib.batch.pbs.job import CFG, PBSJobInfo
from qq_lib.properties.size import Size
from qq_lib.properties.states import BatchState


@pytest.fixture
def sample_dump_file():
    return """
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


def test_get_state(sample_dump_file):
    pbs_job_info = object.__new__(PBSJobInfo)
    pbs_job_info._info = parsePBSDumpToDictionary(sample_dump_file)

    assert pbs_job_info.getState() == BatchState.RUNNING

    pbs_job_info._info["job_state"] = "Q"
    assert pbs_job_info.getState() == BatchState.QUEUED

    pbs_job_info._info["job_state"] = "F"
    # no exit code
    assert pbs_job_info.getState() == BatchState.FAILED

    pbs_job_info._info["job_state"] = "F"
    pbs_job_info._info["Exit_status"] = " 0 "
    assert pbs_job_info.getState() == BatchState.FINISHED

    pbs_job_info._info["job_state"] = "F"
    pbs_job_info._info["Exit_status"] = " 3"
    assert pbs_job_info.getState() == BatchState.FAILED

    pbs_job_info._info["job_state"] = "z"
    assert pbs_job_info.getState() == BatchState.UNKNOWN


def _make_jobinfo_with_info(info: dict[str, str]) -> PBSJobInfo:
    job = PBSJobInfo.__new__(PBSJobInfo)
    job._job_id = "1234"
    job._info = info
    return job


def test_get_comment_present():
    job = _make_jobinfo_with_info({"comment": "This is a test"})
    assert job.getComment() == "This is a test"


def test_get_comment_missing():
    job = _make_jobinfo_with_info({})
    assert job.getComment() is None


def test_get_estimated_success():
    raw_time = "Fri Oct  4 15:30:00 2124"
    vnode = "(node01:some_extra:additional_info)"
    job = _make_jobinfo_with_info(
        {"estimated.start_time": raw_time, "estimated.exec_vnode": vnode}
    )

    result = job.getEstimated()
    assert isinstance(result, tuple)

    parsed_time, parsed_vnode = result

    expected_time = datetime(2124, 10, 4, 15, 30, 0)
    assert parsed_time == expected_time
    assert parsed_vnode == "node01"


def test_get_estimated_in_past_success():
    raw_time = "Fri Oct  4 15:30:00 2024"  # in the past
    vnode = "(node01:some_extra:additional_info)"
    job = _make_jobinfo_with_info(
        {"estimated.start_time": raw_time, "estimated.exec_vnode": vnode}
    )

    result = job.getEstimated()
    assert isinstance(result, tuple)

    parsed_time, parsed_vnode = result

    # current time should be used (within 5 seconds)
    assert abs((parsed_time - datetime.now()).total_seconds()) <= 5
    assert parsed_vnode == "node01"


def test_get_estimated_success_simple_node_name():
    raw_time = "Fri Oct  4 15:30:00 2124"
    vnode = "node01"
    job = _make_jobinfo_with_info(
        {"estimated.start_time": raw_time, "estimated.exec_vnode": vnode}
    )

    result = job.getEstimated()
    assert isinstance(result, tuple)

    parsed_time, parsed_vnode = result

    expected_time = datetime(2124, 10, 4, 15, 30, 0)
    assert parsed_time == expected_time
    assert parsed_vnode == "node01"


def test_get_estimated_missing_time():
    job = _make_jobinfo_with_info(
        {"estimated.exec_vnode": "(node01:some_extra:additional_info)"}
    )
    assert job.getEstimated() is None


def test_get_estimated_missing_vnode():
    raw_time = "Fri Oct  4 15:30:00 2024"
    job = _make_jobinfo_with_info({"estimated.start_time": raw_time})
    assert job.getEstimated() is None


def test_get_estimated_parses_vnode_correctly():
    raw_time = "Fri Oct  4 15:30:00 2124"
    vnode = "(node02:ncpus=4)"
    job = _make_jobinfo_with_info(
        {"estimated.start_time": raw_time, "estimated.exec_vnode": vnode}
    )
    estimated = job.getEstimated()
    assert estimated is not None
    _, parsed_vnode = estimated
    assert parsed_vnode == "node02"


def test_get_estimated_multiple_nodes():
    raw_time = "Fri Oct  4 15:30:00 2124"
    vnode = "(node01:some_extra:additional_info)+(node03:something_else:fake_property) +node05  +  node07+(node09)"
    job = _make_jobinfo_with_info(
        {"estimated.start_time": raw_time, "estimated.exec_vnode": vnode}
    )

    result = job.getEstimated()
    assert isinstance(result, tuple)

    parsed_time, parsed_vnode = result

    expected_time = datetime(2124, 10, 4, 15, 30, 0)
    assert parsed_time == expected_time
    assert parsed_vnode == "node01 + node03 + node05 + node07 + node09"


def test_get_main_node():
    job = _make_jobinfo_with_info({"exec_host2": "node04.fake.server.org:15002/3*8"})

    assert job.getMainNode() == "node04.fake.server.org"


def test_get_main_node_multiple_nodes():
    job = _make_jobinfo_with_info(
        {
            "exec_host2": "node04.fake.server.org:15002/3*8+node05.fake.server.org:15002/3*8 + node07.fake.server.org:15002/3*8"
        }
    )

    assert job.getMainNode() == "node04.fake.server.org"


def test_get_main_node_none():
    job = _make_jobinfo_with_info({})

    assert job.getMainNode() is None


def test_get_nodes():
    job = _make_jobinfo_with_info(
        {
            "exec_host2": "node04.fake.server.org:15002/3*8+node05.fake.server.org:15002/3*8 + node07.fake.server.org:15002/3*8"
        }
    )

    assert job.getNodes() == [
        "node04.fake.server.org",
        "node05.fake.server.org",
        "node07.fake.server.org",
    ]


def test_get_short_nodes_single():
    job = _make_jobinfo_with_info({"exec_host": "node04/3*8"})

    assert job.getShortNodes() == [
        "node04",
    ]


def test_get_short_nodes():
    job = _make_jobinfo_with_info({"exec_host": "node04/3*8+node05/3*8 + node07/3*8"})

    assert job.getShortNodes() == [
        "node04",
        "node05",
        "node07",
    ]


def test_clean_node_name():
    assert PBSJobInfo._cleanNodeName("node02") == "node02"
    assert PBSJobInfo._cleanNodeName("(node02:ncpus=4)") == "node02"
    assert (
        PBSJobInfo._cleanNodeName(
            "(node05:ncpus=8:ngpus=1:mem=8388608kb:scratch_local=8388608kb)"
        )
        == "node05"
    )
    assert (
        PBSJobInfo._cleanNodeName(
            "node08:ncpus=8:ngpus=1:mem=8388608kb:scratch_local=8388608kb"
        )
        == "node08"
    )


def test_pbs_job_info_get_name_present():
    job = _make_jobinfo_with_info({"Job_Name": "training_job"})
    assert job.getName() == "training_job"


def test_pbs_job_info_get_name_missing():
    job = _make_jobinfo_with_info({})
    result = job.getName()
    assert result == "?????"


def test_pbs_job_info_get_ncpus_present():
    job = _make_jobinfo_with_info({"Resource_List.ncpus": "16"})
    assert job.getNCPUs() == 16


def test_pbs_job_info_get_ncpus_missing():
    job = _make_jobinfo_with_info({})
    result = job.getNCPUs()
    assert result == 0


def test_pbs_job_info_get_ngpus_present():
    job = _make_jobinfo_with_info({"Resource_List.ngpus": "2"})
    assert job.getNGPUs() == 2


def test_pbs_job_info_get_ngpus_missing():
    job = _make_jobinfo_with_info({})
    result = job.getNGPUs()
    assert result == 0


def test_pbs_job_info_get_nnodes_present():
    job = _make_jobinfo_with_info({"Resource_List.nodect": "3"})
    assert job.getNNodes() == 3


def test_pbs_job_info_get_nnodes_missing():
    job = _make_jobinfo_with_info({})
    result = job.getNNodes()
    assert result == 0


def test_pbs_job_info_get_mem_present():
    job = _make_jobinfo_with_info({"Resource_List.mem": "8gb"})
    mem = job.getMem()
    assert isinstance(mem, Size)
    assert mem.value == 8
    assert mem.unit == "gb"


def test_pbs_job_info_get_mem_missing():
    job = _make_jobinfo_with_info({})
    mem = job.getMem()
    # smallest size is 1 kb
    assert mem.value == 1 and mem.unit == "kb"


def test_pbs_job_info_get_mem_invalid_value():
    job = _make_jobinfo_with_info({"Resource_List.mem": "invalid123"})
    mem = job.getMem()
    # smallest size is 1kb
    assert mem.value == 1 and mem.unit == "kb"


def test_pbs_job_info_get_start_time_present():
    raw_time = "Sun Sep 21 03:15:27 2025"
    job = _make_jobinfo_with_info({"stime": raw_time})
    result = job.getStartTime()
    assert isinstance(result, datetime)
    assert result.year == 2025
    assert result.month == 9
    assert result.day == 21
    assert result.hour == 3
    assert result.minute == 15
    assert result.second == 27


def test_pbs_job_info_get_start_time_missing():
    job = _make_jobinfo_with_info({})
    result = job.getStartTime()
    assert result is None


def test_pbs_job_info_get_submission_time_present():
    raw_time = "Sun Sep 21 03:15:27 2025"
    job = _make_jobinfo_with_info({"ctime": raw_time})
    result = job.getSubmissionTime()
    assert isinstance(result, datetime)
    assert result.year == 2025
    assert result.month == 9
    assert result.day == 21
    assert result.hour == 3
    assert result.minute == 15
    assert result.second == 27


def test_pbs_job_info_get_submission_time_missing():
    job = _make_jobinfo_with_info({})
    result = job.getSubmissionTime()
    assert result == datetime.min


def test_pbs_job_info_get_completion_time_present():
    raw_time = "Sun Sep 21 03:15:27 2025"
    job = _make_jobinfo_with_info({"obittime": raw_time})
    result = job.getCompletionTime()
    assert isinstance(result, datetime)
    assert result.year == 2025
    assert result.month == 9
    assert result.day == 21
    assert result.hour == 3
    assert result.minute == 15
    assert result.second == 27


def test_pbs_job_info_get_completion_time_missing():
    job = _make_jobinfo_with_info({})
    result = job.getCompletionTime()
    assert result is None


def test_pbs_job_info_get_modification_time_present():
    raw_time = "Sun Sep 21 03:15:27 2025"
    job = _make_jobinfo_with_info({"mtime": raw_time})
    result = job.getModificationTime()
    assert isinstance(result, datetime)
    assert result.year == 2025
    assert result.month == 9
    assert result.day == 21
    assert result.hour == 3
    assert result.minute == 15
    assert result.second == 27


def test_pbs_job_info_get_modification_time_missing_submission_time_present():
    raw_time = "Sun Sep 21 03:15:27 2025"
    job = _make_jobinfo_with_info({"ctime": raw_time})
    result = job.getModificationTime()
    assert isinstance(result, datetime)
    assert result.year == 2025
    assert result.month == 9
    assert result.day == 21
    assert result.hour == 3
    assert result.minute == 15
    assert result.second == 27


def test_pbs_job_info_get_modification_time_missing():
    job = _make_jobinfo_with_info({})
    result = job.getModificationTime()
    assert result == datetime.min


def test_pbs_job_info_get_user_present():
    job = _make_jobinfo_with_info({"Job_Owner": "user@CLUSTER"})
    assert job.getUser() == "user"


def test_pbs_job_info_get_user_missing():
    job = _make_jobinfo_with_info({})
    result = job.getUser()
    assert result == "?????"


def test_pbs_job_info_get_walltime_valid():
    job = _make_jobinfo_with_info({"Resource_List.walltime": "12:35:13"})
    result = job.getWalltime()
    assert result == timedelta(hours=12, minutes=35, seconds=13)


def test_pbs_job_info_get_walltime_missing():
    job = _make_jobinfo_with_info({})
    result = job.getWalltime()
    assert result == timedelta(0)


def test_pbs_job_info_get_walltime_invalid():
    job = _make_jobinfo_with_info({"Resource_List.walltime": "not-a-time"})
    result = job.getWalltime()
    assert result == timedelta(0)


def test_pbs_job_info_get_queue_present():
    job = _make_jobinfo_with_info({"queue": "gpu"})
    assert job.getQueue() == "gpu"


def test_pbs_job_info_get_queue_missing():
    job = _make_jobinfo_with_info({})
    result = job.getQueue()
    assert result == "?????"


def test_pbs_job_info_get_util_cpu_valid():
    job = _make_jobinfo_with_info(
        {"resources_used.cpupercent": "200", "Resource_List.ncpus": "4"}
    )
    assert job.getUtilCPU() == 50


def test_pbs_job_info_get_util_cpu_missing():
    job = _make_jobinfo_with_info({})
    result = job.getUtilCPU()
    assert result is None


def test_pbs_job_info_get_util_cpu_invalid():
    job = _make_jobinfo_with_info(
        {"resources_used.cpupercent": "abc", "Resource_List.ncpus": "4"}
    )
    result = job.getUtilCPU()
    assert result is None


def test_pbs_job_info_get_util_mem_valid():
    job = _make_jobinfo_with_info(
        {"resources_used.mem": "1048576kb", "Resource_List.mem": "8gb"}
    )
    assert job.getUtilMem() == 12


def test_pbs_job_info_get_util_mem_zero():
    job = _make_jobinfo_with_info(
        {"resources_used.mem": "0b", "Resource_List.mem": "8gb"}
    )
    assert job.getUtilMem() == 0


def test_pbs_job_info_get_util_mem_missing():
    job = _make_jobinfo_with_info({})
    result = job.getUtilMem()
    assert result is None


def test_pbs_job_info_get_util_mem_invalid():
    job = _make_jobinfo_with_info(
        {"resources_used.mem": "invalid", "Resource_List.mem": "8gb"}
    )
    result = job.getUtilMem()
    assert result is None


def test_pbs_job_info_get_exit_code_valid():
    job = _make_jobinfo_with_info({"Exit_status": "0"})
    assert job.getExitCode() == 0


def test_pbs_job_info_get_exit_code_valid_nonzero():
    job = _make_jobinfo_with_info({"Exit_status": " 2 "})
    assert job.getExitCode() == 2


def test_pbs_job_info_get_exit_code_invalid():
    job = _make_jobinfo_with_info({"Exit_status": "oops"})
    result = job.getExitCode()
    assert result is None


def test_pbs_job_info_get_exit_code_missing():
    job = _make_jobinfo_with_info({})
    assert job.getExitCode() is None


def test_from_dict_creates_instance():
    info = {"Job_Name": "abc"}
    job = PBSJobInfo.fromDict("job123", info)
    assert isinstance(job, PBSJobInfo)
    assert job._job_id == "job123"
    assert job._info is info


def test_pbs_job_info_get_input_machine():
    job = _make_jobinfo_with_info({"Submit_Host": "random.machine.org"})
    assert job.getInputMachine() == "random.machine.org"


def test_pbs_job_info_get_input_machine_missing():
    job = _make_jobinfo_with_info({})
    assert job.getInputMachine() == "?????"


def test_pbs_job_info_get_input_dir_pbs():
    job = _make_jobinfo_with_info(
        {
            "Variable_List": "PBS_O_LOGNAME=user,PBS_O_WORKDIR=/path/to/input_dir,SINGLE_PROPERTY,PBS_O_HOST=host.example.com,SCRATCH=/scratch/user/job_123456"
        }
    )
    assert job.getInputDir() == Path("/path/to/input_dir")


def test_pbs_job_info_get_input_dir_qq():
    job = _make_jobinfo_with_info(
        {
            "Variable_List": f"PBS_O_LOGNAME=user,{CFG.env_vars.input_dir}=/path/to/input_dir,SINGLE_PROPERTY,PBS_O_HOST=host.example.com,SCRATCH=/scratch/user/job_123456"
        }
    )
    assert job.getInputDir() == Path("/path/to/input_dir")


def test_pbs_job_info_get_input_dir_infinity():
    job = _make_jobinfo_with_info(
        {
            "Variable_List": "PBS_O_LOGNAME=user,INF_INPUT_DIR=/path/to/input_dir,SINGLE_PROPERTY,PBS_O_HOST=host.example.com,SCRATCH=/scratch/user/job_123456"
        }
    )
    assert job.getInputDir() == Path("/path/to/input_dir")


def test_pbs_job_info_get_input_dir_nonexistent():
    job = _make_jobinfo_with_info(
        {
            "Variable_List": "PBS_O_LOGNAME=user,SINGLE_PROPERTY,PBS_O_HOST=host.example.com,SCRATCH=/scratch/user/job_123456"
        }
    )
    assert job.getInputDir() == Path("???")


def test_pbs_job_info_get_info_file():
    job = _make_jobinfo_with_info(
        {
            "Variable_List": f"{CFG.env_vars.info_file}=/path/to/info_file.qqinfo,SINGLE_PROPERTY,PBS_O_HOST=host.example.com,SCRATCH=/scratch/user/job_123456"
        }
    )
    assert job.getInfoFile() == Path("/path/to/info_file.qqinfo")


def test_pbs_job_info_get_info_file_nonexistent():
    job = _make_jobinfo_with_info(
        {
            "Variable_List": "PBS_O_LOGNAME=user,SINGLE_PROPERTY,PBS_O_HOST=host.example.com,SCRATCH=/scratch/user/job_123456"
        }
    )
    assert job.getInfoFile() is None


def test_pbs_job_info_get_env_vars():
    job = _make_jobinfo_with_info(
        {
            "Variable_List": "PBS_O_LOGNAME=user,PBS_O_QUEUE=gpu,SINGLE_PROPERTY,PBS_O_HOST=host.example.com,,,SCRATCH=/scratch/user/job_123456"
        }
    )
    assert job._getEnvVars() == {
        "PBS_O_LOGNAME": "user",
        "PBS_O_QUEUE": "gpu",
        "PBS_O_HOST": "host.example.com",
        "SCRATCH": "/scratch/user/job_123456",
    }


def test_pbs_job_info_get_env_vars_nonexistent():
    job = _make_jobinfo_with_info({})
    assert job._getEnvVars() is None


def test_pbs_job_info_get_env_vars_empty():
    job = _make_jobinfo_with_info({"Variable_List": " "})
    assert job._getEnvVars() == {}


def test_pbs_job_info_to_yaml(sample_dump_file):
    info = parsePBSDumpToDictionary(sample_dump_file)
    job = _make_jobinfo_with_info(info)

    assert (
        job.toYaml()
        == """Job Id: '1234'
Job_Name: example_job
Job_Owner: user@EXAMPLE
resources_used.cpupercent: '100'
resources_used.cput: 01:23:45
resources_used.diag_messages: '''{}'''
resources_used.mem: 102400kb
resources_used.ncpus: '8'
resources_used.vmem: 102400kb
resources_used.walltime: 02:00:00
job_state: R
queue: gpu
server: fake-cluster.example.com
ctime: Sun Sep 21 00:00:00 2025
depend: afterany:123455.fake-cluster.example.com@fake-cluster.example.com
Error_Path: /fake/path/job_123456.log
exec_host: node1/8*8
exec_host2: node1.example.com:15002/8*8
exec_vnode: (node1:ncpus=8:ngpus=1:mem=8192mb:scratch_local=8192mb)
group_list: examplegroup
Hold_Types: n
Join_Path: oe
Mail_Points: n
mtime: Sun Sep 21 02:00:00 2025
Output_Path: /fake/path/job_123456.log
qtime: Sun Sep 21 00:00:00 2025
Rerunable: 'False'
Resource_List.mem: 8gb
Resource_List.mpiprocs: '8'
Resource_List.ncpus: '8'
Resource_List.ngpus: '1'
Resource_List.nodect: '1'
Resource_List.place: free
Resource_List.scratch_local: 8gb
Resource_List.select: 1:ncpus=8:ngpus=1:mpiprocs=8:mem=8gb:scratch_local=8gb:cl_two=true:ompthreads=1:node_owner=everybody
Resource_List.walltime: '24:00:00'
stime: Sun Sep 21 00:00:00 2025
session_id: '123456'
jobdir: /fake/home/user
substate: '42'
Variable_List: QQ_DEBUG=true,QQ_ENV_SET=true,AMS_SITE_SUPPORT=linuxsupport@example.com,PBS_O_LOGNAME=user,PBS_O_QUEUE=gpu,PBS_O_HOST=host.example.com,SCRATCHDIR=/scratch/user/job_123456,SCRATCH=/scratch/user/job_123456,SINGULARITY_TMPDIR=/scratch/user/job_123456,SINGULARITY_CACHEDIR=/scratch/user/job_123456
etime: Sun Sep 21 00:00:00 2025
umask: '77'
run_count: '1'
eligible_time: 00:00:00
project: _pbs_project_default
Submit_Host: host.example.com
credential_id: user@EXAMPLE
credential_validity: Mon Sep 22 06:38:19 2025
"""
    )


def test_pbs_job_info_to_yaml_empty():
    job = _make_jobinfo_with_info({})
    assert job.toYaml().strip() == "Job Id: '1234'"


def test_pbs_job_info_is_empty():
    job = _make_jobinfo_with_info({"Submit_Host": "random.machine.org"})
    assert not job.isEmpty()


def test_pbs_job_info_is_empty_false():
    job = _make_jobinfo_with_info({})
    assert job.isEmpty()
