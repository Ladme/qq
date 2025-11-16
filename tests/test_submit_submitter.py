# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


import os
import socket
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.batch.pbs.pbs import PBS
from qq_lib.core.error import QQError
from qq_lib.info.informer import Informer
from qq_lib.properties.depend import Depend, DependType
from qq_lib.properties.job_type import JobType
from qq_lib.properties.loop import LoopInfo
from qq_lib.properties.resources import Resources
from qq_lib.properties.states import NaiveState
from qq_lib.submit.submitter import CFG, Submitter


def test_submitter_init_sets_all_attributes_correctly(tmp_path):
    script = tmp_path / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    with (
        patch.object(Submitter, "_constructJobName", return_value="job1"),
        patch.object(Submitter, "_hasValidShebang", return_value=True),
    ):
        submitter = Submitter(
            batch_system=PBS,
            queue="default",
            account=None,
            script=script,
            job_type=JobType.STANDARD,
            resources=Resources(),
            command_line=["-q", "default", str(script)],
            exclude=[Path("exclude")],
            include=[Path("include"), Path("/tmp/include")],
        )

        assert submitter._batch_system == PBS
        assert submitter._job_type == JobType.STANDARD
        assert submitter._queue == "default"
        assert submitter._account is None
        assert submitter._loop_info is None
        assert submitter._script == script
        assert submitter._input_dir == tmp_path
        assert submitter._job_name == "job1"
        assert submitter._info_file == tmp_path / f"job1{CFG.suffixes.qq_info}"
        assert submitter._resources == Resources()
        assert submitter._exclude == [tmp_path / "exclude"]
        assert submitter._include == [tmp_path / "include", Path("/tmp/include")]
        assert submitter._command_line == ["-q", "default", str(script)]
        assert submitter._depend == []


def test_submitter_init_raises_error_if_script_does_not_exist(tmp_path):
    script = tmp_path / "nonexistent.sh"

    with pytest.raises(QQError, match="does not exist"):
        Submitter(
            batch_system=PBS,
            queue="default",
            account=None,
            script=script,
            job_type=JobType.STANDARD,
            resources=Resources(),
            command_line=["-q", "default", str(script)],
        )


def test_submitter_init_raises_error_if_invalid_shebang(tmp_path):
    script = tmp_path / "bad_script.sh"
    script.write_text("invalid shebang\n")

    with (
        patch.object(Submitter, "_constructJobName", return_value="job1"),
        patch.object(Submitter, "_hasValidShebang", return_value=False),
        pytest.raises(QQError, match="invalid shebang"),
    ):
        Submitter(
            batch_system=PBS,
            queue="default",
            account="fake-account",
            script=script,
            job_type=JobType.STANDARD,
            resources=Resources(),
            command_line=["-q", "default", str(script)],
        )


def test_submitter_init_sets_all_optional_arguments_correctly(tmp_path):
    script = tmp_path / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    loop_info = LoopInfo(1, 5, Path("storage"), "job%04d")
    exclude_files = [tmp_path / "file1.txt", tmp_path / "file2.txt"]
    depend_jobs = [
        Depend(DependType.AFTER_SUCCESS, ["12345"]),
        Depend(DependType.AFTER_START, ["23456"]),
    ]

    with (
        patch.object(Submitter, "_constructJobName", return_value="job"),
        patch.object(Submitter, "_hasValidShebang", return_value=True),
    ):
        submitter = Submitter(
            batch_system=PBS,
            queue="long",
            account="fake-account",
            script=script,
            job_type=JobType.LOOP,
            resources=Resources(),
            command_line=["-q", "long", str(script)],
            loop_info=loop_info,
            exclude=exclude_files,
            depend=depend_jobs,
        )

        assert submitter._batch_system == PBS
        assert submitter._job_type == JobType.LOOP
        assert submitter._queue == "long"
        assert submitter._account == "fake-account"
        assert submitter._loop_info == loop_info
        assert submitter._script == script
        assert submitter._input_dir == tmp_path
        assert submitter._script_name == script.name
        assert submitter._job_name == "job"
        assert submitter._info_file == tmp_path / f"job{CFG.suffixes.qq_info}"
        assert submitter._resources == Resources()
        assert submitter._exclude == exclude_files
        assert submitter._command_line == ["-q", "long", str(script)]
        assert submitter._depend == depend_jobs


def test_submitter_construct_job_name_returns_script_name_for_standard_job(
    tmp_path,
):
    script = tmp_path / "job.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")
    submitter = Submitter.__new__(Submitter)
    submitter._script_name = "job.sh"
    submitter._loop_info = None

    result = submitter._constructJobName()

    assert result == "job.sh"


def test_submitter_construct_job_name_returns_name_with_cycle_number_for_loop_job(
    tmp_path,
):
    script = tmp_path / "job.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")
    submitter = Submitter.__new__(Submitter)
    submitter._script_name = "job.sh"

    class DummyLoopInfo:
        current = 3

    submitter._loop_info = DummyLoopInfo()

    result = submitter._constructJobName()

    assert result == f"job{CFG.loop_jobs.pattern % 3}.sh"


def test_submitter_construct_job_name_loop_job_no_extension(
    tmp_path,
):
    script = tmp_path / "job"
    script.write_text("#!/usr/bin/env -S qq run\n")
    submitter = Submitter.__new__(Submitter)
    submitter._script_name = "job"

    class DummyLoopInfo:
        current = 3

    submitter._loop_info = DummyLoopInfo()

    result = submitter._constructJobName()

    assert result == f"job{CFG.loop_jobs.pattern % 3}"


def test_submitter_has_valid_shebang_returns_true_for_valid_shebang(tmp_path):
    script = tmp_path / "valid_script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    submitter = Submitter.__new__(Submitter)
    result = submitter._hasValidShebang(script)

    assert result is True


def test_submitter_has_valid_shebang_returns_false_if_not_ending_with_qq_run(
    tmp_path,
):
    script = tmp_path / "wrong_end.sh"
    script.write_text("#!/usr/bin/env python\n")

    submitter = Submitter.__new__(Submitter)
    result = submitter._hasValidShebang(script)

    assert result is False


def test_submitter__has_valid_shebang_returns_false_when_no_shebang_line(tmp_path):
    script = tmp_path / "random_command.sh"
    script.write_text("echo 'hello world'\n")

    submitter = Submitter.__new__(Submitter)
    result = submitter._hasValidShebang(script)

    assert result is False


@pytest.mark.parametrize("debug_mode", [True, False])
def test_submitter_create_env_vars_dict_sets_all_required_variables(
    tmp_path, debug_mode
):
    script = tmp_path / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    submitter = Submitter.__new__(Submitter)
    submitter._info_file = tmp_path / "job.qqinfo"
    submitter._batch_system = PBS
    submitter._loop_info = None
    submitter._input_dir = tmp_path
    submitter._resources = Resources(nnodes=2, ncpus=8, ngpus=2, walltime="1d")

    if debug_mode:
        with patch.dict(os.environ, {CFG.env_vars.debug_mode: "true"}):
            env = submitter._createEnvVarsDict()
    else:
        env = submitter._createEnvVarsDict()

    assert env[CFG.env_vars.guard] == "true"
    assert env[CFG.env_vars.info_file] == str(submitter._info_file)
    assert env[CFG.env_vars.input_machine] == socket.gethostname()
    assert env[CFG.env_vars.batch_system] == str(submitter._batch_system)
    assert env[CFG.env_vars.input_dir] == str(submitter._input_dir)
    assert env[CFG.env_vars.nnodes] == str(submitter._resources.nnodes)
    assert env[CFG.env_vars.ncpus] == str(submitter._resources.ncpus)
    assert env[CFG.env_vars.ngpus] == str(submitter._resources.ngpus)
    assert env[CFG.env_vars.walltime] == "24.0"
    if debug_mode:
        assert env[CFG.env_vars.debug_mode] == "true"
    else:
        assert CFG.env_vars.debug_mode not in env


@pytest.mark.parametrize("debug_mode", [True, False])
def test_submitter_create_env_vars_dict_sets_loop_variables(tmp_path, debug_mode):
    script = tmp_path / "script.sh"
    script.write_text("#!/usr/bin/env -S qq run\n")

    class DummyLoop:
        current = 1
        start = 0
        end = 5
        archive_format = "zip"

    submitter = Submitter.__new__(Submitter)
    submitter._info_file = tmp_path / "job.qqinfo"
    submitter._batch_system = "BatchSystem"
    submitter._loop_info = DummyLoop()
    submitter._input_dir = tmp_path
    submitter._resources = Resources()

    if debug_mode:
        with patch.dict(os.environ, {CFG.env_vars.debug_mode: "true"}):
            env = submitter._createEnvVarsDict()
    else:
        env = submitter._createEnvVarsDict()

    assert env[CFG.env_vars.guard] == "true"
    assert env[CFG.env_vars.info_file] == str(submitter._info_file)
    assert env[CFG.env_vars.input_machine] == socket.gethostname()
    assert env[CFG.env_vars.batch_system] == str(submitter._batch_system)
    assert env[CFG.env_vars.input_dir] == str(submitter._input_dir)

    assert env[CFG.env_vars.loop_current] == str(DummyLoop.current)
    assert env[CFG.env_vars.loop_start] == str(DummyLoop.start)
    assert env[CFG.env_vars.loop_end] == str(DummyLoop.end)
    assert env[CFG.env_vars.archive_format] == DummyLoop.archive_format
    assert env[CFG.env_vars.no_resubmit] == str(CFG.exit_codes.qq_run_no_resubmit)
    if debug_mode:
        assert env[CFG.env_vars.debug_mode] == "true"
    else:
        assert CFG.env_vars.debug_mode not in env


def test_submitter_get_input_dir_returns_correct_path(tmp_path):
    submitter = Submitter.__new__(Submitter)
    submitter._input_dir = tmp_path

    result = submitter.getInputDir()

    assert result == tmp_path


def test_submitter_continues_loop_returns_true_for_valid_continuation(tmp_path):
    submitter = Submitter.__new__(Submitter)
    submitter._loop_info = MagicMock(current=2)
    submitter._input_dir = tmp_path

    dummy_info = MagicMock()
    dummy_info.loop_info = MagicMock(current=1)
    dummy_info.job_state = NaiveState.FINISHED

    dummy_informer = MagicMock()
    dummy_informer.info = dummy_info

    with (
        patch(
            "qq_lib.submit.submitter.get_info_file",
            return_value=tmp_path / "job.qqinfo",
        ),
        patch.object(Informer, "fromFile", return_value=dummy_informer),
    ):
        result = submitter.continuesLoop()

    assert result is True


def test_submitter_continues_loop_returns_false_if_previous_not_finished(tmp_path):
    submitter = Submitter.__new__(Submitter)
    submitter._loop_info = MagicMock(current=2)
    submitter._input_dir = tmp_path

    dummy_info = MagicMock()
    dummy_info.loop_info = MagicMock(current=1)
    dummy_info.job_state = NaiveState.RUNNING

    dummy_informer = MagicMock()
    dummy_informer.info = dummy_info

    with (
        patch(
            "qq_lib.submit.submitter.get_info_file",
            return_value=tmp_path / "job.qqinfo",
        ),
        patch.object(Informer, "fromFile", return_value=dummy_informer),
    ):
        result = submitter.continuesLoop()

    assert result is False


def test_submitter_continues_loop_returns_false_if_previous_cycle_mismatch(tmp_path):
    submitter = Submitter.__new__(Submitter)
    submitter._loop_info = MagicMock(current=5)
    submitter._input_dir = tmp_path

    dummy_info = MagicMock()
    dummy_info.loop_info = MagicMock(current=3)
    dummy_info.job_state = NaiveState.FINISHED

    dummy_informer = MagicMock()
    dummy_informer.info = dummy_info

    with (
        patch(
            "qq_lib.submit.submitter.get_info_file",
            return_value=tmp_path / "job.qqinfo",
        ),
        patch.object(Informer, "fromFile", return_value=dummy_informer),
    ):
        result = submitter.continuesLoop()

    assert result is False


def test_submitter_continues_loop_returns_false_if_no_loop_info_in_past(tmp_path):
    submitter = Submitter.__new__(Submitter)
    submitter._loop_info = MagicMock(current=3)
    submitter._input_dir = tmp_path

    dummy_info = MagicMock()
    dummy_info.loop_info = None
    dummy_info.job_state = NaiveState.FINISHED

    dummy_informer = MagicMock()
    dummy_informer.info = dummy_info

    with (
        patch(
            "qq_lib.submit.submitter.get_info_file",
            return_value=tmp_path / "job.qqinfo",
        ),
        patch.object(Informer, "fromFile", return_value=dummy_informer),
    ):
        result = submitter.continuesLoop()

    assert result is False


def test_submitter_continues_loop_returns_false_if_no_loop_info_current(tmp_path):
    submitter = Submitter.__new__(Submitter)
    submitter._loop_info = None
    submitter._input_dir = tmp_path

    dummy_info = MagicMock()
    dummy_info.loop_info = MagicMock(current=3)
    dummy_info.job_state = NaiveState.FINISHED

    dummy_informer = MagicMock()
    dummy_informer.info = dummy_info

    with (
        patch(
            "qq_lib.submit.submitter.get_info_file",
            return_value=tmp_path / "job.qqinfo",
        ),
        patch.object(Informer, "fromFile", return_value=dummy_informer),
    ):
        result = submitter.continuesLoop()

    assert result is False


def test_submitter_continues_loop_returns_false_on_qqerror(tmp_path):
    submitter = Submitter.__new__(Submitter)

    submitter._loop_info = MagicMock(current=2)
    submitter._input_dir = tmp_path

    with patch("qq_lib.submit.submitter.get_info_file", side_effect=QQError("error")):
        result = submitter.continuesLoop()

    assert result is False


def test_submitter_submit_calls_all_steps_and_returns_job_id(tmp_path):
    submitter = Submitter.__new__(Submitter)
    submitter._batch_system = MagicMock()
    submitter._resources = Resources()
    submitter._queue = "default"
    submitter._account = None
    submitter._script = tmp_path / "script.sh"
    submitter._job_name = "job1"
    submitter._script_name = "script.sh"
    submitter._job_type = JobType.STANDARD
    submitter._input_dir = tmp_path
    submitter._loop_info = None
    submitter._exclude = []
    submitter._include = []
    submitter._command_line = ["-q", "default", str(submitter._script)]
    submitter._depend = []
    submitter._info_file = tmp_path / f"{submitter._job_name}.qqinfo"
    env_vars = {CFG.env_vars.guard: "true"}

    with (
        patch.object(
            submitter, "_createEnvVarsDict", return_value=env_vars
        ) as mock_set_env,
        patch.object(
            submitter._batch_system, "jobSubmit", return_value="jobid123"
        ) as mock_job_submit,
        patch("qq_lib.submit.submitter.Informer") as mock_informer_class,
        patch("qq_lib.__version__", "1.0"),
    ):
        mock_informer_instance = MagicMock()
        mock_informer_class.return_value = mock_informer_instance

        result = submitter.submit()

    mock_set_env.assert_called_once()
    mock_job_submit.assert_called_once_with(
        submitter._resources,
        submitter._queue,
        submitter._script,
        submitter._job_name,
        submitter._depend,
        env_vars,
        submitter._account,
    )
    mock_informer_class.assert_called_once()
    mock_informer_instance.toFile.assert_called_once_with(submitter._info_file)
    assert result == "jobid123"


def test_submitter_submit(tmp_path):
    submitter = Submitter.__new__(Submitter)
    submitter._batch_system = MagicMock()
    submitter._resources = Resources()
    submitter._queue = "default"
    submitter._account = "fake-account"
    submitter._script = tmp_path / "script.sh"
    submitter._job_name = "job1"
    submitter._script_name = "script.sh"
    submitter._job_type = JobType.STANDARD
    submitter._input_dir = tmp_path
    submitter._loop_info = None
    submitter._exclude = ["exclude1"]
    submitter._include = ["include1"]
    submitter._command_line = ["-q", "default", str(submitter._script)]
    submitter._depend = []
    submitter._info_file = tmp_path / f"{submitter._job_name}.qqinfo"
    env_vars = {CFG.env_vars.guard: "true"}

    with (
        patch.object(
            submitter, "_createEnvVarsDict", return_value=env_vars
        ) as mock_set_env,
        patch.object(
            submitter._batch_system, "jobSubmit", return_value="jobid123"
        ) as mock_job_submit,
        patch("qq_lib.submit.submitter.Informer") as mock_informer_class,
        patch("qq_lib.__version__", "1.0"),
        patch("getpass.getuser", return_value="testuser"),
        patch("socket.gethostname", return_value="host123"),
        patch("qq_lib.submit.submitter.datetime") as mock_datetime,
    ):
        mock_datetime.now.return_value = datetime(2025, 10, 14, 12, 0, 0)
        mock_informer_instance = MagicMock()
        mock_informer_class.return_value = mock_informer_instance

        result = submitter.submit()

    mock_set_env.assert_called_once()
    mock_job_submit.assert_called_once_with(
        submitter._resources,
        submitter._queue,
        submitter._script,
        submitter._job_name,
        submitter._depend,
        env_vars,
        submitter._account,
    )
    mock_informer_class.assert_called_once()
    mock_informer_instance.toFile.assert_called_once_with(submitter._info_file)
    assert result == "jobid123"

    # capture the Info passed to Informer
    info_arg = mock_informer_class.call_args[0][0]

    assert info_arg.batch_system == submitter._batch_system
    assert info_arg.qq_version == "1.0"
    assert info_arg.username == "testuser"
    assert info_arg.job_id == "jobid123"
    assert info_arg.job_name == submitter._job_name
    assert info_arg.script_name == submitter._script_name
    assert info_arg.queue == submitter._queue
    assert info_arg.account == submitter._account
    assert info_arg.job_type == submitter._job_type
    assert info_arg.input_machine == "host123"
    assert info_arg.input_dir == submitter._input_dir
    assert info_arg.job_state == NaiveState.QUEUED
    assert info_arg.submission_time == datetime(2025, 10, 14, 12, 0, 0)
    assert info_arg.stdout_file == str(
        Path(submitter._job_name).with_suffix(CFG.suffixes.stdout)
    )
    assert info_arg.stderr_file == str(
        Path(submitter._job_name).with_suffix(CFG.suffixes.stderr)
    )
    assert info_arg.resources == submitter._resources
    assert info_arg.loop_info == submitter._loop_info
    assert info_arg.excluded_files == submitter._exclude
    assert info_arg.included_files == submitter._include
    assert info_arg.command_line == submitter._command_line
    assert info_arg.depend == submitter._depend
