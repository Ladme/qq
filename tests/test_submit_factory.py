# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.batch.interface import BatchMeta
from qq_lib.batch.interface.interface import BatchInterface
from qq_lib.core.error import QQError
from qq_lib.properties.depend import Depend
from qq_lib.properties.job_type import JobType
from qq_lib.properties.loop import LoopInfo
from qq_lib.properties.resources import Resources
from qq_lib.properties.size import Size
from qq_lib.submit.factory import SubmitterFactory


def test_submitter_factory_init(tmp_path):
    script = tmp_path / "script.sh"
    params = [MagicMock(), MagicMock()]
    command_line = ["-q", "default", str(script)]
    kwargs = {"queue": "default"}

    with patch("qq_lib.submit.factory.Parser") as mock_parser_class:
        mock_parser_instance = MagicMock()
        mock_parser_class.return_value = mock_parser_instance

        factory = SubmitterFactory(script, params, command_line, **kwargs)

    assert factory._parser == mock_parser_instance
    assert factory._script == script
    assert factory._input_dir == tmp_path
    assert factory._command_line == command_line
    assert factory._kwargs == kwargs
    mock_parser_class.assert_called_once_with(script, params)


def test_submitter_factory_get_depend():
    mock_parser = MagicMock()
    parser_depend = [MagicMock(), MagicMock()]
    mock_parser.getDepend.return_value = parser_depend

    factory = SubmitterFactory.__new__(SubmitterFactory)
    factory._parser = mock_parser
    factory._kwargs = {"depend": "afterok=1234,afterany=2345"}

    cli_depend_list = [MagicMock(), MagicMock()]

    with patch.object(
        Depend, "multiFromStr", return_value=cli_depend_list
    ) as mock_multi:
        result = factory._getDepend()

    mock_multi.assert_called_once_with("afterok=1234,afterany=2345")
    assert result == cli_depend_list + parser_depend


def test_submitter_factory_get_exclude():
    mock_parser = MagicMock()
    parser_excludes = [Path("/tmp/file1"), Path("/tmp/file2")]
    mock_parser.getExclude.return_value = parser_excludes

    factory = SubmitterFactory.__new__(SubmitterFactory)
    factory._parser = mock_parser
    factory._kwargs = {"exclude": "/tmp/file3,/tmp/file4"}

    cli_excludes = [Path("/tmp/file3"), Path("/tmp/file4")]

    with patch(
        "qq_lib.submit.factory.split_files_list", return_value=cli_excludes
    ) as mock_split:
        result = factory._getExclude()

    mock_split.assert_called_once_with("/tmp/file3,/tmp/file4")
    assert set(result) == set(cli_excludes + parser_excludes)


def test_submitter_factory_get_loop_info_uses_cli_over_parser():
    mock_parser = MagicMock()
    mock_parser.getLoopStart.return_value = 2
    mock_parser.getLoopEnd.return_value = 5
    mock_parser.getArchive.return_value = Path("storage")
    mock_parser.getArchiveFormat.return_value = "job%02d"

    factory = SubmitterFactory.__new__(SubmitterFactory)
    factory._input_dir = Path("fake_path")
    factory._parser = mock_parser
    factory._kwargs = {
        "loop_start": 10,
        "loop_end": 20,
        "archive": "archive",
        "archive_format": "job%04d",
    }

    loop_info = factory._getLoopInfo()

    assert isinstance(loop_info, LoopInfo)
    assert loop_info.start == 10
    assert loop_info.end == 20
    assert loop_info.archive == Path("archive").resolve()
    assert loop_info.archive_format == "job%04d"


def test_submitter_factory_get_loop_info_falls_back_to_parser():
    mock_parser = MagicMock()
    mock_parser.getLoopStart.return_value = 2
    mock_parser.getLoopEnd.return_value = 5
    mock_parser.getArchive.return_value = Path("archive")
    mock_parser.getArchiveFormat.return_value = "job%02d"

    factory = SubmitterFactory.__new__(SubmitterFactory)
    factory._input_dir = Path("fake_path")
    factory._parser = mock_parser
    factory._kwargs = {}  # nothing from CLI

    loop_info = factory._getLoopInfo()

    assert isinstance(loop_info, LoopInfo)
    assert loop_info.start == 2
    assert loop_info.end == 5
    assert loop_info.archive == Path("archive").resolve()
    assert loop_info.archive_format == "job%02d"


def test_submitter_factory_get_loop_info_mixed_cli_parser_and_defaults():
    mock_parser = MagicMock()
    mock_parser.getLoopStart.return_value = None
    mock_parser.getLoopEnd.return_value = 50
    mock_parser.getArchive.return_value = None
    mock_parser.getArchiveFormat.return_value = "job%02d"

    factory = SubmitterFactory.__new__(SubmitterFactory)
    factory._input_dir = Path("fake_path")
    factory._parser = mock_parser
    factory._kwargs = {
        "loop_start": 10,
    }

    loop_info = factory._getLoopInfo()

    assert isinstance(loop_info, LoopInfo)
    assert loop_info.start == 10  # CLI
    assert loop_info.end == 50  # parser
    assert loop_info.archive == Path("storage").resolve()  # default
    assert loop_info.archive_format == "job%02d"  # parser


def test_submitter_factory_get_resources():
    mock_parser = MagicMock()
    parser_resources = Resources(ncpus=4, mem="4gb")
    mock_parser.getResources.return_value = parser_resources

    factory = SubmitterFactory.__new__(SubmitterFactory)
    factory._parser = mock_parser
    factory._kwargs = {"ncpus": 8, "walltime": "1d", "foo": "bar"}  # CLI resources

    mock_batch_system = MagicMock()

    transformed_resources = Resources(ncpus=999, mem="999gb")
    mock_batch_system.transformResources.return_value = transformed_resources

    result = factory._getResources(mock_batch_system, "default")

    merged_resources_arg = mock_batch_system.transformResources.call_args[0][1]

    # CLI overrides parser where provided
    assert merged_resources_arg.ncpus == 8
    assert merged_resources_arg.mem == Size(4, "gb")  # from parser
    assert merged_resources_arg.walltime == "24:00:00"  # from CLI

    assert result == transformed_resources


def test_submitter_factory_get_queue_uses_cli_over_parser():
    mock_parser = MagicMock()
    mock_parser.getQueue.return_value = "parser_queue"

    factory = SubmitterFactory.__new__(SubmitterFactory)
    factory._parser = mock_parser
    factory._kwargs = {"queue": "cli_queue"}

    queue = factory._getQueue()
    assert queue == "cli_queue"


def test_submitter_factory_get_queue_uses_parser_if_no_cli():
    mock_parser = MagicMock()
    mock_parser.getQueue.return_value = "parser_queue"

    factory = SubmitterFactory.__new__(SubmitterFactory)
    factory._parser = mock_parser
    factory._kwargs = {}  # no CLI queue

    queue = factory._getQueue()
    assert queue == "parser_queue"


def test_submitter_factory_get_queue_raises_error_if_missing():
    mock_parser = MagicMock()
    mock_parser.getQueue.return_value = None

    factory = SubmitterFactory.__new__(SubmitterFactory)
    factory._parser = mock_parser
    factory._kwargs = {}

    with pytest.raises(QQError, match="Submission queue not specified."):
        factory._getQueue()


def test_submitter_factory_get_account_uses_cli_over_parser():
    mock_parser = MagicMock()
    mock_parser.getAccount.return_value = "parser_account"

    factory = SubmitterFactory.__new__(SubmitterFactory)
    factory._parser = mock_parser
    factory._kwargs = {"account": "cli_account"}

    queue = factory._getAccount()
    assert queue == "cli_account"


def test_submitter_factory_get_account_uses_parser_if_no_cli():
    mock_parser = MagicMock()
    mock_parser.getAccount.return_value = "parser_account"

    factory = SubmitterFactory.__new__(SubmitterFactory)
    factory._parser = mock_parser
    factory._kwargs = {}  # no CLI account

    queue = factory._getAccount()
    assert queue == "parser_account"


def test_submitter_factory_get_job_type_uses_cli_over_parser():
    mock_parser = MagicMock()
    parser_job_type = JobType.LOOP
    mock_parser.getJobType.return_value = parser_job_type

    factory = SubmitterFactory.__new__(SubmitterFactory)
    factory._parser = mock_parser
    factory._kwargs = {"job_type": "standard"}

    with patch.object(
        JobType, "fromStr", return_value=JobType.STANDARD
    ) as mock_from_str:
        result = factory._getJobType()

    mock_from_str.assert_called_once_with("standard")
    assert result == JobType.STANDARD


def test_submitter_factory_get_job_type_uses_parser_if_no_cli():
    mock_parser = MagicMock()
    parser_job_type = JobType.LOOP
    mock_parser.getJobType.return_value = parser_job_type

    factory = SubmitterFactory.__new__(SubmitterFactory)
    factory._parser = mock_parser
    factory._kwargs = {}  # no CLI job_type

    result = factory._getJobType()
    assert result == parser_job_type


def test_submitter_factory_get_job_type_defaults_to_standard_if_missing():
    mock_parser = MagicMock()
    mock_parser.getJobType.return_value = None

    factory = SubmitterFactory.__new__(SubmitterFactory)
    factory._parser = mock_parser
    factory._kwargs = {}

    result = factory._getJobType()
    assert result == JobType.STANDARD


def test_submitter_factory_get_batch_system_uses_cli_over_parser_and_env():
    mock_parser = MagicMock()
    parser_batch = MagicMock(spec=BatchInterface)
    mock_parser.getBatchSystem.return_value = parser_batch

    factory = SubmitterFactory.__new__(SubmitterFactory)
    factory._parser = mock_parser
    factory._kwargs = {"batch_system": "PBS"}

    mock_class = MagicMock(spec=BatchInterface)
    with patch.object(BatchMeta, "fromStr", return_value=mock_class) as mock_from_str:
        result = factory._getBatchSystem()

    mock_from_str.assert_called_once_with("PBS")
    assert result == mock_class


def test_submitter_factory_get_batch_system_uses_parser_if_no_cli():
    mock_parser = MagicMock()
    parser_batch = MagicMock(spec=BatchInterface)
    mock_parser.getBatchSystem.return_value = parser_batch

    factory = SubmitterFactory.__new__(SubmitterFactory)
    factory._parser = mock_parser
    factory._kwargs = {}  # no CLI

    result = factory._getBatchSystem()
    assert result == parser_batch


def test_submitter_factory_get_batch_system_uses_env_guess_if_no_cli_or_parser():
    mock_parser = MagicMock()
    mock_parser.getBatchSystem.return_value = None

    factory = SubmitterFactory.__new__(SubmitterFactory)
    factory._parser = mock_parser
    factory._kwargs = {}  # no CLI

    mock_guess = MagicMock(spec=BatchInterface)
    with patch.object(
        BatchMeta, "fromEnvVarOrGuess", return_value=mock_guess
    ) as mock_method:
        result = factory._getBatchSystem()

    mock_method.assert_called_once()
    assert result == mock_guess


def test_submitter_factory_make_submitter_standard_job():
    mock_parser = MagicMock()
    mock_parser.parse = MagicMock()
    mock_parser.getJobType.return_value = JobType.STANDARD
    resources = Resources()
    excludes = [Path("/tmp/file1")]
    depends = []
    account = "fake-account"

    factory = SubmitterFactory.__new__(SubmitterFactory)
    factory._parser = mock_parser
    factory._script = Path("/tmp/script.sh")
    factory._command_line = ["--arg"]
    factory._kwargs = {"queue": "default", "job_type": "standard"}

    BatchSystem = MagicMock()
    queue = "default"

    with (
        patch.object(
            factory, "_getBatchSystem", return_value=BatchSystem
        ) as mock_get_batch,
        patch.object(factory, "_getQueue", return_value=queue) as mock_get_queue,
        patch.object(factory, "_getLoopInfo") as mock_get_loop,
        patch.object(factory, "_getResources", return_value=resources) as mock_get_res,
        patch.object(factory, "_getExclude", return_value=excludes) as mock_get_excl,
        patch.object(factory, "_getDepend", return_value=depends) as mock_get_dep,
        patch.object(factory, "_getAccount", return_value=account) as mock_get_acct,
        patch("qq_lib.submit.factory.QQSubmitter") as mock_submitter_class,
    ):
        mock_submit_instance = MagicMock()
        mock_submitter_class.return_value = mock_submit_instance

        result = factory.makeSubmitter()

    mock_parser.parse.assert_called_once()
    mock_get_batch.assert_called_once()
    mock_get_queue.assert_called_once()
    mock_get_loop.assert_not_called()  # STANDARD job, loop info not used
    mock_get_res.assert_called_once_with(BatchSystem, queue)
    mock_get_excl.assert_called_once()
    mock_get_dep.assert_called_once()
    mock_get_acct.assert_called_once()

    mock_submitter_class.assert_called_once_with(
        BatchSystem,
        queue,
        account,
        factory._script,
        JobType.STANDARD,
        resources,
        factory._command_line,
        None,  # loop_info is None for STANDARD job
        excludes,
        depends,
    )
    assert result == mock_submit_instance


def test_submitter_factory_make_submitter_loop_job():
    mock_parser = MagicMock()
    mock_parser.parse = MagicMock()
    mock_parser.getJobType.return_value = JobType.LOOP
    resources = Resources()
    excludes = [Path("/tmp/file1")]
    depends = []
    account = None

    factory = SubmitterFactory.__new__(SubmitterFactory)
    factory._parser = mock_parser
    factory._script = Path("/tmp/script.sh")
    factory._command_line = ["--arg"]
    factory._kwargs = {"queue": "default", "job_type": "loop"}

    BatchSystem = MagicMock()
    queue = "default"
    loop_info = MagicMock()

    with (
        patch.object(
            factory, "_getBatchSystem", return_value=BatchSystem
        ) as mock_get_batch,
        patch.object(factory, "_getQueue", return_value=queue) as mock_get_queue,
        patch.object(factory, "_getLoopInfo", return_value=loop_info) as mock_get_loop,
        patch.object(factory, "_getResources", return_value=resources) as mock_get_res,
        patch.object(factory, "_getExclude", return_value=excludes) as mock_get_excl,
        patch.object(factory, "_getDepend", return_value=depends) as mock_get_dep,
        patch.object(factory, "_getAccount", return_value=account) as mock_get_acct,
        patch("qq_lib.submit.factory.QQSubmitter") as mock_submitter_class,
    ):
        mock_submit_instance = MagicMock()
        mock_submitter_class.return_value = mock_submit_instance

        result = factory.makeSubmitter()

    mock_parser.parse.assert_called_once()
    mock_get_batch.assert_called_once()
    mock_get_queue.assert_called_once()
    mock_get_loop.assert_called_once()
    mock_get_res.assert_called_once_with(BatchSystem, queue)
    mock_get_excl.assert_called_once()
    mock_get_dep.assert_called_once()
    mock_get_acct.assert_called_once()

    mock_submitter_class.assert_called_once_with(
        BatchSystem,
        queue,
        account,
        factory._script,
        JobType.LOOP,
        resources,
        factory._command_line,
        loop_info,
        excludes,
        depends,
    )
    assert result == mock_submit_instance
