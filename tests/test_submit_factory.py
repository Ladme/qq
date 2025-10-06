# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import os
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qq_lib.batch import QQBatchMeta
from qq_lib.error import QQError
from qq_lib.job_type import QQJobType
from qq_lib.loop import QQLoopInfo
from qq_lib.pbs import QQPBS
from qq_lib.resources import QQResources
from qq_lib.submit import submit
from qq_lib.submit_factory import QQSubmitterFactory


@pytest.fixture
def factory():
    factory = QQSubmitterFactory.__new__(QQSubmitterFactory)
    factory._parser = MagicMock()
    factory._kwargs = {}
    return factory


@pytest.mark.parametrize(
    "kwargs_value,parser_value,expected",
    [
        (None, False, True),
        (None, True, False),
        (True, False, False),
        (True, True, False),
        (False, False, True),
        (False, True, False),
    ],
)
def test_get_interactive(factory, kwargs_value, parser_value, expected):
    factory._kwargs["non_interactive"] = kwargs_value
    factory._parser.getNonInteractive.return_value = parser_value

    result = factory._getInteractive()
    assert result is expected


@pytest.mark.parametrize(
    "kwargs_value,parser_value_raw,expected_raw",
    [
        (None, [], []),
        ("file1.txt,file2.txt", [], ["file1.txt", "file2.txt"]),
        (None, ["a.txt", "b.txt"], ["a.txt", "b.txt"]),
        ("file1.txt", ["a.txt"], ["file1.txt", "a.txt"]),
        (
            "file1.txt file2.txt",
            ["a.txt", "b.txt"],
            ["file1.txt", "file2.txt", "a.txt", "b.txt"],
        ),
        ("file1.txt:file2.txt", ["a.txt"], ["file1.txt", "file2.txt", "a.txt"]),
        ("file1.txt:file2.txt", ["file1.txt"], ["file1.txt", "file2.txt"]),
    ],
)
def test_get_exclude(factory, kwargs_value, parser_value_raw, expected_raw):
    parser_value = [Path(p).resolve() for p in parser_value_raw]
    expected = [Path(p).resolve() for p in expected_raw]

    factory._kwargs["exclude"] = kwargs_value
    factory._parser.getExclude.return_value = parser_value
    result = factory._getExclude()
    assert set(result) == set(expected)


def test_get_loop_info_all_kwargs(factory):
    factory._kwargs = {
        "loop_start": 2,
        "loop_end": 10,
        "archive": "archive",
        "archive_format": "cycle_%03d",
    }

    factory._parser.getLoopStart.return_value = None
    factory._parser.getLoopEnd.return_value = None
    factory._parser.getArchive.return_value = None
    factory._parser.getArchiveFormat.return_value = None

    loop_info = factory._getLoopInfo()

    assert isinstance(loop_info, QQLoopInfo)
    assert loop_info.start == 2
    assert loop_info.end == 10
    assert loop_info.archive == Path("archive").resolve()
    assert loop_info.archive_format == "cycle_%03d"


def test_get_loop_info_fallback_to_parser(factory):
    factory._kwargs = {}  # nothing provided
    factory._parser.getLoopStart.return_value = 5
    factory._parser.getLoopEnd.return_value = 20
    factory._parser.getArchive.return_value = Path("archive")
    factory._parser.getArchiveFormat.return_value = "cycle_%03d"

    loop_info = factory._getLoopInfo()

    assert loop_info.start == 5
    assert loop_info.end == 20
    assert loop_info.archive == Path("archive").resolve()
    assert loop_info.archive_format == "cycle_%03d"


def test_get_loop_info_mixed(factory):
    factory._kwargs = {
        "loop_start": 2,
        "loop_end": 10,
        "archive_format": "job%04d",
    }

    factory._parser.getLoopStart.return_value = 5
    factory._parser.getLoopEnd.return_value = 20
    factory._parser.getArchive.return_value = Path("archive")  # only this will be used
    factory._parser.getArchiveFormat.return_value = "cycle_%03d"

    loop_info = factory._getLoopInfo()

    assert loop_info.start == 2
    assert loop_info.end == 10
    assert loop_info.archive == Path("archive").resolve()
    assert loop_info.archive_format == "job%04d"


def test_get_loop_info_default_start_and_archive(factory):
    factory._kwargs = {"loop_end": 7}  # only end provided
    factory._parser.getLoopStart.return_value = None
    factory._parser.getLoopEnd.return_value = None
    factory._parser.getArchive.return_value = None
    factory._parser.getArchiveFormat.return_value = None

    loop_info = factory._getLoopInfo()

    assert loop_info.start == 1
    assert loop_info.end == 7
    assert loop_info.archive == Path("storage").resolve()
    assert loop_info.archive_format == "job%04d"


def test_get_loop_info_raises_if_missing(factory):
    factory._kwargs = {"loop_start": 1}
    factory._parser.getLoopStart.return_value = None
    factory._parser.getLoopEnd.return_value = None
    factory._parser.getArchive.return_value = None
    factory._parser.getArchiveFormat.return_value = None

    with pytest.raises(QQError, match="Attribute 'loop-end' is undefined"):
        factory._getLoopInfo()


def test_getResources_all_from_kwargs(factory):
    factory._kwargs = {
        "nnodes": 2,
        "ncpus": 8,
        "ngpus": 1,
        "walltime": "01:00:00",
        "work_dir": "scratch",
        "work_size": "4gb",
        "props": {"cl_cluster": "true"},
    }

    with patch.object(
        QQPBS, "transformResources", side_effect=lambda _queue, res: res
    ) as mocked:
        resources = factory._getResources(QQPBS, "default")

    assert resources.nnodes == 2
    assert resources.ncpus == 8
    assert resources.ngpus == 1
    assert resources.walltime == "01:00:00"
    assert resources.work_dir == "scratch"
    assert resources.work_size is not None
    assert resources.work_size.value == 4
    assert resources.work_size.unit == "gb"
    assert resources.props == {"cl_cluster": "true"}

    mocked.assert_called_once()


def test_get_resources_all_from_parser(factory):
    parser_resources = QQResources(
        nnodes=4,
        ncpus=16,
        ngpus=2,
        walltime="02:00:00",
        work_dir="scratch_local",
        work_size="8gb",
        props={"cl_cluster": "true"},
    )
    factory._parser.getResources.return_value = parser_resources

    with patch.object(
        QQPBS, "transformResources", side_effect=lambda _queue, res: res
    ) as mocked:
        resources = factory._getResources(QQPBS, "queue1")

    assert resources.nnodes == 4
    assert resources.ncpus == 16
    assert resources.ngpus == 2
    assert resources.walltime == "02:00:00"
    assert resources.work_dir == "scratch_local"
    assert resources.work_size is not None
    assert resources.work_size.value == 8
    assert resources.work_size.unit == "gb"
    assert resources.props == {"cl_cluster": "true"}

    mocked.assert_called_once()


def test_get_resources_mixed_script_and_kwargs(factory):
    # parser provides some resources
    parser_resources = QQResources(nnodes=4, ncpus=16, ngpus=2, walltime="02:00:00")
    factory._parser.getResources.return_value = parser_resources

    # kwargs overrides ncpus and adds work_dir
    factory._kwargs = {"ncpus": 32, "work_dir": "scratch_local"}

    with patch.object(
        QQPBS, "transformResources", side_effect=lambda _queue, res: res
    ) as mocked:
        resources = factory._getResources(QQPBS, "queue1")

    assert resources.nnodes == 4
    assert resources.ncpus == 32
    assert resources.ngpus == 2
    assert resources.walltime == "02:00:00"
    assert resources.work_dir == "scratch_local"

    mocked.assert_called_once()


def test_get_queue_from_kwargs_overrides_parser(factory):
    factory._kwargs = {"queue": "default"}
    factory._parser.getQueue.return_value = "cpu"

    queue = factory._getQueue()
    assert queue == "default"
    factory._parser.getQueue.assert_not_called()


def test_get_queue_from_parser_if_kwargs_missing(factory):
    factory._parser.getQueue.return_value = "cpu"

    queue = factory._getQueue()
    assert queue == "cpu"
    factory._parser.getQueue.assert_called_once()


def test_get_queue_raises_if_no_queue(factory):
    factory._parser.getQueue.return_value = None

    with pytest.raises(QQError, match="Submission queue not specified."):
        factory._getQueue()


def test_get_job_type_from_kwargs(factory):
    factory._kwargs = {"job_type": "loop"}
    factory._parser.getJobType.return_value = QQJobType.STANDARD

    job_type = factory._getJobType()
    assert job_type == QQJobType.LOOP
    # parser method should not be called if kwargs is present
    factory._parser.getJobType.assert_not_called()


def test_get_job_type_from_parser_if_no_kwargs(factory):
    factory._parser.getJobType.return_value = QQJobType.LOOP

    job_type = factory._getJobType()
    assert job_type == QQJobType.LOOP
    factory._parser.getJobType.assert_called_once()


def test_get_job_type_default_if_neither(factory):
    factory._parser.getJobType.return_value = None

    job_type = factory._getJobType()
    assert job_type == QQJobType.STANDARD
    factory._parser.getJobType.assert_called_once()


def test_get_job_type_raises_if_invalid_string_in_kwargs(factory):
    factory._kwargs = {"job_type": "invalid"}
    factory._parser.getJobType.return_value = QQJobType.STANDARD

    with pytest.raises(QQError, match="Could not recognize a job type"):
        factory._getJobType()


def test_get_batch_system_from_kwargs(factory):
    factory._kwargs = {"batch_system": "PBS"}

    batch_system = factory._getBatchSystem()
    assert batch_system == QQPBS

    factory._parser.getBatchSystem.assert_not_called()


def test_get_batch_system_from_parser(factory):
    factory._parser.getBatchSystem.return_value = QQPBS

    batch_system = factory._getBatchSystem()
    assert batch_system == QQPBS
    factory._parser.getBatchSystem.assert_called_once()


def test_get_batch_system_from_env(factory):
    factory._parser.getBatchSystem.return_value = None
    with patch.object(QQBatchMeta, "fromEnvVarOrGuess") as mock_env:
        mock_env.return_value = QQPBS

        batch_system = factory._getBatchSystem()
        assert batch_system == QQPBS
        mock_env.assert_called_once()


def test_make_submitter_standard_job(tmp_path):
    script_content = """#!/usr/bin/env -S qq run
# qq batch_system PBS
# qq queue default
# qq job_type standard
# qq ncpus 4
"""
    with tempfile.NamedTemporaryFile(mode="w+", dir=tmp_path) as tmp:
        tmp.write(script_content)
        tmp.flush()
        script = Path(tmp.name)

        os.chdir(tmp_path)

        factory = QQSubmitterFactory(script, submit.params, ["job.sh"], ncpus=8)
        submitter = factory.makeSubmitter()

        assert submitter._batch_system == QQPBS
        assert submitter._queue == "default"
        assert submitter._job_type == QQJobType.STANDARD
        assert submitter._resources.ncpus == 8
        assert submitter._loop_info is None
        assert submitter._command_line == ["job.sh"]
        assert submitter._interactive is True


def test_make_submitter_loop_job(tmp_path):
    script_content = """#!/usr/bin/env -S qq run
# qq batch_system PBS
# qq queue default
# qq job_type loop
# qq loop_start 2
# qq loop_end 5
# qq archive archive
# qq archive_format job%02d
"""
    with tempfile.NamedTemporaryFile(mode="w+", dir=tmp_path, delete=False) as tmp:
        tmp.write(script_content)
        tmp.flush()
        script = Path(tmp.name)

        os.chdir(tmp_path)

        factory = QQSubmitterFactory(
            script, submit.params, ["job_loop.sh"], loop_end=10
        )
        submitter = factory.makeSubmitter()

        assert submitter._job_type == QQJobType.LOOP
        loop_info = submitter._loop_info
        assert loop_info is not None
        assert loop_info.start == 2
        assert loop_info.end == 10
        assert loop_info.archive == Path("archive").resolve()
        assert loop_info.archive_format == "job%02d"


def test_make_submitter_missing_queue(tmp_path):
    # queue missing both in kwargs and script
    script_content = """#!/usr/bin/env -S qq run
# qq batch_system PBS
# qq job_type standard
"""
    script = tmp_path / "job_missing_queue.sh"
    script.write_text(script_content)

    factory = QQSubmitterFactory(script, submit.params, ["job_missing_queue.sh"])

    with pytest.raises(QQError, match="Submission queue not specified"):
        factory.makeSubmitter()


def test_make_submitter_script_not_exists():
    script = Path("nonexistent.sh")
    factory = QQSubmitterFactory(script, submit.params, ["nonexistent.sh"])

    with pytest.raises(QQError, match="Could not open"):
        factory.makeSubmitter()
