# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import shutil
import tempfile
from pathlib import Path

import pytest

from qq_lib.core.error import QQError
from qq_lib.properties.loop import LoopInfo


def test_valid_constructor(tmp_path):
    input_dir = tmp_path / "job"

    loop_info = LoopInfo(
        start=1,
        end=5,
        archive=input_dir / "archive",
        input_dir=input_dir,
        archive_format="md%04d",
    )

    assert loop_info.start == 1
    assert loop_info.end == 5
    assert loop_info.current == 1
    assert loop_info.archive == (input_dir / "archive").resolve()
    assert loop_info.archive_format == "md%04d"


def test_constructor_with_current(tmp_path):
    input_dir = tmp_path / "job"

    loop_info = LoopInfo(
        start=1,
        end=5,
        archive=input_dir / "archive",
        input_dir=input_dir,
        archive_format="md%04d",
        current=5,
    )

    assert loop_info.start == 1
    assert loop_info.end == 5
    assert loop_info.current == 5
    assert loop_info.archive == (input_dir / "archive").resolve()
    assert loop_info.archive_format == "md%04d"


def test_missing_end(tmp_path):
    input_dir = tmp_path / "job"

    with pytest.raises(QQError, match="loop-end"):
        LoopInfo(
            start=1,
            end=None,
            archive=input_dir / "archive",
            input_dir=input_dir,
            archive_format="md%04d",
        )


def test_start_greater_than_end(tmp_path):
    input_dir = tmp_path / "job"

    with pytest.raises(QQError, match="loop-start"):
        LoopInfo(
            start=10,
            end=5,
            archive=input_dir / "archive",
            input_dir=input_dir,
            archive_format="md%04d",
        )


def test_start_negative(tmp_path):
    input_dir = tmp_path / "job"

    with pytest.raises(QQError, match="loop-start"):
        LoopInfo(
            start=-1,
            end=5,
            archive=input_dir / "archive",
            input_dir=input_dir,
            archive_format="md%04d",
        )


def test_current_greater_than_end(tmp_path):
    input_dir = tmp_path / "job"

    with pytest.raises(QQError, match="Current cycle number"):
        LoopInfo(
            start=1,
            end=5,
            archive=input_dir / "archive",
            input_dir=input_dir,
            archive_format="md%04d",
            current=6,
        )


def test_invalid_archive_dir(tmp_path):
    input_dir = tmp_path / "job"

    with pytest.raises(
        QQError, match="Input directory cannot be used as the loop job's archive"
    ):
        LoopInfo(
            start=1,
            end=5,
            archive=input_dir,
            input_dir=input_dir,
            archive_format="md%04d",
        )


def _create_loop_info_stub(start, archive_path, archive_format="md%04d"):
    loop_info = LoopInfo.__new__(LoopInfo)
    loop_info.start = start
    loop_info.archive = Path(archive_path).resolve()
    loop_info.archive_format = archive_format
    return loop_info


@pytest.fixture
def temp_dir():
    tmp = tempfile.mkdtemp()
    yield Path(tmp)
    shutil.rmtree(tmp)


def test_get_cycle_returns_start_if_archive_does_not_exist(tmp_path):
    loop_info = _create_loop_info_stub(5, tmp_path / "nonexistent", "md%04d")
    assert loop_info._getCycle() == 5


def test_get_cycle_returns_start_if_no_matching_files(temp_dir):
    (temp_dir / "foo.txt").write_text("dummy")
    loop_info = _create_loop_info_stub(2, temp_dir, "md%04d")
    assert loop_info._getCycle() == 2


def test_get_cycle_selects_highest_number(temp_dir):
    (temp_dir / "md0001.xtc").write_text("x")
    (temp_dir / "md0002.csv").write_text("x")
    (temp_dir / "md0007.txt").write_text("x")
    loop_info = _create_loop_info_stub(0, temp_dir, "md%04d")
    assert loop_info._getCycle() == 7


def test_get_cycle_selects_highest_number_partial_match(temp_dir):
    (temp_dir / "md0001.xtc").write_text("x")
    (temp_dir / "md0002.csv").write_text("x")
    (temp_dir / "md0007_px.txt").write_text("x")
    loop_info = _create_loop_info_stub(0, temp_dir, "md%04d")
    assert loop_info._getCycle() == 7


def test_get_cycle_selects_highest_number_partial_match2(temp_dir):
    (temp_dir / "md0001.xtc").write_text("x")
    (temp_dir / "md0002.csv").write_text("x")
    (temp_dir / "file_md0007.txt").write_text("x")
    loop_info = _create_loop_info_stub(0, temp_dir, "md%04d")
    assert loop_info._getCycle() == 7


def test_get_cycle_files_without_digits_are_ignored(temp_dir):
    (temp_dir / "mdabcd.md").write_text("x")
    (temp_dir / "mdxxxx.txt").write_text("x")
    loop_info = _create_loop_info_stub(3, temp_dir, "md.*")
    # no numerical values in filenames; use start cycle
    assert loop_info._getCycle() == 3


def test_get_cycle_mixed_files_some_match_some_not(temp_dir):
    (temp_dir / "md0002.gro").write_text("x")
    (temp_dir / "md25.xtc").write_text("x")  # wrong stem
    (temp_dir / "md0005.mdp").write_text("x")
    loop_info = _create_loop_info_stub(0, temp_dir, "md%04d")
    assert loop_info._getCycle() == 5


def test_get_cycle_multiple_digit_sequences_in_stem(temp_dir):
    (temp_dir / "md0003extra123.tpr").write_text("x")
    loop_info = _create_loop_info_stub(0, temp_dir, "md.*")
    assert loop_info._getCycle() == 3


def test_get_cycle_start_value_is_used_as_lower_bound(temp_dir):
    (temp_dir / "md0001.xtc").write_text("x")
    loop_info = _create_loop_info_stub(5, temp_dir, "md%04d")
    assert loop_info._getCycle() == 5


def test_get_cycle_non_numeric_files_are_ignored_but_numeric_stems_count(temp_dir):
    (temp_dir / "md0010.xtc").write_text("x")
    (temp_dir / "mdxxxx.txt").write_text("x")
    loop_info = _create_loop_info_stub(0, temp_dir, "md.*")
    assert loop_info._getCycle() == 10


def test_to_command_line_basic():
    info = LoopInfo(
        start=1,
        end=10,
        archive=Path("/tmp/archive"),
        archive_format="job%04d",
    )

    assert info.toCommandLine() == [
        "--loop-start",
        "1",
        "--loop-end",
        "10",
        "--archive",
        "archive",
        "--archive-format",
        "job%04d",
    ]


def test_to_command_line_archive_name_only():
    info = LoopInfo(
        start=0,
        end=5,
        archive=Path("/very/long/path/to/myarchive"),
        archive_format="md%03d",
    )

    assert info.toCommandLine() == [
        "--loop-start",
        "0",
        "--loop-end",
        "5",
        "--archive",
        "myarchive",
        "--archive-format",
        "md%03d",
    ]
