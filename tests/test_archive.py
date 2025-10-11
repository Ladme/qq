# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import re
import socket
from pathlib import Path

import pytest

from qq_lib.archive.archiver import QQArchiver
from qq_lib.batch.pbs import QQPBS
from qq_lib.core.constants import QQ_SUFFIXES, SHARED_SUBMIT


def test_remove_files(tmp_path):
    files = []
    for i in range(3):
        f = tmp_path / f"file{i}.txt"
        f.write_text("test")
        files.append(f)

    QQArchiver._removeFiles(files)

    for f in files:
        assert not f.exists()


def test_remove_files_raises(tmp_path):
    f = tmp_path / "file.txt"
    f.write_text("test")

    f.unlink()

    with pytest.raises(OSError):
        QQArchiver._removeFiles([f])


def test_remove_files_empty_list():
    QQArchiver._removeFiles([])


def test_prepare_regex_pattern_printf():
    pattern = "md%04d"
    regex = QQArchiver._prepare_regex_pattern(pattern)

    assert isinstance(regex, re.Pattern)

    assert regex.fullmatch("md0001")
    assert regex.fullmatch("md1234")

    assert not regex.fullmatch("md12")
    assert not regex.fullmatch("mdabcd")


def test_prepare_regex_pattern_simple_regex():
    pattern = "file\\d{3}"
    regex = QQArchiver._prepare_regex_pattern(pattern)

    assert regex.pattern.startswith("^")
    assert regex.pattern.endswith("$")

    assert regex.fullmatch("file123")
    assert not regex.fullmatch("afile123")
    assert not regex.fullmatch("file1234")


def test_prepare_regex_pattern_already_anchored():
    pattern = "^abc\\d+$"
    regex = QQArchiver._prepare_regex_pattern(pattern)

    assert regex.pattern == pattern
    assert regex.fullmatch("abc123")
    assert not regex.fullmatch("xabc123")


@pytest.fixture
def input_dir(tmp_path):
    (tmp_path / "job").mkdir()
    return tmp_path / "job"


@pytest.fixture
def archive_dir(input_dir):
    return input_dir / "archive"


@pytest.fixture
def work_dir(tmp_path):
    (tmp_path / "work").mkdir()
    return tmp_path / "work"


def test_make_archive_dir_creates_directory(monkeypatch, archive_dir, input_dir):
    monkeypatch.setenv(SHARED_SUBMIT, "true")

    archiver = QQArchiver(
        archive=archive_dir,
        archive_format="job%04d",
        input_machine="fake_host",
        input_dir=input_dir,
        batch_system=QQPBS,
    )

    assert not archive_dir.exists()
    archiver.makeArchiveDir()
    assert archive_dir.exists()


def test_make_archive_dir_already_exists(monkeypatch, archive_dir, input_dir):
    monkeypatch.setenv(SHARED_SUBMIT, "true")

    # pre-create the archive directory
    archive_dir.mkdir(parents=True)

    archiver = QQArchiver(
        archive=archive_dir,
        archive_format="job%04d",
        input_machine="fake_host",
        input_dir=input_dir,
        batch_system=QQPBS,
    )

    archiver.makeArchiveDir()
    assert archive_dir.exists()
    assert archive_dir.is_dir()


@pytest.fixture
def archiver(input_dir, archive_dir):
    return QQArchiver(
        archive=archive_dir,
        archive_format="job%04d",
        input_machine="fake_host",
        input_dir=input_dir,
        batch_system=QQPBS,
    )


def touch_files(directory: Path, filenames):
    for f in filenames:
        (directory / f).touch()


HOSTS = [None, "fake_host", socket.gethostname()]


@pytest.mark.parametrize("host", HOSTS)
def test_get_files_printf_pattern_with_cycle(monkeypatch, archiver, input_dir, host):
    monkeypatch.setenv(SHARED_SUBMIT, "true")
    filenames = ["job0001.dat", "job0002.dat", "job0001.qqout", "job0001.err"]
    touch_files(input_dir, filenames)

    result = archiver._getFiles(input_dir, host=host, pattern="job%04d", cycle=1)
    expected = [input_dir / "job0001.dat"]
    assert set(result) == {f.resolve() for f in expected}


@pytest.mark.parametrize("host", HOSTS)
def test_get_files_include_qq_files(monkeypatch, archiver, input_dir, host):
    monkeypatch.setenv(SHARED_SUBMIT, "true")
    filenames = ["job0001.dat"] + [f"job0001{ext}" for ext in QQ_SUFFIXES]
    touch_files(input_dir, filenames)

    # include_qq_files=False: QQ_SUFFIXES filtered out
    result = archiver._getFiles(
        input_dir, host=host, pattern="job%04d", cycle=1, include_qq_files=False
    )
    expected = [input_dir / "job0001.dat"]
    assert set(result) == {f.resolve() for f in expected}

    # include_qq_files=True: QQ_SUFFIXES included
    result2 = archiver._getFiles(
        input_dir, host=host, pattern="job%04d", cycle=1, include_qq_files=True
    )
    expected2 = [input_dir / f for f in filenames]
    assert set(result2) == {f.resolve() for f in expected2}


@pytest.mark.parametrize("host", HOSTS)
def test_get_files_cycle_not_matching(monkeypatch, archiver, input_dir, host):
    monkeypatch.setenv(SHARED_SUBMIT, "true")
    filenames = ["job0001.dat", "job0002.dat"]
    touch_files(input_dir, filenames)

    result = archiver._getFiles(input_dir, host=host, pattern="job%04d", cycle=3)
    assert result == []


@pytest.mark.parametrize("host", HOSTS)
def test_get_files_regex_pattern(monkeypatch, archiver, input_dir, host):
    monkeypatch.setenv(SHARED_SUBMIT, "true")
    filenames = ["data_01.txt", "data_02.txt", "job0001.dat"]
    touch_files(input_dir, filenames)

    result = archiver._getFiles(input_dir, host=host, pattern=r"data_\d\d")
    expected = [input_dir / "data_01.txt", input_dir / "data_02.txt"]
    assert set(result) == {f.resolve() for f in expected}


@pytest.mark.parametrize("host", HOSTS)
def test_get_files_regex_pattern_with_cycle(monkeypatch, archiver, input_dir, host):
    monkeypatch.setenv(SHARED_SUBMIT, "true")
    filenames = ["data_01.txt", "data_02.txt", "job0001.dat"]
    touch_files(input_dir, filenames)

    result = archiver._getFiles(input_dir, host=host, pattern=r"data_\d\d", cycle=2)
    expected = [input_dir / "data_01.txt", input_dir / "data_02.txt"]
    assert set(result) == {f.resolve() for f in expected}


@pytest.mark.parametrize("host", HOSTS)
def test_get_files_printf_pattern_without_cycle(monkeypatch, archiver, input_dir, host):
    monkeypatch.setenv(SHARED_SUBMIT, "true")

    filenames = ["job0001.dat", "job0002.dat", "job0003.qqout", "job4.dat"]
    for f in filenames:
        (input_dir / f).touch()

    result = archiver._getFiles(input_dir, host=host, pattern="job%04d", cycle=None)
    expected = [input_dir / "job0001.dat", input_dir / "job0002.dat"]
    assert set(result) == {f.resolve() for f in expected}

    result = archiver._getFiles(
        input_dir, host=host, pattern="job%04d", cycle=None, include_qq_files=True
    )
    expected = [
        input_dir / "job0001.dat",
        input_dir / "job0002.dat",
        input_dir / "job0003.qqout",
    ]
    assert set(result) == {f.resolve() for f in expected}


@pytest.mark.parametrize("cycle", [None, 1])
def test_archive_from_copies_files(monkeypatch, archiver, archive_dir, work_dir, cycle):
    monkeypatch.setenv(SHARED_SUBMIT, "true")
    archiver.makeArchiveDir()

    filenames = ["job0001.dat", "job0002.dat", "other.txt", "job0001.qqinfo"]
    touch_files(archive_dir, filenames)

    archiver.archiveFrom(work_dir, cycle=cycle)

    if cycle == 1:
        expected_files = [archive_dir / "job0001.dat"]
    else:
        expected_files = [archive_dir / "job0001.dat", archive_dir / "job0002.dat"]

    for f in expected_files:
        copied_file = work_dir / f.name
        assert copied_file.exists()
        assert copied_file.is_file()

        # files still exist in the archive
        assert f.exists()
        assert f.is_file()

    # files not matching pattern should not be copied
    assert not (work_dir / "other.txt").exists()


def test_archive_from_nothing_to_fetch(monkeypatch, archiver, work_dir):
    monkeypatch.setenv(SHARED_SUBMIT, "true")
    archiver.makeArchiveDir()

    # archive directory is empty
    archiver.archiveFrom(work_dir)

    # work directory should remain empty
    assert list(work_dir.iterdir()) == []


def test_archive_to_copies_and_removes_files(
    monkeypatch, archiver, archive_dir, work_dir
):
    monkeypatch.setenv(SHARED_SUBMIT, "true")
    archiver.makeArchiveDir()

    filenames = [
        "job0001.dat",
        "job0002.dat",
        "other.txt",
        "job0001.out",
        "job0002.err",
    ]
    touch_files(work_dir, filenames)

    archiver.archiveTo(work_dir)

    expected_copied = [archive_dir / "job0001.dat", archive_dir / "job0002.dat"]
    for f in expected_copied:
        assert f.exists() and f.is_file()

    # matching files should be removed from work_dir
    assert not (work_dir / "job0001.dat").exists()
    assert not (work_dir / "job0002.dat").exists()

    # non-matching files should remain
    assert (work_dir / "other.txt").exists()

    # qq runtime files should remain as well
    assert (work_dir / "job0001.out").exists()
    assert (work_dir / "job0002.err").exists()


def test_archive_to_nothing_to_archive(monkeypatch, archiver, archive_dir, work_dir):
    monkeypatch.setenv(SHARED_SUBMIT, "true")
    archiver.makeArchiveDir()

    # work dir is empty
    archiver.archiveTo(work_dir)

    # archive remains empty
    assert list(archive_dir.iterdir()) == []

    # work dir remains empty
    assert list(work_dir.iterdir()) == []


def test_archive_to_qq_suffix_files(monkeypatch, archiver, archive_dir, work_dir):
    monkeypatch.setenv(SHARED_SUBMIT, "true")
    archiver.makeArchiveDir()

    filenames = ["job0001.dat"] + [f"job0001{ext}" for ext in QQ_SUFFIXES]
    touch_files(work_dir, filenames)

    archiver.archiveTo(work_dir)

    # only non-QQ_SUFFIXES should be archived
    expected_copied = [archive_dir / "job0001.dat"]
    for f in expected_copied:
        assert f.exists()

    # non-QQ_SUFFIXES should be removed from work_dir
    assert not (work_dir / "job0001.dat").exists()

    # QQ_SUFFIXES remain in work_dir
    for ext in QQ_SUFFIXES:
        assert (work_dir / f"job0001{ext}").exists()


def test_archive_runtime_files_moves_and_renames(
    monkeypatch, archiver, input_dir, archive_dir
):
    monkeypatch.setenv(SHARED_SUBMIT, "true")
    archiver.makeArchiveDir()

    filenames = [f"script+0005{ext}" for ext in QQ_SUFFIXES] + ["other.txt"]
    touch_files(input_dir, filenames)

    archiver.archiveRunTimeFiles("script\\+0005", 5)

    # check moved files exist in archive with renamed pattern
    expected_files = [archive_dir / f"job0005{ext}" for ext in QQ_SUFFIXES]
    for f in expected_files:
        assert f.exists() and f.is_file()

    # original runtime files removed from input_dir
    for f in [input_dir / f"script+0005{ext}" for ext in QQ_SUFFIXES]:
        assert not f.exists()

    # non-matching files remain
    assert (input_dir / "other.txt").exists()

    # non-matching files are not present in the archive
    assert not (archive_dir / "other.txt").exists()


def test_archive_runtime_files_nothing_to_archive(
    monkeypatch, archiver, input_dir, archive_dir
):
    monkeypatch.setenv(SHARED_SUBMIT, "true")
    archiver.makeArchiveDir()

    # no relevant runtime files in input_dir
    touch_files(input_dir, ["other.txt", "script+0006.qqinfo", "script+0004.qqout"])

    archiver.archiveRunTimeFiles("script\\+0005", 5)

    # archive dir should remain empty
    assert list(archive_dir.iterdir()) == []

    # input_dir files remain
    assert (input_dir / "other.txt").exists()
    assert (input_dir / "script+0006.qqinfo").exists()
    assert (input_dir / "script+0004.qqout").exists()

    # job dir files not present in archive
    assert not (archive_dir / "other.txt").exists()
    assert not (archive_dir / "script+0006.qqinfo").exists()
    assert not (archive_dir / "script+0004.qqout").exists()
