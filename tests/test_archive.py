# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

r"""
import pytest
from qq_lib.archive import QQArchiver

class MockInfo:
    def __init__(self, archive_dir="archive", archive_format=None):
        self.archive_dir = archive_dir
        self.archive_format = archive_format

class MockInformer:
    def __init__(self, info):
        self.info = info

@pytest.mark.parametrize(
    "files, archive_format, expected_matches",
    [
        # printf-style pattern
        (["md0001.xtc", "md0001.tpr", "md0003.csv", "readme.txt"], "md%04d", ["md0001.xtc", "md0001.tpr", "md0003.csv"]),

        # regex pattern
        (["file123.txt", "file12.txt", "file999.csv", "notes.txt"], r"file\d{3}", ["file123.txt", "file999.csv"]),

        # mixed, printf-style with extensions
        (["data001.csv", "other.txt", "data10.csv", "data002.csv"], "data%03d", ["data001.csv", "data002.csv"]),

        # no matches
        (["a.txt", "b.csv"], "md%04d", []),

        # regex matching letters
        (["foo.txt", "bar.csv", "baz.doc"], r"ba.", ["bar.csv", "baz.doc"]),
    ]
)
def test_get_files_to_archive(tmp_path, files, archive_format, expected_matches):
    for fname in files:
        (tmp_path / fname).write_text("dummy")

    info = MockInfo(archive_dir=str(tmp_path), archive_format=archive_format)
    informer = MockInformer(info)
    archiver = QQArchiver(informer, direction=ArchiveDirection.TO)

    result = archiver._getFilesToArchive(tmp_path)
    result_names = [f.name for f in result]
    # order does not matter
    assert sorted(result_names) == sorted(expected_matches)
    # paths must be absolute
    for f in result:
        assert f.is_absolute()

def test_get_files_to_archive_non_recursive(tmp_path):
    # create subdirectory with matching file
    (tmp_path / "subdir").mkdir()
    (tmp_path / "file001.txt").write_text("dummy")
    (tmp_path / "subdir" / "file002.txt").write_text("dummy")

    info = MockInfo(archive_dir=str(tmp_path), archive_format="file%03d")
    archiver = QQArchiver(MockInformer(info), direction=ArchiveDirection.TO)

    result = archiver._getFilesToArchive(tmp_path)
    result_names = [f.name for f in result]
    # only the file in the top-level directory matches
    assert result_names == ["file001.txt"]

    # paths must be absolute
    for f in result:
        assert f.is_absolute()
"""
