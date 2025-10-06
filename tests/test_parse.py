# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import tempfile
from pathlib import Path

import pytest

from qq_lib.error import QQError
from qq_lib.job_type import QQJobType
from qq_lib.parse import QQParser
from qq_lib.pbs import QQPBS
from qq_lib.submit import submit


@pytest.fixture
def temp_script_file():
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp_file:
        yield tmp_file, Path(tmp_file.name)
    # cleanup after test
    tmp_file_path = Path(tmp_file.name)
    if tmp_file_path.exists():
        tmp_file_path.unlink()


def test_parse_happy_path(temp_script_file):
    tmp_file, path = temp_script_file
    tmp_file.write("""#!/usr/bin/env -S qq run
# qq batch_system=PBS
# QQ Queue   default
# qq ncpus 8
# qq   WorkDir job_dir
# qq work-size=4gb
# qq exclude file1.txt,file2.txt
# Qq    non_interactive=true
# qq loop-start 1
# qq    loop_end 10
# qq Archive    archive
# qQ   archive_format=cycle_%03d
# first non-qq line
command run here
""")
    tmp_file.flush()

    parser = QQParser(path, submit.params)
    parser.parse()

    opts = parser._options
    assert opts["batch_system"] == "PBS"
    assert opts["queue"] == "default"
    assert opts["ncpus"] == 8
    assert opts["work_dir"] == "job_dir"
    assert opts["work_size"] == "4gb"
    assert opts["exclude"] == "file1.txt,file2.txt"
    assert opts["non_interactive"] == "true"
    assert opts["loop_start"] == 1
    assert opts["loop_end"] == 10
    assert opts["archive"] == "archive"
    assert opts["archive_format"] == "cycle_%03d"


def test_parse_works_with_key_value_separator_equals(temp_script_file):
    tmp_file, path = temp_script_file
    tmp_file.write("""#!/usr/bin/env -S qq run
# qq ncpus=4
# qq workdir=scratch_ssd
# qq non_interactive=false
""")
    tmp_file.flush()

    parser = QQParser(path, submit.params)
    parser.parse()

    opts = parser._options
    assert opts["ncpus"] == 4
    assert opts["work_dir"] == "scratch_ssd"
    assert opts["non_interactive"] == "false"


def test_parse_raises_for_malformed_line(temp_script_file):
    tmp_file, path = temp_script_file
    tmp_file.write("""#!/usr/bin/env -S qq run
# qq ncpus
""")
    tmp_file.flush()

    parser = QQParser(path, submit.params)
    with pytest.raises(QQError, match="Invalid qq submit option line"):
        parser.parse()


def test_parse_raises_for_unknown_option(temp_script_file):
    tmp_file, path = temp_script_file
    tmp_file.write("""#!/usr/bin/env -S qq run
# qq unknown_option=42
""")
    tmp_file.flush()

    parser = QQParser(path, submit.params)
    with pytest.raises(QQError, match="Unknown qq submit option"):
        parser.parse()


def test_parse_stops_at_first_non_qq_line(temp_script_file):
    tmp_file, path = temp_script_file
    tmp_file.write("""#!/usr/bin/env -S qq run
# qq ncpus 8
qq command that should stop parsing
# qq workdir scratch_local
""")
    tmp_file.flush()

    parser = QQParser(path, submit.params)
    parser.parse()

    opts = parser._options
    # only ncpus should be parsed
    assert opts["ncpus"] == 8
    assert "work_dir" not in opts


def test_parse_normalizes_keys_and_integer_conversion(temp_script_file):
    tmp_file, path = temp_script_file
    tmp_file.write("""#!/usr/bin/env -S qq run
# qq WorkDir scratch_local
# qq ncpus 16
# qq Ngpus 4
# qq worksize 16gb
""")
    tmp_file.flush()

    parser = QQParser(path, submit.params)
    parser.parse()

    opts = parser._options

    assert "work_dir" in opts
    assert "work_size" in opts

    assert opts["ncpus"] == 16
    assert opts["ngpus"] == 4


def _make_parser_with_options(options: dict[str, str]) -> QQParser:
    parser = QQParser.__new__(QQParser)
    parser._options = options
    parser._known_options = {}  # not relevant
    return parser


def test_get_batch_system_specified():
    parser = _make_parser_with_options({"batch_system": "PBS"})
    batch_system = parser.getBatchSystem()
    assert batch_system is QQPBS


def test_get_batch_system_none_if_missing():
    parser = _make_parser_with_options({})
    batch_system = parser.getBatchSystem()
    assert batch_system is None


def test_get_batch_system_unknown_raises():
    parser = _make_parser_with_options({"batch_system": "FakeSystem"})
    with pytest.raises(QQError, match="No batch system registered"):
        _ = parser.getBatchSystem()


def test_get_queue():
    parser = _make_parser_with_options({"queue": "default"})
    assert parser.getQueue() == "default"

    parser = _make_parser_with_options({})
    assert parser.getQueue() is None


def test_get_loop_start():
    parser = _make_parser_with_options({"loop_start": 1})
    assert parser.getLoopStart() == 1

    parser = _make_parser_with_options({})
    assert parser.getLoopStart() is None


def test_get_loop_end():
    parser = _make_parser_with_options({"loop_end": 5})
    assert parser.getLoopEnd() == 5

    parser = _make_parser_with_options({})
    assert parser.getLoopEnd() is None


def test_get_archive():
    parser = _make_parser_with_options({"archive": "archive"})
    assert parser.getArchive() == Path("archive")

    parser = _make_parser_with_options({})
    assert parser.getArchive() is None


def test_get_archive_format():
    parser = _make_parser_with_options({"archive_format": "job%04d"})
    assert parser.getArchiveFormat() == "job%04d"

    parser = _make_parser_with_options({})
    assert parser.getArchiveFormat() is None


def test_get_job_type_standard():
    parser = _make_parser_with_options({"job_type": "STANDARD"})
    assert parser.getJobType() == QQJobType.STANDARD


def test_get_job_type_loop():
    parser = _make_parser_with_options({"job_type": "loop"})
    assert parser.getJobType() == QQJobType.LOOP


def test_get_job_type_case_insensitive():
    parser = _make_parser_with_options({"job_type": "StAnDaRd"})
    assert parser.getJobType() == QQJobType.STANDARD


def test_get_job_type_none_if_missing():
    parser = _make_parser_with_options({})
    assert parser.getJobType() is None


def test_get_job_type_invalid_raises():
    parser = _make_parser_with_options({"job_type": "invalid_type"})
    with pytest.raises(QQError, match="Could not recognize a job type"):
        parser.getJobType()


def test_get_exclude_none():
    parser = _make_parser_with_options({})
    assert parser.getExclude() == []


def test_get_exclude_empty_string():
    parser = _make_parser_with_options({"exclude": ""})
    assert parser.getExclude() == []


def test_get_exclude_single_file():
    parser = _make_parser_with_options({"exclude": "file1.txt"})
    result = parser.getExclude()
    expected = [Path("file1.txt").resolve()]
    assert result == expected


def test_get_exclude_multiple_comma_separated():
    parser = _make_parser_with_options({"exclude": "file1.txt,file2.txt"})
    result = parser.getExclude()
    expected = [Path("file1.txt").resolve(), Path("file2.txt").resolve()]
    assert result == expected


def test_get_exclude_multiple_colon_separated():
    parser = _make_parser_with_options({"exclude": "file1.txt:file2.txt"})
    result = parser.getExclude()
    expected = [Path("file1.txt").resolve(), Path("file2.txt").resolve()]
    assert result == expected


def test_get_exclude_multiple_whitespace_separated():
    parser = _make_parser_with_options(
        {"exclude": "file1.txt file2.txt\tfile3.txt\nfile4.txt"}
    )
    result = parser.getExclude()
    expected = [
        Path("file1.txt").resolve(),
        Path("file2.txt").resolve(),
        Path("file3.txt").resolve(),
        Path("file4.txt").resolve(),
    ]
    assert result == expected


def test_get_exclude_mixed_separators():
    parser = _make_parser_with_options(
        {"exclude": "file1.txt, file2.txt:file3.txt file4.txt"}
    )
    result = parser.getExclude()
    expected = [
        Path("file1.txt").resolve(),
        Path("file2.txt").resolve(),
        Path("file3.txt").resolve(),
        Path("file4.txt").resolve(),
    ]
    assert result == expected


def test_non_interactive_true():
    parser = _make_parser_with_options({"non_interactive": "true"})
    assert parser.getNonInteractive() is True


def test_non_interactive_false():
    parser = _make_parser_with_options({"non_interactive": "false"})
    assert parser.getNonInteractive() is False


def test_non_interactive_missing():
    parser = _make_parser_with_options({})
    assert parser.getNonInteractive() is False


def test_non_interactive_other_value():
    parser = _make_parser_with_options({"non_interactive": "yes"})
    assert parser.getNonInteractive() is False


def test_non_interactive_empty_string():
    parser = _make_parser_with_options({"non_interactive": ""})
    assert parser.getNonInteractive() is False


def test_qqparser_integration():
    script_content = """#!/usr/bin/env -S qq run
# Qq   BatchSystem=PBS
# qq queue  default
#qq job-type=standard
#   qq   ncpus  8
# qq workdir scratch_local
#QQ work-size=4gb
# qq exclude=file1.txt,file2.txt
# qq NonInteractive=true
#qq loop-start    2
# qq loop_end=10
#   qq archive    archive
# qq archiveFormat=cycle_%03d
# this is an example qq script
# qq ngpus 3
# the above line should not be parsed

# add a module
module add random_module
run_random_program path/to/random/script

# qq this line should definitely not be parsed
exit 0
"""

    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp_file:
        tmp_file.write(script_content)
        tmp_file_path = Path(tmp_file.name)

    parser = QQParser(tmp_file_path, submit.params)
    parser.parse()

    batch_system = parser.getBatchSystem()
    assert batch_system == QQPBS

    assert parser.getQueue() == "default"

    job_type = parser.getJobType()
    assert job_type == QQJobType.STANDARD

    resources = parser.getResources()
    assert resources.ncpus == 8
    assert resources.work_dir == "scratch_local"
    assert resources.work_size is not None
    assert resources.work_size.value == 4
    assert resources.work_size.unit == "gb"
    assert resources.ngpus is None

    exclude = parser.getExclude()
    assert exclude == [Path.cwd() / "file1.txt", Path.cwd() / "file2.txt"]

    assert parser.getNonInteractive() is True

    assert parser.getLoopStart() == 2
    assert parser.getLoopEnd() == 10

    assert parser.getArchive() == Path("archive")
    assert parser.getArchiveFormat() == "cycle_%03d"

    # we have to delete the temporary file manually
    tmp_file_path.unlink()


def test_qqparser_integration_nonexistent_script_raises():
    parser = QQParser(Path("non_existent.sh"), submit.params)
    with pytest.raises(QQError, match="Could not open"):
        parser.parse()
