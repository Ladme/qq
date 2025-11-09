# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import tempfile
from dataclasses import fields
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click_option_group import GroupedOption

from qq_lib.batch.interface.interface import BatchInterface
from qq_lib.batch.interface.meta import BatchMeta
from qq_lib.batch.pbs import PBS
from qq_lib.core.error import QQError
from qq_lib.properties.depend import Depend
from qq_lib.properties.job_type import JobType
from qq_lib.properties.resources import Resources
from qq_lib.properties.size import Size
from qq_lib.submit import submit
from qq_lib.submit.parser import QQParser

# ruff: noqa: W293


def test_qqparser_init(tmp_path):
    script = tmp_path / "script.sh"

    param1 = MagicMock(spec=GroupedOption)
    param1.name = "opt1"
    param2 = MagicMock()
    param2.name = "opt2"  # not a GroupedOption, should be ignored
    param3 = MagicMock(spec=GroupedOption)
    param3.name = "opt3"

    parser = QQParser(script, [param1, param2, param3])
    assert parser._known_options == {"opt1", "opt3"}
    assert parser._options == {}


@pytest.mark.parametrize(
    "input_line, expected",
    [
        # basic and normal cases
        ("# qq key=value", ["key", "value"]),
        ("#qq key=value", ["key", "value"]),
        ("#  qq   key=value", ["key", "value"]),
        ("# QQ key=value", ["key", "value"]),
        ("# qQ   key=value", ["key", "value"]),
        # spaces instead of equals
        ("# qq key value", ["key", "value"]),
        ("#qq key    value", ["key", "value"]),
        ("# qq   key    value", ["key", "value"]),
        # tabs
        ("# qq\tkey\tvalue", ["key", "value"]),
        # equals inside second part
        ("# qq key=value=another", ["key", "value=another"]),
        ("# qq props vnode=tyr", ["props", "vnode=tyr"]),
        # only one token
        ("# qq singleword", ["singleword"]),
        ("# qq singleword   ", ["singleword"]),
        ("# qq    key", ["key"]),
        # trailing and leading whitespace
        ("   # qq key=value   ", ["key", "value"]),
        ("\t# qq key=value\t", ["key", "value"]),
        # weird spacing between # and qq
        ("#    qq   key=value", ["key", "value"]),
        ("#qqkey=value", ["key", "value"]),
        # uppercase directive
        ("# QQ key=value", ["key", "value"]),
        ("# Qq key=value", ["key", "value"]),
        # multiple equals, split only once
        ("# qq name=John=Doe", ["name", "John=Doe"]),
        # inline comments
        ("# qq key=value # key is value", ["key", "value"]),
        ("# qq key value# key is value", ["key", "value"]),
        # empty or malformed input
        ("# qq", [""]),
        ("# qq    ", [""]),
        ("#", ["#"]),  # not matching qq → not stripped
        ("notacomment", ["notacomment"]),
        ("", [""]),
    ],
)
def test_qqparser_strip_and_split(input_line, expected):
    assert QQParser._stripAndSplit(input_line) == expected


def test_qqparser_get_depend_empty_list():
    parser = QQParser.__new__(QQParser)
    parser._options = {}

    result = parser.getDepend()
    assert result == []


def test_qqparser_get_depend_calls_multi_from_str():
    parser = QQParser.__new__(QQParser)
    parser._options = {"depend": "afterok=1234,after=2345"}

    mock_depend_list = [MagicMock(), MagicMock()]

    with patch.object(
        Depend, "multiFromStr", return_value=mock_depend_list
    ) as mock_multi:
        result = parser.getDepend()

    mock_multi.assert_called_once_with("afterok=1234,after=2345")
    assert result == mock_depend_list


def test_qqparser_get_archive_format_none():
    parser = QQParser.__new__(QQParser)
    parser._options = {}

    result = parser.getArchiveFormat()
    assert result is None


def test_qqparser_get_archive_format_value():
    parser = QQParser.__new__(QQParser)
    parser._options = {"archive_format": "job%04d"}

    result = parser.getArchiveFormat()
    assert result == "job%04d"


def test_qqparser_get_archive_none():
    parser = QQParser.__new__(QQParser)
    parser._options = {}

    result = parser.getArchive()
    assert result is None


def test_qqparser_get_archive_value():
    parser = QQParser.__new__(QQParser)
    parser._options = {"archive": "storage"}

    result = parser.getArchive()
    assert result == Path("storage")


def test_qqparser_get_loop_end_none():
    parser = QQParser.__new__(QQParser)
    parser._options = {}

    result = parser.getLoopEnd()
    assert result is None


def test_qqparser_get_loop_end_value():
    parser = QQParser.__new__(QQParser)
    parser._options = {"loop_end": 10}

    result = parser.getLoopEnd()
    assert result == 10


def test_qqparser_get_loop_start_none():
    parser = QQParser.__new__(QQParser)
    parser._options = {}

    result = parser.getLoopStart()
    assert result is None


def test_qqparser_get_loop_start_value():
    parser = QQParser.__new__(QQParser)
    parser._options = {"loop_start": 2}

    result = parser.getLoopStart()
    assert result == 2


def test_qqparser_get_account_value():
    parser = QQParser.__new__(QQParser)
    parser._options = {"account": "parser_account"}

    result = parser.getAccount()
    assert result == "parser_account"


def test_qqparser_get_account_none():
    parser = QQParser.__new__(QQParser)
    parser._options = {}

    result = parser.getAccount()
    assert result is None


def test_qqparser_get_exclude_empty_list():
    parser = QQParser.__new__(QQParser)
    parser._options = {}

    result = parser.getExclude()
    assert result == []


def test_qqparser_get_exclude_calls_split_files_list():
    parser = QQParser.__new__(QQParser)
    parser._options = {"exclude": "file1,file2"}

    mock_split_result = [Path("file1"), Path("file2")]

    with patch(
        "qq_lib.submit.parser.split_files_list", return_value=mock_split_result
    ) as mock_split:
        result = parser.getExclude()

    mock_split.assert_called_once_with("file1,file2")
    assert result == mock_split_result


def test_qqparser_get_resources_returns_empty_resources_if_no_matching_options():
    parser = QQParser.__new__(QQParser)
    parser._options = {"foo": "bar"}  # not a Resources field

    result = parser.getResources()

    assert isinstance(result, Resources)
    for f in fields(Resources):
        assert getattr(result, f.name) == f.default or getattr(result, f.name) is None


def test_qqparser_get_resources_returns_resources_with_matching_fields():
    parser = QQParser.__new__(QQParser)
    parser._options = {"ncpus": 4, "mem": "4gb", "foo": "bar"}

    result = parser.getResources()

    assert isinstance(result, Resources)
    assert getattr(result, "ncpus") == 4
    assert getattr(result, "mem") == Size(4, "gb")


def test_qqparser_get_job_type_none():
    parser = QQParser.__new__(QQParser)
    parser._options = {}

    result = parser.getJobType()
    assert result is None


def test_qqparser_get_job_type_calls_from_str():
    parser = QQParser.__new__(QQParser)
    parser._options = {"job_type": "standard"}

    mock_enum = JobType.STANDARD

    with patch.object(JobType, "fromStr", return_value=mock_enum) as mock_from_str:
        result = parser.getJobType()

    mock_from_str.assert_called_once_with("standard")
    assert result == mock_enum


def test_qqparser_get_queue_none():
    parser = QQParser.__new__(QQParser)
    parser._options = {}

    result = parser.getQueue()
    assert result is None


def test_qqparser_get_queue_value():
    parser = QQParser.__new__(QQParser)
    parser._options = {"queue": "default"}

    result = parser.getQueue()
    assert result == "default"


def test_qqparser_get_batch_system_none():
    parser = QQParser.__new__(QQParser)
    parser._options = {}

    result = parser.getBatchSystem()
    assert result is None


def test_qqparser_get_batch_system_value():
    parser = QQParser.__new__(QQParser)
    parser._options = {"batch_system": "PBS"}

    mock_class = MagicMock(spec=BatchInterface)

    with patch.object(BatchMeta, "fromStr", return_value=mock_class) as mock_from_str:
        result = parser.getBatchSystem()

    mock_from_str.assert_called_once_with("PBS")
    assert result == mock_class


@pytest.fixture
def temp_script_file():
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp_file:
        yield tmp_file, Path(tmp_file.name)
    # cleanup after test
    tmp_file_path = Path(tmp_file.name)
    if tmp_file_path.exists():
        tmp_file_path.unlink()


def test_qqparser_parse_integration_happy_path(temp_script_file):
    tmp_file, path = temp_script_file
    tmp_file.write("""#!/usr/bin/env -S qq run

# qq batch_system=PBS
# QQ Queue   default
# qq ncpus 8
# qq   WorkDir job_dir
        # qq work-size=4gb
# this is a commented - should be ignored
# qq exclude file1.txt,file2.txt
# Qq    mem-per-cpu=1gb
# qq loop-start 1

# qq    loop_end 10
# qq Archive    archive
# qQ   archive_format=cycle_%03d
# qq props=vnode=my_node
command run here # parsing ends here
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
    assert opts["loop_start"] == 1
    assert opts["loop_end"] == 10
    assert opts["archive"] == "archive"
    assert opts["archive_format"] == "cycle_%03d"
    assert opts["props"] == "vnode=my_node"
    assert opts["mem_per_cpu"] == "1gb"


def test_qqparser_parse_integration_works_with_key_value_separator_equals(
    temp_script_file,
):
    tmp_file, path = temp_script_file
    tmp_file.write("""#!/usr/bin/env -S qq run
# qq ncpus=4
# qq workdir=scratch_ssd
""")
    tmp_file.flush()

    parser = QQParser(path, submit.params)
    parser.parse()

    opts = parser._options
    assert opts["ncpus"] == 4
    assert opts["work_dir"] == "scratch_ssd"


def test_qqparser_parse_integration_raises_for_malformed_line(temp_script_file):
    tmp_file, path = temp_script_file
    tmp_file.write("""#!/usr/bin/env -S qq run
# qq ncpus
""")
    tmp_file.flush()

    parser = QQParser(path, submit.params)
    with pytest.raises(QQError, match="Invalid qq submit option line"):
        parser.parse()


def test_qqparser_parse_integration_raises_for_unknown_option(temp_script_file):
    tmp_file, path = temp_script_file
    tmp_file.write("""#!/usr/bin/env -S qq run
# qq unknown_option=42
""")
    tmp_file.flush()

    parser = QQParser(path, submit.params)
    with pytest.raises(QQError, match="Unknown qq submit option"):
        parser.parse()


def test_qqparser_parse_integration_stops_at_first_non_qq_line(temp_script_file):
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


def test_qqparser_parse_integration_skips_empty_lines(temp_script_file):
    tmp_file, path = temp_script_file
    tmp_file.write("""#!/usr/bin/env -S qq run
# qq ncpus 8

# qq workdir scratch_local
""")
    tmp_file.flush()

    parser = QQParser(path, submit.params)
    parser.parse()

    opts = parser._options
    assert opts["ncpus"] == 8
    assert opts["work_dir"] == "scratch_local"


def test_qqparser_parse_integration_skips_empty_lines_at_start(temp_script_file):
    tmp_file, path = temp_script_file
    tmp_file.write("""#!/usr/bin/env -S qq run
  

    


# qq ncpus 8

# qq workdir scratch_local
""")
    tmp_file.flush()

    parser = QQParser(path, submit.params)
    parser.parse()

    opts = parser._options
    assert opts["ncpus"] == 8
    assert opts["work_dir"] == "scratch_local"


def test_qqparser_parse_integration_skips_commented_lines(temp_script_file):
    tmp_file, path = temp_script_file
    tmp_file.write("""#!/usr/bin/env -S qq run
# qq ncpus 8
# comments are allowed
# qq workdir scratch_local
""")
    tmp_file.flush()

    parser = QQParser(path, submit.params)
    parser.parse()

    opts = parser._options
    assert opts["ncpus"] == 8
    assert opts["work_dir"] == "scratch_local"


def test_qqparser_parse_integration_commented_out_qq_command(temp_script_file):
    tmp_file, path = temp_script_file
    tmp_file.write("""#!/usr/bin/env -S qq run
# qq ncpus 8
## qq workdir scratch_local
""")
    tmp_file.flush()

    parser = QQParser(path, submit.params)
    parser.parse()

    opts = parser._options
    assert opts["ncpus"] == 8
    assert "work_dir" not in opts


def test_qqparser_parse_integration_normalizes_keys_and_integer_conversion(
    temp_script_file,
):
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


def test_qqparser_parse_integration_inline_comments_are_ignored(temp_script_file):
    tmp_file, path = temp_script_file
    tmp_file.write("""#!/usr/bin/env -S qq run
# qq ncpus 8  # inline comments are also allowed
# qq workdir scratch_local
""")
    tmp_file.flush()

    parser = QQParser(path, submit.params)
    parser.parse()

    opts = parser._options
    assert opts["ncpus"] == 8
    assert opts["work_dir"] == "scratch_local"


def test_qqparser_parse_integration_no_qq_lines(temp_script_file):
    tmp_file, path = temp_script_file
    tmp_file.write("""#!/usr/bin/env -S qq run
random_command
another_random_command

""")
    tmp_file.flush()

    parser = QQParser(path, submit.params)
    parser.parse()

    assert parser._options == {}


def test_qqparser_integration():
    script_content = """#!/usr/bin/env -S qq run
# Qq   BatchSystem=PBS
# qq queue  default
#  qQ account= fake-account

#qq job-type=standard # comments can be here as well
#   qq   ncpus  8
   # comment
# qq workdir scratch_local
#QQ work-size=4gb
# qq exclude=file1.txt,file2.txt
#               qq props=vnode=node
# parsing continues here
#qq loop-start    2
# qq loop_end=10



#   qq archive    archive
# qq archiveFormat=cycle_%03d

# add a module
module add random_module
run_random_program path/to/random/script
# qq ngpus 3
# the above line should not be parsed
   
# qq this line should definitely not be parsed
# qq mem 16gb
exit 0
"""

    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp_file:
        tmp_file.write(script_content)
        tmp_file_path = Path(tmp_file.name)

    parser = QQParser(tmp_file_path, submit.params)
    parser.parse()

    batch_system = parser.getBatchSystem()
    assert batch_system == PBS

    assert parser.getQueue() == "default"

    job_type = parser.getJobType()
    assert job_type == JobType.STANDARD

    resources = parser.getResources()
    assert resources.ncpus == 8
    assert resources.work_dir == "scratch_local"
    assert resources.work_size is not None
    assert resources.work_size.value == 4194304
    assert resources.ngpus is None
    assert resources.mem is None
    assert resources.props == {"vnode": "node"}

    exclude = parser.getExclude()
    assert exclude == [Path.cwd() / "file1.txt", Path.cwd() / "file2.txt"]

    assert parser.getLoopStart() == 2
    assert parser.getLoopEnd() == 10

    assert parser.getArchive() == Path("archive")
    assert parser.getArchiveFormat() == "cycle_%03d"
    assert parser.getAccount() == "fake-account"

    # we have to delete the temporary file manually
    tmp_file_path.unlink()


def test_qqparser_integration_nonexistent_script_raises():
    parser = QQParser(Path("non_existent.sh"), submit.params)
    with pytest.raises(QQError, match="Could not open"):
        parser.parse()
