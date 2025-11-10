# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

import pytest

from qq_lib.core.error import QQError
from qq_lib.properties.resources import Resources
from qq_lib.properties.size import Size


def test_init_converts_mem_and_storage_per_cpu_strings():
    res = Resources(mem_per_cpu="2gb", work_size_per_cpu="1gb")
    assert isinstance(res.mem_per_cpu, Size)
    assert str(res.mem_per_cpu) == "2gb"
    assert isinstance(res.work_size_per_cpu, Size)
    assert str(res.work_size_per_cpu) == "1gb"


def test_init_converts_mem_and_storage_strings():
    res = Resources(mem="4gb", work_size="10gb")
    assert isinstance(res.mem, Size)
    assert str(res.mem) == "4gb"
    assert isinstance(res.work_size, Size)
    assert str(res.work_size) == "10gb"


def test_init_converts_walltime_seconds():
    res = Resources(walltime="3600s")
    assert res.walltime == "1:00:00"


def test_init_does_not_convert_walltime_with_colon():
    res = Resources(walltime="02:30:00")
    assert res.walltime == "02:30:00"


def test_init_converts_props_string_to_dict_equal_sign():
    res = Resources(props="gpu_type=a100,property=new")
    assert res.props == {"gpu_type": "a100", "property": "new"}


def test_init_converts_props_string_to_dict_flags():
    res = Resources(props="avx512 ^smt")
    assert res.props == {"avx512": "true", "smt": "false"}


def test_init_converts_props_string_with_mixed_delimiters():
    res = Resources(props="gpu_type=a100 property=new:debug")
    assert res.props == {"gpu_type": "a100", "property": "new", "debug": "true"}


def test_init_converts_numeric_strings_to_integers():
    res = Resources(nnodes="2", ncpus="16", ngpus="4")
    assert res.nnodes == 2
    assert res.ncpus == 16
    assert res.ngpus == 4


def test_init_mem_overrides_mem_per_cpu():
    res = Resources(mem_per_cpu="1gb", mem="4gb")
    assert res.mem_per_cpu is None

    assert res.mem is not None
    assert res.mem.value == 4194304


def test_init_worksize_overrides_work_size_per_cpu():
    res = Resources(work_size_per_cpu="1gb", work_size="4gb")
    assert res.work_size_per_cpu is None

    assert res.work_size is not None
    assert res.work_size.value == 4194304


def test_init_leaves_already_converted_types_unchanged():
    res = Resources(
        nnodes=2,
        ncpus=16,
        ngpus=4,
        mem=Size.fromString("8gb"),
        mem_per_cpu=Size.fromString("2gb"),
        work_size=Size.fromString("100gb"),
        work_size_per_cpu=Size.fromString("10gb"),
        walltime="01:00:00",
        props={"gpu": "true"},
    )
    assert res.nnodes == 2
    assert res.ncpus == 16
    assert res.ngpus == 4
    assert str(res.mem) == "8gb"
    assert res.mem_per_cpu is None  # overriden by res.mem
    assert str(res.work_size) == "100gb"
    assert res.work_size_per_cpu is None  # override by res.work_size
    assert res.walltime == "01:00:00"
    assert res.props == {"gpu": "true"}


def test_merge_resources_basic_field_precedence():
    r1 = Resources(ncpus=4, work_dir="input_dir")
    r2 = Resources(ncpus=8, work_dir="scratch_local")
    merged = Resources.mergeResources(r1, r2)

    assert merged.ncpus == 4
    assert merged.work_dir == "input_dir"


def test_merge_resources_props_merging_order_and_dedup():
    r1 = Resources(props="cl_example,ssd")
    r2 = Resources(props="ssd:infiniband")
    r3 = Resources(props=None)
    merged = Resources.mergeResources(r1, r2, r3)

    assert merged.props == {"cl_example": "true", "ssd": "true", "infiniband": "true"}


def test_merge_resources_props_merging_order_and_dedup_disallowed():
    r1 = Resources(props="vnode=example_node  ^ssd")
    r2 = Resources(props="ssd,infiniband:^property")
    r3 = Resources(props=None)
    merged = Resources.mergeResources(r1, r2, r3)

    assert merged.props == {
        "vnode": "example_node",
        "ssd": "false",
        "infiniband": "true",
        "property": "false",
    }


def test_merge_resources_props_merging_order_and_dedup_disallowed2():
    r1 = Resources(props="vnode=^example_node  ssd")
    r2 = Resources(props=None)
    r3 = Resources(props="^ssd,infiniband:property")
    merged = Resources.mergeResources(r1, r2, r3)

    assert merged.props == {
        "vnode": "^example_node",
        "ssd": "true",
        "infiniband": "true",
        "property": "true",
    }


def test_merge_resources_props_none_when_no_values():
    r1 = Resources()
    r2 = Resources()
    merged = Resources.mergeResources(r1, r2)
    assert merged.props is None


def test_merge_resources_mem_with_mem_per_cpu_precedence():
    r1 = Resources(mem="16gb")
    r2 = Resources(mem="32gb", mem_per_cpu="4gb")
    r3 = Resources(mem="64gb")
    merged = Resources.mergeResources(r1, r2, r3)
    assert merged.mem is not None
    assert merged.mem.value == 16777216

    assert merged.mem_per_cpu is None


def test_merge_resources_mem_with_mem_per_cpu_precedence2():
    r1 = Resources()
    r2 = Resources(mem="32gb", mem_per_cpu="4gb")
    r3 = Resources(mem="64gb")
    merged = Resources.mergeResources(r1, r2, r3)
    assert merged.mem is not None
    assert merged.mem.value == 33554432
    assert merged.mem_per_cpu is None


def test_merge_resources_mem_with_mem_per_cpu_precedence3():
    r1 = Resources()
    r2 = Resources(mem_per_cpu="4gb")
    r3 = Resources(mem="64gb")
    merged = Resources.mergeResources(r1, r2, r3)
    assert merged.mem is None
    assert merged.mem_per_cpu is not None
    assert merged.mem_per_cpu.value == 4194304


def test_merge_resources_mem_skipped_if_mem_per_cpu_seen_first():
    r1 = Resources(mem_per_cpu="4gb")
    r2 = Resources(mem="32gb")
    merged = Resources.mergeResources(r1, r2)
    assert merged.mem is None
    assert merged.mem_per_cpu is not None
    assert merged.mem_per_cpu.value == 4194304


def test_merge_resources_work_size_with_work_size_per_cpu_precedence():
    r1 = Resources(work_size="100gb")
    r2 = Resources(work_size="200gb", work_size_per_cpu="10gb")
    r3 = Resources(work_size="300gb")
    merged = Resources.mergeResources(r1, r2, r3)
    assert merged.work_size is not None
    assert merged.work_size.value == 104857600

    assert merged.work_size_per_cpu is None


def test_merge_resources_work_size_with_work_size_per_cpu_precedence2():
    r1 = Resources()
    r2 = Resources(work_size="200gb", work_size_per_cpu="10gb")
    r3 = Resources(work_size="300gb")
    merged = Resources.mergeResources(r1, r2, r3)
    assert merged.work_size is not None
    assert merged.work_size.value == 209715200

    assert merged.work_size_per_cpu is None


def test_merge_resources_work_size_with_work_size_per_cpu_precedence3():
    r1 = Resources()
    r2 = Resources(work_size_per_cpu="10gb")
    r3 = Resources(work_size="300gb")
    merged = Resources.mergeResources(r1, r2, r3)
    assert merged.work_size is None
    assert merged.work_size_per_cpu is not None
    assert merged.work_size_per_cpu.value == 10485760


def test_merge_resources_work_size_skipped_if_work_size_per_cpu_seen_first():
    r1 = Resources(work_size_per_cpu="10gb")
    r2 = Resources(work_size="200gb")
    merged = Resources.mergeResources(r1, r2)
    assert merged.work_size is None
    assert merged.work_size_per_cpu is not None
    assert merged.work_size_per_cpu.value == 10485760


def test_merge_resources_all_fields_combined():
    r1 = Resources(
        nnodes=2,
        ncpus=4,
        mem_per_cpu="16gb",
        work_size="24gb",
        work_dir="scratch_local",
        props="gpu",
    )
    r2 = Resources(
        nnodes=None,
        ncpus=8,
        mem="32gb",
        work_size_per_cpu="1gb",
        work_dir=None,
        props="^ssd",
    )
    merged = Resources.mergeResources(r1, r2)
    assert merged.nnodes == 2
    assert merged.ncpus == 4
    assert merged.mem is None
    assert merged.mem_per_cpu is not None
    assert merged.mem_per_cpu.value == 16777216
    assert merged.work_size_per_cpu is None
    assert merged.work_size is not None
    assert merged.work_size.value == 25165824
    assert merged.work_dir == "scratch_local"
    assert merged.props == {"gpu": "true", "ssd": "false"}


def test_merge_resources_with_none_resources():
    r1 = Resources()
    r2 = Resources()
    merged = Resources.mergeResources(r1, r2)
    for f in r1.__dataclass_fields__:
        assert getattr(merged, f) is None


def test_parse_size_from_string():
    result = Resources._parseSize("4gb")
    assert isinstance(result, Size)
    assert result.value == 4194304


def test_parse_size_from_size():
    result = Resources._parseSize(Size(4, "gb"))
    assert isinstance(result, Size)
    assert result.value == 4194304


def test_parse_size_from_dict():
    data = {"value": 8, "unit": "mb"}
    result = Resources._parseSize(data)
    assert isinstance(result, Size)
    assert result.value == 8192


def test_parse_size_invalid_type_int():
    result = Resources._parseSize(123)
    assert result is None


def test_parse_size_invalid_type_none():
    result = Resources._parseSize(None)
    assert result is None


@pytest.mark.parametrize(
    "props, expected",
    [
        ("foo=bar", {"foo": "bar"}),
        ("foo=1, bar=2 baz=3", {"foo": "1", "bar": "2", "baz": "3"}),
        ("enable", {"enable": "true"}),
        ("^disable", {"disable": "false"}),
        ("foo=bar, ^baz", {"foo": "bar", "baz": "false"}),
        ("foo bar", {"foo": "true", "bar": "true"}),
        ("foo:bar:baz=42", {"foo": "true", "bar": "true", "baz": "42"}),
        ("foo   bar,baz=42", {"foo": "true", "bar": "true", "baz": "42"}),
        ("", {}),
        ("   ", {}),
    ],
)
def test_parse_props_various_cases(props, expected):
    result = Resources._parseProps(props)
    assert result == expected


def test_parse_props_strips_empty_parts():
    result = Resources._parseProps("foo,, ,bar=1")
    assert result == {"foo": "true", "bar": "1"}


@pytest.mark.parametrize(
    "props",
    [
        "foo=1 foo=2",  # duplicate with explicit values
        "foo ^foo",  # positive and negated
        "foo foo",  # repeated bare key
        "foo=1,foo=1",  # duplicate with same value
        "foo:bar:foo",  # multiple delimiters still dup
    ],
)
def test_parse_props_raises_on_duplicate_keys(props):
    with pytest.raises(QQError, match="Property 'foo' is defined multiple times."):
        Resources._parseProps(props)
