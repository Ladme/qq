# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab

from qq_lib.resources import QQResources
from qq_lib.size import Size


def test_init_converts_mem_and_storage_strings():
    res = QQResources(
        mem="4gb", mem_per_cpu="2gb", work_size="10gb", work_size_per_cpu="1gb"
    )
    assert isinstance(res.mem, Size)
    assert str(res.mem) == "4gb"
    assert isinstance(res.mem_per_cpu, Size)
    assert str(res.mem_per_cpu) == "2gb"
    assert isinstance(res.work_size, Size)
    assert str(res.work_size) == "10gb"
    assert isinstance(res.work_size_per_cpu, Size)
    assert str(res.work_size_per_cpu) == "1gb"


def test_init_converts_walltime_seconds():
    res = QQResources(walltime="3600s")
    assert res.walltime == "1:00:00"


def test_init_does_not_convert_walltime_with_colon():
    res = QQResources(walltime="02:30:00")
    assert res.walltime == "02:30:00"


def test_init_converts_props_string_to_dict_equal_sign():
    res = QQResources(props="gpu_type=a100,property=new")
    assert res.props == {"gpu_type": "a100", "property": "new"}


def test_init_converts_props_string_to_dict_flags():
    res = QQResources(props="avx512 ^smt")
    assert res.props == {"avx512": "true", "smt": "false"}


def test_init_converts_props_string_with_mixed_delimiters():
    res = QQResources(props="gpu_type=a100 property=new:debug")
    assert res.props == {"gpu_type": "a100", "property": "new", "debug": "true"}


def test_init_converts_numeric_strings_to_integers():
    res = QQResources(nnodes="2", ncpus="16", ngpus="4")
    assert res.nnodes == 2
    assert res.ncpus == 16
    assert res.ngpus == 4


def test_init_leaves_already_converted_types_unchanged():
    res = QQResources(
        nnodes=2,
        ncpus=16,
        ngpus=4,
        mem=Size.from_string("8gb"),
        mem_per_cpu=Size.from_string("2gb"),
        work_size=Size.from_string("100gb"),
        work_size_per_cpu=Size.from_string("10gb"),
        walltime="01:00:00",
        props={"gpu": "true"},
    )
    assert res.nnodes == 2
    assert res.ncpus == 16
    assert res.ngpus == 4
    assert str(res.mem) == "8gb"
    assert str(res.mem_per_cpu) == "2gb"
    assert str(res.work_size) == "100gb"
    assert str(res.work_size_per_cpu) == "10gb"
    assert res.walltime == "01:00:00"
    assert res.props == {"gpu": "true"}


def test_merge_resources_basic_field_precedence():
    r1 = QQResources(ncpus=4, work_dir="job_dir")
    r2 = QQResources(ncpus=8, work_dir="scratch_local")
    merged = QQResources.mergeResources(r1, r2)

    assert merged.ncpus == 4
    assert merged.work_dir == "job_dir"


def test_merge_resources_props_merging_order_and_dedup():
    r1 = QQResources(props="cl_example,ssd")
    r2 = QQResources(props="ssd:infiniband")
    r3 = QQResources(props=None)
    merged = QQResources.mergeResources(r1, r2, r3)

    assert merged.props == {"cl_example": "true", "ssd": "true", "infiniband": "true"}


def test_merge_resources_props_merging_order_and_dedup_disallowed():
    r1 = QQResources(props="vnode=example_node  ^ssd")
    r2 = QQResources(props="ssd,infiniband:^property")
    r3 = QQResources(props=None)
    merged = QQResources.mergeResources(r1, r2, r3)

    assert merged.props == {
        "vnode": "example_node",
        "ssd": "false",
        "infiniband": "true",
        "property": "false",
    }


def test_merge_resources_props_merging_order_and_dedup_disallowed2():
    r1 = QQResources(props="vnode=^example_node  ssd")
    r2 = QQResources(props=None)
    r3 = QQResources(props="^ssd,infiniband:property")
    merged = QQResources.mergeResources(r1, r2, r3)

    assert merged.props == {
        "vnode": "^example_node",
        "ssd": "true",
        "infiniband": "true",
        "property": "true",
    }


def test_merge_resources_props_none_when_no_values():
    r1 = QQResources()
    r2 = QQResources()
    merged = QQResources.mergeResources(r1, r2)
    assert merged.props is None


def test_merge_resources_mem_with_mem_per_cpu_precedence():
    r1 = QQResources(mem="16gb")
    r2 = QQResources(mem="32gb", mem_per_cpu="4gb")
    r3 = QQResources(mem="64gb")
    merged = QQResources.mergeResources(r1, r2, r3)
    assert merged.mem is not None
    assert merged.mem.value == 16
    assert merged.mem.unit == "gb"

    assert merged.mem_per_cpu is not None
    assert merged.mem_per_cpu.value == 4
    assert merged.mem_per_cpu.unit == "gb"


def test_merge_resources_mem_with_mem_per_cpu_precedence2():
    r1 = QQResources()
    r2 = QQResources(mem="32gb", mem_per_cpu="4gb")
    r3 = QQResources(mem="64gb")
    merged = QQResources.mergeResources(r1, r2, r3)
    assert merged.mem is None
    assert merged.mem_per_cpu is not None
    assert merged.mem_per_cpu.value == 4
    assert merged.mem_per_cpu.unit == "gb"


def test_merge_resources_mem_with_mem_per_cpu_precedence3():
    r1 = QQResources()
    r2 = QQResources(mem_per_cpu="4gb")
    r3 = QQResources(mem="64gb")
    merged = QQResources.mergeResources(r1, r2, r3)
    assert merged.mem is None
    assert merged.mem_per_cpu is not None
    assert merged.mem_per_cpu.value == 4
    assert merged.mem_per_cpu.unit == "gb"


def test_merge_resources_mem_skipped_if_mem_per_cpu_seen_first():
    r1 = QQResources(mem_per_cpu="4gb", mem="16gb")
    r2 = QQResources(mem="32gb")
    merged = QQResources.mergeResources(r1, r2)
    assert merged.mem is None
    assert merged.mem_per_cpu is not None
    assert merged.mem_per_cpu.value == 4
    assert merged.mem_per_cpu.unit == "gb"


def test_merge_resources_work_size_with_work_size_per_cpu_precedence():
    r1 = QQResources(work_size="100gb")
    r2 = QQResources(work_size="200gb", work_size_per_cpu="10gb")
    r3 = QQResources(work_size="300gb")
    merged = QQResources.mergeResources(r1, r2, r3)
    assert merged.work_size is not None
    assert merged.work_size.value == 100
    assert merged.work_size.unit == "gb"

    assert merged.work_size_per_cpu is not None
    assert merged.work_size_per_cpu.value == 10
    assert merged.work_size_per_cpu.unit == "gb"


def test_merge_resources_work_size_with_work_size_per_cpu_precedence2():
    r1 = QQResources()
    r2 = QQResources(work_size="200gb", work_size_per_cpu="10gb")
    r3 = QQResources(work_size="300gb")
    merged = QQResources.mergeResources(r1, r2, r3)
    assert merged.work_size is None
    assert merged.work_size_per_cpu is not None
    assert merged.work_size_per_cpu.value == 10
    assert merged.work_size_per_cpu.unit == "gb"


def test_merge_resources_work_size_with_work_size_per_cpu_precedence3():
    r1 = QQResources()
    r2 = QQResources(work_size_per_cpu="10gb")
    r3 = QQResources(work_size="300gb")
    merged = QQResources.mergeResources(r1, r2, r3)
    assert merged.work_size is None
    assert merged.work_size_per_cpu is not None
    assert merged.work_size_per_cpu.value == 10
    assert merged.work_size_per_cpu.unit == "gb"


def test_merge_resources_work_size_skipped_if_work_size_per_cpu_seen_first():
    r1 = QQResources(work_size_per_cpu="10gb", work_size="50gb")
    r2 = QQResources(work_size="200gb")
    merged = QQResources.mergeResources(r1, r2)
    assert merged.work_size is None
    assert merged.work_size_per_cpu is not None
    assert merged.work_size_per_cpu.value == 10
    assert merged.work_size_per_cpu.unit == "gb"


def test_merge_resources_all_fields_combined():
    r1 = QQResources(
        nnodes=2,
        ncpus=4,
        mem="16gb",
        mem_per_cpu=None,
        work_dir="scratch_local",
        props="gpu",
    )
    r2 = QQResources(
        nnodes=None, ncpus=8, mem="32gb", mem_per_cpu="8gb", work_dir=None, props="^ssd"
    )
    merged = QQResources.mergeResources(r1, r2)
    assert merged.nnodes == 2
    assert merged.ncpus == 4
    assert merged.mem is not None
    assert merged.mem.value == 16
    assert merged.mem.unit == "gb"
    assert merged.mem_per_cpu is not None
    assert merged.mem_per_cpu.value == 8
    assert merged.mem_per_cpu.unit == "gb"
    assert merged.work_dir == "scratch_local"
    assert merged.props == {"gpu": "true", "ssd": "false"}


def test_merge_resources_with_none_resources():
    r1 = QQResources()
    r2 = QQResources()
    merged = QQResources.mergeResources(r1, r2)
    for f in r1.__dataclass_fields__:
        assert getattr(merged, f) is None


def test_parse_size_from_string():
    result = QQResources._parse_size("4gb")
    assert isinstance(result, Size)
    assert result.value == 4
    assert result.unit == "gb"


def test_parse_size_from_size():
    result = QQResources._parse_size(Size(4, "gb"))
    assert isinstance(result, Size)
    assert result.value == 4
    assert result.unit == "gb"


def test_parse_size_from_dict():
    data = {"value": 8, "unit": "mb"}
    result = QQResources._parse_size(data)
    assert isinstance(result, Size)
    assert result.value == 8
    assert result.unit == "mb"


def test_parse_size_invalid_type_int():
    result = QQResources._parse_size(123)
    assert result is None


def test_parse_size_invalid_type_none():
    result = QQResources._parse_size(None)
    assert result is None
