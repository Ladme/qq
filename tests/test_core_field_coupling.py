# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from dataclasses import dataclass

from qq_lib.core.field_coupling import FieldCoupling, HasCouplingMethods, coupled_fields


def test_field_coupling_init():
    coupling = FieldCoupling("foo", "bar")
    assert coupling.fields == ["foo", "bar"]


def test_field_coupling_init_many_fields():
    fields = [
        "foo",
        "bar",
        "baz",
        "qux",
        "quux",
        "corge",
        "grault",
        "garply",
        "waldo",
        "fred",
        "plugh",
        "xyzzy",
        "thud",
    ]
    coupling = FieldCoupling(*fields)
    assert coupling.fields == fields


def test_field_coupling_contains():
    coupling = FieldCoupling("foo", "bar")
    assert coupling.contains("foo") is True
    assert coupling.contains("bar") is True


def test_field_coupling_contains_with_unrelated_field():
    coupling = FieldCoupling("foo", "bar")
    assert coupling.contains("baz") is False
    assert coupling.contains("") is False


def test_field_coupling_get_fields():
    coupling = FieldCoupling("foo", "bar", "baz", "qux")
    assert coupling.getFields() == ("foo", "bar", "baz", "qux")


def test_field_coupling_has_value_with_first_set():
    @dataclass
    class MockClass:
        foo: str | None = None
        bar: str | None = None
        baz: str | None = None

    coupling = FieldCoupling("foo", "bar", "baz")
    instance = MockClass(foo="value")
    assert coupling.hasValue(instance) is True


def test_field_coupling_has_value_with_last_set():
    @dataclass
    class MockClass:
        foo: str | None = None
        bar: str | None = None
        baz: str | None = None

    coupling = FieldCoupling("foo", "bar", "baz")
    instance = MockClass(baz="value")
    assert coupling.hasValue(instance) is True


def test_field_coupling_has_value_with_two_set():
    @dataclass
    class MockClass:
        foo: str | None = None
        bar: str | None = None
        baz: str | None = None

    coupling = FieldCoupling("foo", "bar", "baz")
    instance = MockClass(foo="value1", bar="value2")
    assert coupling.hasValue(instance) is True


def test_field_coupling_has_value_with_all_set():
    @dataclass
    class MockClass:
        foo: str | None = None
        bar: str | None = None
        baz: str | None = None

    coupling = FieldCoupling("foo", "bar", "baz")
    instance = MockClass(foo="value1", bar="value2", baz="value3")
    assert coupling.hasValue(instance) is True


def test_field_coupling_has_value_with_neither_set():
    @dataclass
    class MockClass:
        foo: str | None = None
        bar: str | None = None
        baz: str | None = None

    coupling = FieldCoupling("foo", "bar", "baz")
    instance = MockClass()
    assert coupling.hasValue(instance) is False


def test_decorator_single_coupling_dominant_overrides_recessive():
    @dataclass
    @coupled_fields(FieldCoupling("alpha", "beta"))
    class TestClass(HasCouplingMethods):
        alpha: str | None = None
        beta: str | None = None

    obj = TestClass(alpha="A", beta="B")
    assert obj.alpha == "A"
    assert obj.beta is None


def test_decorator_single_coupling_dominant_overrides_recessive_three_fields():
    @dataclass
    @coupled_fields(FieldCoupling("alpha", "beta", "gamma"))
    class TestClass(HasCouplingMethods):
        alpha: str | None = None
        beta: str | None = None
        gamma: str | None = None

    obj = TestClass(alpha="A", gamma="G")
    assert obj.alpha == "A"
    assert obj.beta is None
    assert obj.gamma is None


def test_decorator_single_coupling_second_overrides_third():
    @dataclass
    @coupled_fields(FieldCoupling("alpha", "beta", "gamma"))
    class TestClass(HasCouplingMethods):
        alpha: str | None = None
        beta: str | None = None
        gamma: str | None = None

    obj = TestClass(beta="B", gamma="G")
    assert obj.alpha is None
    assert obj.beta == "B"
    assert obj.gamma is None


def test_decorator_single_coupling_recessive_preserved_when_dominant_none():
    @dataclass
    @coupled_fields(FieldCoupling("alpha", "beta"))
    class TestClass(HasCouplingMethods):
        alpha: str | None = None
        beta: str | None = None

    obj = TestClass(beta="B")
    assert obj.alpha is None
    assert obj.beta == "B"


def test_decorator_single_coupling_recessive_preserved_when_dominant_none_three_fields():
    @dataclass
    @coupled_fields(FieldCoupling("alpha", "beta", "gamma"))
    class TestClass(HasCouplingMethods):
        alpha: str | None = None
        beta: str | None = None
        gamma: str | None = None

    obj = TestClass(gamma="G")
    assert obj.alpha is None
    assert obj.beta is None
    assert obj.gamma == "G"


def test_decorator_single_coupling_both_none():
    @dataclass
    @coupled_fields(FieldCoupling("alpha", "beta"))
    class TestClass(HasCouplingMethods):
        alpha: str | None = None
        beta: str | None = None

    obj = TestClass()
    assert obj.alpha is None
    assert obj.beta is None


def test_decorator_single_coupling_all_none():
    @dataclass
    @coupled_fields(FieldCoupling("alpha", "beta", "gamma", "delta"))
    class TestClass(HasCouplingMethods):
        alpha: str | None = None
        beta: str | None = None
        gamma: str | None = None
        delta: str | None = None

    obj = TestClass()
    assert obj.alpha is None
    assert obj.beta is None
    assert obj.gamma is None
    assert obj.delta is None


def test_decorator_multiple_couplings_independent():
    @dataclass
    @coupled_fields(
        FieldCoupling("foo", "bar"),
        FieldCoupling("baz", "qux", "corge"),
    )
    class TestClass(HasCouplingMethods):
        foo: str | None = None
        bar: str | None = None
        baz: str | None = None
        qux: str | None = None
        corge: str | None = None

    obj = TestClass(foo="F", bar="B", qux="Q", corge="C")
    assert obj.foo == "F"
    assert obj.bar is None  # overridden by foo
    assert obj.baz is None
    assert obj.qux == "Q"  # preserved because baz is None
    assert obj.corge is None  # override by qux


def test_decorator_uncoupled_fields_unaffected():
    @dataclass
    @coupled_fields(FieldCoupling("foo", "bar"))
    class TestClass(HasCouplingMethods):
        foo: str | None = None
        bar: str | None = None
        uncoupled: str | None = None

    obj = TestClass(foo="F", bar="B", uncoupled="U")
    assert obj.foo == "F"
    assert obj.bar is None
    assert obj.uncoupled == "U"


def test_decorator_get_coupling_for_field():
    @dataclass
    @coupled_fields(FieldCoupling("foo", "bar"))
    class TestClass(HasCouplingMethods):
        foo: str | None = None
        bar: str | None = None

    coupling = TestClass.getCouplingForField("foo")
    assert coupling is not None
    assert coupling.fields == ["foo", "bar"]

    coupling = TestClass.getCouplingForField("bar")
    assert coupling is not None
    assert coupling.fields == ["foo", "bar"]


def test_decorator_get_coupling_for_field_returns_none_for_uncoupled():
    @dataclass
    @coupled_fields(FieldCoupling("foo", "bar"))
    class TestClass(HasCouplingMethods):
        foo: str | None = None
        bar: str | None = None
        baz: str | None = None

    coupling = TestClass.getCouplingForField("baz")
    assert coupling is None


def test_decorator_get_coupling_for_field_with_multiple_couplings():
    @dataclass
    @coupled_fields(
        FieldCoupling("foo", "bar"),
        FieldCoupling("baz", "qux", "corge"),
    )
    class TestClass(HasCouplingMethods):
        foo: str | None = None
        bar: str | None = None
        baz: str | None = None
        qux: str | None = None
        corge: str | None = None

    coupling1 = TestClass.getCouplingForField("foo")
    assert coupling1 is not None
    assert coupling1.fields == ["foo", "bar"]

    coupling2 = TestClass.getCouplingForField("corge")
    assert coupling2 is not None
    assert coupling2.fields == ["baz", "qux", "corge"]


def test_decorator_custom_post_init_preserved():
    @dataclass
    @coupled_fields(FieldCoupling("foo", "bar"))
    class TestClass(HasCouplingMethods):
        foo: str | None = None
        bar: str | None = None
        computed: str | None = None

        def __post_init__(self):
            self.computed = f"foo={self.foo},bar={self.bar}"

    obj = TestClass(foo="F", bar="B")
    assert obj.foo == "F"
    assert obj.bar is None  # coupling rule applied first
    assert obj.computed == "foo=F,bar=None"


def test_decorator_field_couplings_metadata_stored():
    coupling1 = FieldCoupling("a", "b")
    coupling2 = FieldCoupling("c", "d", "e")

    @dataclass
    @coupled_fields(coupling1, coupling2)
    class TestClass(HasCouplingMethods):
        a: str | None = None
        b: str | None = None
        c: str | None = None
        d: str | None = None
        e: str | None = None

    assert hasattr(TestClass, "_field_couplings")
    assert len(TestClass._field_couplings) == 2
    assert TestClass._field_couplings[0] is coupling1
    assert TestClass._field_couplings[1] is coupling2


def test_decorator_empty_decorator():
    @dataclass
    @coupled_fields()
    class TestClass(HasCouplingMethods):
        foo: str | None = None
        bar: str | None = None

    obj = TestClass(foo="F", bar="B")
    assert obj.foo == "F"
    assert obj.bar == "B"
    assert TestClass.getCouplingForField("foo") is None


def test_field_coupling_get_most_dominant_set_field_first():
    @dataclass
    class MockClass:
        alpha: str | None = None
        beta: str | None = None
        gamma: str | None = None

    coupling = FieldCoupling("alpha", "beta", "gamma")
    instance = MockClass(alpha="A", beta="B", gamma="C")
    assert coupling.getMostDominantSetField(instance) == "alpha"


def test_field_coupling_get_most_dominant_set_field_middle():
    @dataclass
    class MockClass:
        alpha: str | None = None
        beta: str | None = None
        gamma: str | None = None

    coupling = FieldCoupling("alpha", "beta", "gamma")
    instance = MockClass(beta="B", gamma="C")
    assert coupling.getMostDominantSetField(instance) == "beta"


def test_field_coupling_get_most_dominant_set_field_last():
    @dataclass
    class MockClass:
        alpha: str | None = None
        beta: str | None = None
        gamma: str | None = None

    coupling = FieldCoupling("alpha", "beta", "gamma")
    instance = MockClass(gamma="C")
    assert coupling.getMostDominantSetField(instance) == "gamma"


def test_field_coupling_get_most_dominant_set_field_none_set():
    @dataclass
    class MockClass:
        alpha: str | None = None
        beta: str | None = None
        gamma: str | None = None

    coupling = FieldCoupling("alpha", "beta", "gamma")
    instance = MockClass()
    assert coupling.getMostDominantSetField(instance) is None


def test_field_coupling_enforce_first_dominant_kept():
    @dataclass
    class MockClass:
        alpha: str | None = None
        beta: str | None = None
        gamma: str | None = None

    coupling = FieldCoupling("alpha", "beta", "gamma")
    instance = MockClass(alpha="A", beta="B", gamma="C")
    coupling.enforce(instance)

    assert instance.alpha == "A"
    assert instance.beta is None
    assert instance.gamma is None


def test_field_coupling_enforce_middle_dominant_kept():
    @dataclass
    class MockClass:
        alpha: str | None = None
        beta: str | None = None
        gamma: str | None = None

    coupling = FieldCoupling("alpha", "beta", "gamma")
    instance = MockClass(beta="B", gamma="C")
    coupling.enforce(instance)

    assert instance.alpha is None
    assert instance.beta == "B"
    assert instance.gamma is None


def test_field_coupling_enforce_last_dominant_kept():
    @dataclass
    class MockClass:
        alpha: str | None = None
        beta: str | None = None
        gamma: str | None = None

    coupling = FieldCoupling("alpha", "beta", "gamma")
    instance = MockClass(gamma="C")
    coupling.enforce(instance)

    assert instance.alpha is None
    assert instance.beta is None
    assert instance.gamma == "C"


def test_field_coupling_enforce_does_not_change_uncoupled_fields():
    @dataclass
    class MockClass:
        alpha: str | None = None
        beta: str | None = None
        gamma: str | None = None
        extra1: str | None = None
        extra2: str | None = None

    coupling = FieldCoupling("alpha", "beta", "gamma")

    instance = MockClass(
        alpha=None,
        beta="B",
        gamma="C",
        extra1="keep1",
        extra2="keep2",
    )

    coupling.enforce(instance)

    assert instance.alpha is None
    assert instance.beta == "B"
    assert instance.gamma is None

    # uncoupled fields must remain untouched
    assert instance.extra1 == "keep1"
    assert instance.extra2 == "keep2"
