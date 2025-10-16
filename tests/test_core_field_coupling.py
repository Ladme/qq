# Released under MIT License.
# Copyright (c) 2025 Ladislav Bartos and Robert Vacha Lab


from dataclasses import dataclass

from qq_lib.core.field_coupling import FieldCoupling, HasCouplingMethods, coupled_fields


def test_field_coupling_init():
    coupling = FieldCoupling(dominant="foo", recessive="bar")
    assert coupling.dominant == "foo"
    assert coupling.recessive == "bar"


def test_field_coupling_contains_with_dominant_field():
    coupling = FieldCoupling(dominant="foo", recessive="bar")
    assert coupling.contains("foo") is True


def test_field_coupling_contains_with_recessive_field():
    coupling = FieldCoupling(dominant="foo", recessive="bar")
    assert coupling.contains("bar") is True


def test_field_coupling_contains_with_unrelated_field():
    coupling = FieldCoupling(dominant="foo", recessive="bar")
    assert coupling.contains("baz") is False
    assert coupling.contains("") is False


def test_field_coupling_get_pair():
    coupling = FieldCoupling(dominant="foo", recessive="bar")
    assert coupling.getPair() == ("foo", "bar")


def test_field_coupling_has_value_with_dominant_set():
    @dataclass
    class MockClass:
        foo: str | None = None
        bar: str | None = None

    coupling = FieldCoupling(dominant="foo", recessive="bar")
    instance = MockClass(foo="value")
    assert coupling.hasValue(instance) is True


def test_field_coupling_has_value_with_recessive_set():
    @dataclass
    class MockClass:
        foo: str | None = None
        bar: str | None = None

    coupling = FieldCoupling(dominant="foo", recessive="bar")
    instance = MockClass(bar="value")
    assert coupling.hasValue(instance) is True


def test_field_coupling_has_value_with_both_set():
    @dataclass
    class MockClass:
        foo: str | None = None
        bar: str | None = None

    coupling = FieldCoupling(dominant="foo", recessive="bar")
    instance = MockClass(foo="value1", bar="value2")
    assert coupling.hasValue(instance) is True


def test_field_coupling_has_value_with_neither_set():
    @dataclass
    class MockClass:
        foo: str | None = None
        bar: str | None = None

    coupling = FieldCoupling(dominant="foo", recessive="bar")
    instance = MockClass()
    assert coupling.hasValue(instance) is False


def test_decorator_single_coupling_dominant_overrides_recessive():
    @dataclass
    @coupled_fields(FieldCoupling(dominant="alpha", recessive="beta"))
    class TestClass(HasCouplingMethods):
        alpha: str | None = None
        beta: str | None = None

    obj = TestClass(alpha="A", beta="B")
    assert obj.alpha == "A"
    assert obj.beta is None


def test_decorator_single_coupling_recessive_preserved_when_dominant_none():
    @dataclass
    @coupled_fields(FieldCoupling(dominant="alpha", recessive="beta"))
    class TestClass(HasCouplingMethods):
        alpha: str | None = None
        beta: str | None = None

    obj = TestClass(beta="B")
    assert obj.alpha is None
    assert obj.beta == "B"


def test_decorator_single_coupling_both_none():
    @dataclass
    @coupled_fields(FieldCoupling(dominant="alpha", recessive="beta"))
    class TestClass(HasCouplingMethods):
        alpha: str | None = None
        beta: str | None = None

    obj = TestClass()
    assert obj.alpha is None
    assert obj.beta is None


def test_decorator_multiple_couplings_independent():
    @dataclass
    @coupled_fields(
        FieldCoupling(dominant="foo", recessive="bar"),
        FieldCoupling(dominant="baz", recessive="qux"),
    )
    class TestClass(HasCouplingMethods):
        foo: str | None = None
        bar: str | None = None
        baz: str | None = None
        qux: str | None = None

    obj = TestClass(foo="F", bar="B", qux="Q")
    assert obj.foo == "F"
    assert obj.bar is None  # overridden by foo
    assert obj.baz is None
    assert obj.qux == "Q"  # preserved because baz is None


def test_decorator_uncoupled_fields_unaffected():
    @dataclass
    @coupled_fields(FieldCoupling(dominant="foo", recessive="bar"))
    class TestClass(HasCouplingMethods):
        foo: str | None = None
        bar: str | None = None
        uncoupled: str | None = None

    obj = TestClass(foo="F", bar="B", uncoupled="U")
    assert obj.foo == "F"
    assert obj.bar is None
    assert obj.uncoupled == "U"


def test_decorator_get_coupling_for_field_finds_dominant():
    @dataclass
    @coupled_fields(FieldCoupling(dominant="foo", recessive="bar"))
    class TestClass(HasCouplingMethods):
        foo: str | None = None
        bar: str | None = None

    coupling = TestClass.getCouplingForField("foo")
    assert coupling is not None
    assert coupling.dominant == "foo"
    assert coupling.recessive == "bar"


def test_decorator_get_coupling_for_field_finds_recessive():
    @dataclass
    @coupled_fields(FieldCoupling(dominant="foo", recessive="bar"))
    class TestClass(HasCouplingMethods):
        foo: str | None = None
        bar: str | None = None

    coupling = TestClass.getCouplingForField("bar")
    assert coupling is not None
    assert coupling.dominant == "foo"
    assert coupling.recessive == "bar"


def test_decorator_get_coupling_for_field_returns_none_for_uncoupled():
    @dataclass
    @coupled_fields(FieldCoupling(dominant="foo", recessive="bar"))
    class TestClass(HasCouplingMethods):
        foo: str | None = None
        bar: str | None = None
        baz: str | None = None

    coupling = TestClass.getCouplingForField("baz")
    assert coupling is None


def test_decorator_get_coupling_for_field_with_multiple_couplings():
    @dataclass
    @coupled_fields(
        FieldCoupling(dominant="foo", recessive="bar"),
        FieldCoupling(dominant="baz", recessive="qux"),
    )
    class TestClass(HasCouplingMethods):
        foo: str | None = None
        bar: str | None = None
        baz: str | None = None
        qux: str | None = None

    coupling1 = TestClass.getCouplingForField("foo")
    assert coupling1 is not None
    assert coupling1.dominant == "foo"
    assert coupling1.recessive == "bar"

    coupling2 = TestClass.getCouplingForField("qux")
    assert coupling2 is not None
    assert coupling2.dominant == "baz"
    assert coupling2.recessive == "qux"


def test_decorator_custom_post_init_preserved():
    @dataclass
    @coupled_fields(FieldCoupling(dominant="foo", recessive="bar"))
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
    coupling1 = FieldCoupling(dominant="a", recessive="b")
    coupling2 = FieldCoupling(dominant="c", recessive="d")

    @dataclass
    @coupled_fields(coupling1, coupling2)
    class TestClass(HasCouplingMethods):
        a: str | None = None
        b: str | None = None
        c: str | None = None
        d: str | None = None

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
