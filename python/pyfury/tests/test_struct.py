from dataclasses import dataclass
from typing import Dict, Any, List

import pytest
import typing

import pyfury
from pyfury import Fury, Language


def ser_de(fury, obj):
    binary = fury.serialize(obj)
    return fury.deserialize(binary)


@dataclass
class SimpleObject:
    f1: Dict[pyfury.Int32Type, pyfury.Float64Type] = None


@dataclass
class ComplexObject:
    f1: Any = None
    f2: Any = None
    f3: pyfury.Int8Type = 0
    f4: pyfury.Int16Type = 0
    f5: pyfury.Int32Type = 0
    f6: pyfury.Int64Type = 0
    f7: pyfury.Float32Type = 0
    f8: pyfury.Float64Type = 0
    f9: List[pyfury.Int16Type] = None
    f10: Dict[pyfury.Int32Type, pyfury.Float64Type] = None


def test_struct():
    fury = Fury(language=Language.XLANG, ref_tracking=True)
    fury.register_class_tag(SimpleObject, "example.SimpleObject")
    fury.register_class_tag(ComplexObject, "example.ComplexObject")
    o = SimpleObject(f1={1: 1.0 / 3})
    # assert ser_de(fury, o) == o

    o = ComplexObject(
        f1="str",
        f2={"k1": -1, "k2": [1, 2]},
        f3=2**7 - 1,
        f4=2**15 - 1,
        f5=2**31 - 1,
        f6=2**63 - 1,
        f7=1.0 / 2,
        f8=2.0 / 3,
        f9=[1, 2],
        f10={1: 1.0 / 3, 100: 2 / 7.0},
    )
    assert ser_de(fury, o) == o
    with pytest.raises(AssertionError):
        assert ser_de(fury, ComplexObject(f7=1.0 / 3)) == ComplexObject(f7=1.0 / 3)
    with pytest.raises(OverflowError):
        assert ser_de(fury, ComplexObject(f3=2**8)) == ComplexObject(f3=2**8)
    with pytest.raises(OverflowError):
        assert ser_de(fury, ComplexObject(f4=2**16)) == ComplexObject(f4=2**16)
    with pytest.raises(OverflowError):
        assert ser_de(fury, ComplexObject(f5=2**32)) == ComplexObject(f5=2**32)
    with pytest.raises(OverflowError):
        assert ser_de(fury, ComplexObject(f6=2**64)) == ComplexObject(f6=2**64)


@dataclass
class SuperClass1:
    f1: Any = None
    f2: pyfury.Int8Type = 0


@dataclass
class ChildClass1(SuperClass1):
    f3: Dict[str, pyfury.Float64Type] = None


def test_inheritance():
    type_hints = typing.get_type_hints(ChildClass1)
    print(type_hints)
    assert type_hints.keys() == {"f1", "f2", "f3"}
    fury = Fury(language=Language.PYTHON, ref_tracking=True)
    obj = ChildClass1(f1="a", f2=-10, f3={"a": -10.0, "b": 1 / 3})
    assert ser_de(fury, obj) == obj
    assert (
        type(fury.class_resolver.get_serializer(ChildClass1))
        == pyfury.DataClassSerializer
    )
