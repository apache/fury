# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from dataclasses import dataclass
from typing import Dict, Any, List

import pytest
import typing

import pyfory
from pyfory import Fory, Language
from pyfory.error import TypeUnregisteredError


def ser_de(fory, obj):
    binary = fory.serialize(obj)
    return fory.deserialize(binary)


@dataclass
class SimpleObject:
    f1: Dict[pyfory.Int32Type, pyfory.Float64Type] = None


@dataclass
class ComplexObject:
    f1: Any = None
    f2: Any = None
    f3: pyfory.Int8Type = 0
    f4: pyfory.Int16Type = 0
    f5: pyfory.Int32Type = 0
    f6: pyfory.Int64Type = 0
    f7: pyfory.Float32Type = 0
    f8: pyfory.Float64Type = 0
    f9: List[pyfory.Int16Type] = None
    f10: Dict[pyfory.Int32Type, pyfory.Float64Type] = None


def test_struct():
    fory = Fory(language=Language.XLANG, ref_tracking=True)
    fory.register_type(SimpleObject, typename="SimpleObject")
    fory.register_type(ComplexObject, typename="example.ComplexObject")
    o = SimpleObject(f1={1: 1.0 / 3})
    assert ser_de(fory, o) == o

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
    assert ser_de(fory, o) == o
    with pytest.raises(AssertionError):
        assert ser_de(fory, ComplexObject(f7=1.0 / 3)) == ComplexObject(f7=1.0 / 3)
    with pytest.raises(OverflowError):
        assert ser_de(fory, ComplexObject(f3=2**8)) == ComplexObject(f3=2**8)
    with pytest.raises(OverflowError):
        assert ser_de(fory, ComplexObject(f4=2**16)) == ComplexObject(f4=2**16)
    with pytest.raises(OverflowError):
        assert ser_de(fory, ComplexObject(f5=2**32)) == ComplexObject(f5=2**32)
    with pytest.raises(OverflowError):
        assert ser_de(fory, ComplexObject(f6=2**64)) == ComplexObject(f6=2**64)


@dataclass
class SuperClass1:
    f1: Any = None
    f2: pyfory.Int8Type = 0


@dataclass
class ChildClass1(SuperClass1):
    f3: Dict[str, pyfory.Float64Type] = None


def test_require_class_registration():
    fory = Fory(language=Language.PYTHON, ref_tracking=True)
    obj = ChildClass1(f1="a", f2=-10, f3={"a": -10.0, "b": 1 / 3})
    with pytest.raises(TypeUnregisteredError):
        fory.serialize(obj)


def test_inheritance():
    type_hints = typing.get_type_hints(ChildClass1)
    print(type_hints)
    assert type_hints.keys() == {"f1", "f2", "f3"}
    fory = Fory(
        language=Language.PYTHON, ref_tracking=True, require_class_registration=False
    )
    obj = ChildClass1(f1="a", f2=-10, f3={"a": -10.0, "b": 1 / 3})
    assert ser_de(fory, obj) == obj
    assert (
        type(fory.class_resolver.get_serializer(ChildClass1))
        == pyfory.DataClassSerializer
    )
