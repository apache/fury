import array
import datetime
import gc
import io
import pickle
import weakref
from enum import Enum
from typing import Any, List, Dict

import numpy as np
import pandas as pd

from dataclasses import dataclass

import pytest

import pyfury
from pyfury.buffer import Buffer
from pyfury import Fury, Language, _serialization, EnumSerializer
from pyfury.serializer import (
    TimestampSerializer,
    DateSerializer,
    PyArraySerializer,
    Numpy1DArraySerializer,
)
from pyfury.tests.core import require_pyarrow
from pyfury.type import FuryType
from pyfury.util import lazy_import

pa = lazy_import("pyarrow")


def test_float():
    fury = Fury(language=Language.PYTHON, ref_tracking=True)
    assert ser_de(fury, -1.0) == -1.0
    assert ser_de(fury, 1 / 3) == 1 / 3
    serializer = fury.class_resolver.get_serializer(float)
    assert type(serializer) is pyfury.DoubleSerializer


def test_tuple():
    fury = Fury(language=Language.PYTHON, ref_tracking=True)
    print(len(fury.serialize((-1.0, 2))))
    assert ser_de(fury, (-1.0, 2)) == (-1.0, 2)


def test_dict():
    fury = Fury(language=Language.PYTHON, ref_tracking=True)
    assert ser_de(fury, {1: 2}) == {1: 2}
    assert ser_de(fury, {1 / 3: 2.0}) == {1 / 3: 2.0}
    assert ser_de(fury, {1 / 3: 2}) == {1 / 3: 2}
    assert ser_de(fury, {"1": 2}) == {"1": 2}
    assert ser_de(fury, {"1": 1 / 3}) == {"1": 1 / 3}
    assert ser_de(fury, {"1": {}}) == {"1": {}}
    assert ser_de(fury, {"1": {1: 2}}) == {"1": {1: 2}}
    assert ser_de(fury, {"k1": {"a": 2.0}, "k2": {-1.0: -1.0}}) == {
        "k1": {"a": 2.0},
        "k2": {-1.0: -1.0},
    }
    # make multiple references point to same `-1.0`.
    dict3 = {
        1: {5: -1.0},
        2: {-1.0: -1.0, 10: -1.0},
    }
    assert ser_de(fury, dict3) == dict3


@pytest.mark.parametrize("language", [Language.XLANG, Language.PYTHON])
def test_basic_serializer(language):
    fury = Fury(language=language, ref_tracking=True)
    datetime_serializer = fury.class_resolver.get_serializer(datetime.datetime)
    assert isinstance(
        datetime_serializer, (TimestampSerializer, _serialization.TimestampSerializer)
    )
    assert datetime_serializer.get_xtype_id() == FuryType.TIMESTAMP.value
    date_serializer = fury.class_resolver.get_serializer(datetime.date)
    assert isinstance(date_serializer, (DateSerializer, _serialization.DateSerializer))
    assert date_serializer.get_xtype_id() == FuryType.DATE32.value
    assert ser_de(fury, True) is True
    assert ser_de(fury, False) is False
    assert ser_de(fury, -1) == -1
    assert ser_de(fury, 2**7 - 1) == 2**7 - 1
    assert ser_de(fury, 2**15 - 1) == 2**15 - 1
    assert ser_de(fury, -(2**15)) == -(2**15)
    assert ser_de(fury, 2**31 - 1) == 2**31 - 1
    assert ser_de(fury, 2**63 - 1) == 2**63 - 1
    assert ser_de(fury, -(2**63)) == -(2**63)
    assert ser_de(fury, 1.0) == 1.0
    assert ser_de(fury, -1.0) == -1.0
    assert ser_de(fury, "str") == "str"
    assert ser_de(fury, b"") == b""
    now = datetime.datetime.now()
    assert ser_de(fury, now) == now
    day = datetime.date(2021, 11, 23)
    assert ser_de(fury, day) == day
    list_ = ["a", 1, -1.0, True, now, day]
    assert ser_de(fury, list_) == list_
    dict1_ = {"k1": "a", "k2": 1, "k3": -1.0, "k4": True, "k5": now, "k6": day}
    assert ser_de(fury, dict1_) == dict1_
    dict2_ = {"a": "a", 1: 1, -1.0: -1.0, True: True, now: now, day: day}
    assert ser_de(fury, dict2_) == dict2_
    set_ = {"a", 1, -1.0, True, now, day}
    assert ser_de(fury, set_) == set_


@pytest.mark.parametrize("language", [Language.XLANG, Language.PYTHON])
def test_ref_tracking(language):
    fury = Fury(language=language, ref_tracking=True)

    simple_list = []
    simple_list.append(simple_list)
    new_simple_list = ser_de(fury, simple_list)
    assert new_simple_list[0] is new_simple_list

    now = datetime.datetime.now()
    day = datetime.date(2021, 11, 23)
    list_ = ["a", 1, -1.0, True, now, day]
    dict1 = {f"k{i}": v for i, v in enumerate(list_)}
    dict2 = {v: v for v in list_}
    dict3 = {
        "list1_0": list_,
        "list1_1": list_,
        "dict1_0": dict1,
        "dict1_1": dict1,
        "dict2_0": dict2,
        "dict2_1": dict2,
    }
    dict3["dict3_0"] = dict3
    dict3["dict3_1"] = dict3
    new_dict3 = ser_de(fury, dict3)
    assert new_dict3["list1_0"] == list_
    assert new_dict3["list1_0"] is new_dict3["list1_1"]
    assert new_dict3["dict1_0"] == dict1
    assert new_dict3["dict1_0"] is new_dict3["dict1_1"]
    assert new_dict3["dict2_0"] == dict2
    assert new_dict3["dict2_0"] is new_dict3["dict2_1"]
    assert new_dict3["dict3_0"] is new_dict3
    assert new_dict3["dict3_0"] is new_dict3["dict3_0"]


@pytest.mark.parametrize("language", [Language.PYTHON, Language.XLANG])
def test_tmp_ref(language):
    # FIXME this can't simulate the case where new objects are allocated on memory
    #  address of released tmp object.
    fury = Fury(language=language, ref_tracking=True)
    buffer = Buffer.allocate(128)
    writer_index = buffer.writer_index
    x = 1
    fury.serialize([x], buffer)
    fury.serialize([x], buffer)
    fury.serialize([x], buffer)
    assert buffer.writer_index > writer_index + 15

    l1 = fury.deserialize(buffer)
    l2 = fury.deserialize(buffer)
    l3 = fury.deserialize(buffer)
    assert l1 == [x]
    assert l2 == [x]
    assert l3 == [x]
    assert l1 is not l2
    assert l1 is not l3
    assert l2 is not l3


@pytest.mark.parametrize("language", [Language.PYTHON, Language.XLANG])
def test_multiple_ref(language):
    # FIXME this can't simulate the case where new objects are allocated on memory
    #  address of released tmp object.
    fury = Fury(language=language, ref_tracking=True)
    buffer = Buffer.allocate(128)
    for i in range(1000):
        fury.serialize([], buffer)
    objs = []
    for i in range(1000):
        objs.append(fury.deserialize(buffer))
    assert len(set(id(o) for o in objs)) == 1000


class RefTestClass1:
    def __init__(self, f1=None):
        self.f1 = f1


class RefTestClass2:
    def __init__(self, f1):
        self.f1 = f1


@pytest.mark.parametrize("language", [Language.PYTHON])
def test_ref_cleanup(language):
    # FIXME this can't simulate the case where new objects are allocated on memory
    #  address of released tmp object.
    fury = Fury(language=language, ref_tracking=True, require_class_registration=False)
    # TODO support Language.XLANG, current unpickler will error for xlang,
    o1 = RefTestClass1()
    o2 = RefTestClass2(f1=o1)
    pickle.loads(pickle.dumps(o2))
    ref1 = weakref.ref(o1)
    ref2 = weakref.ref(o2)
    data = fury.serialize(o2)
    del o1, o2
    gc.collect()
    assert ref1() is None
    assert ref2() is None
    fury.deserialize(data)


@pytest.mark.parametrize("language", [Language.XLANG, Language.PYTHON])
def test_array_serializer(language):
    fury = Fury(language=language, ref_tracking=True, require_class_registration=False)
    for typecode in PyArraySerializer.typecode_dict.keys():
        arr = array.array(typecode, list(range(10)))
        assert ser_de(fury, arr) == arr
    for dtype in Numpy1DArraySerializer.dtypes_dict.keys():
        arr = np.array(list(range(10)), dtype=dtype)
        new_arr = ser_de(fury, arr)
        assert np.array_equal(new_arr, arr)
        np.testing.assert_array_equal(new_arr, arr)


def ser_de(fury, obj):
    binary = fury.serialize(obj)
    return fury.deserialize(binary)


def test_pickle():
    buf = Buffer.allocate(32)
    pickler = pickle.Pickler(buf)
    pickler.dump(b"abc")
    buf.write_int32(-1)
    pickler.dump("abcd")
    assert buf.writer_index - 4 == len(pickle.dumps(b"abc")) + len(pickle.dumps("abcd"))
    print(f"writer_index {buf.writer_index}")

    bytes_io_ = io.BytesIO(buf)
    unpickler = pickle.Unpickler(bytes_io_)
    assert unpickler.load() == b"abc"
    bytes_io_.seek(bytes_io_.tell() + 4)
    assert unpickler.load() == "abcd"
    print(f"reader_index {buf.reader_index} {bytes_io_.tell()}")

    if pa:
        pa_buf = pa.BufferReader(buf)
        unpickler = pickle.Unpickler(pa_buf)
        assert unpickler.load() == b"abc"
        pa_buf.seek(pa_buf.tell() + 4)
        assert unpickler.load() == "abcd"
        print(f"reader_index {buf.reader_index} {pa_buf.tell()} {buf.reader_index}")

    unpickler = pickle.Unpickler(buf)
    assert unpickler.load() == b"abc"
    buf.reader_index = buf.reader_index + 4
    assert unpickler.load() == "abcd"
    print(f"reader_index {buf.reader_index}")


@require_pyarrow
def test_serialize_arrow():
    record_batch = create_record_batch(10000)
    table = pa.Table.from_batches([record_batch, record_batch])
    fury = Fury(language=Language.XLANG, ref_tracking=True)
    serialized_data = Buffer.allocate(32)
    fury.serialize(record_batch, buffer=serialized_data)
    fury.serialize(table, buffer=serialized_data)
    new_batch = fury.deserialize(serialized_data)
    new_table = fury.deserialize(serialized_data)
    assert new_batch == record_batch
    assert new_table == table


@require_pyarrow
def test_serialize_arrow_zero_copy():
    record_batch = create_record_batch(10000)
    table = pa.Table.from_batches([record_batch, record_batch])
    buffer_objects = []
    fury = Fury(language=Language.XLANG, ref_tracking=True)
    serialized_data = Buffer.allocate(32)
    fury.serialize(
        record_batch, buffer=serialized_data, buffer_callback=buffer_objects.append
    )
    fury.serialize(table, buffer=serialized_data, buffer_callback=buffer_objects.append)
    buffers = [o.to_buffer() for o in buffer_objects]
    new_batch = fury.deserialize(serialized_data, buffers=buffers[:1])
    new_table = fury.deserialize(serialized_data, buffers=buffers[1:])
    buffer_objects.clear()
    assert new_batch == record_batch
    assert new_table == table


def create_record_batch(size):
    data = [
        pa.array([bool(i % 2) for i in range(size)]),
        pa.array([f"test{i}" for i in range(size)]),
    ]
    return pa.RecordBatch.from_arrays(data, ["boolean", "varchar"])


@dataclass
class Foo:
    f1: int


@dataclass
class Bar(Foo):
    f2: int


class BarSerializer(pyfury.Serializer):
    def xwrite(self, buffer, value: Bar):
        buffer.write_int32(value.f1)
        buffer.write_int32(value.f2)

    def xread(self, buffer):
        return Bar(buffer.read_int32(), buffer.read_int32())

    def get_xtype_id(self):
        return pyfury.FuryType.FURY_TYPE_TAG.value

    def get_xtype_tag(self):
        return "test.Bar"


class RegisterClass:
    def __init__(self, f1=None):
        self.f1 = f1


def test_register_py_serializer():
    fury = Fury(
        language=Language.PYTHON, ref_tracking=True, require_class_registration=False
    )

    class Serializer(pyfury.Serializer):
        def write(self, buffer, value):
            buffer.write_int32(value.f1)

        def read(self, buffer):
            a = A()
            a.f1 = buffer.read_int32()
            return a

        def xwrite(self, buffer, value):
            raise NotImplementedError

        def xread(self, buffer):
            raise NotImplementedError

    fury.register_serializer(A, Serializer(fury, RegisterClass))
    assert fury.deserialize(fury.serialize(RegisterClass(100))).f1 == 100


class A:
    class B:
        class C:
            pass


def test_register_class():
    fury = Fury(language=Language.PYTHON, ref_tracking=True)

    class Serializer(pyfury.Serializer):
        def write(self, buffer, value):
            pass

        def read(self, buffer):
            return self.type_()

        def xwrite(self, buffer, value):
            raise NotImplementedError

        def xread(self, buffer):
            raise NotImplementedError

    fury.register_serializer(A, Serializer(fury, A))
    fury.register_serializer(A.B, Serializer(fury, A.B))
    fury.register_serializer(A.B.C, Serializer(fury, A.B.C))
    assert isinstance(fury.deserialize(fury.serialize(A())), A)
    assert isinstance(fury.deserialize(fury.serialize(A.B())), A.B)
    assert isinstance(fury.deserialize(fury.serialize(A.B.C())), A.B.C)


def test_pickle_fallback():
    fury = Fury(
        language=Language.PYTHON, ref_tracking=True, require_class_registration=False
    )
    o1 = [1, True, np.dtype(np.int32)]
    data1 = fury.serialize(o1)
    new_o1 = fury.deserialize(data1)
    assert o1 == new_o1


def test_unsupported_callback():
    fury = Fury(language=Language.PYTHON, ref_tracking=True)

    def f1(x):
        return x

    def f2(x):
        return x + x

    obj1 = [1, True, f1, f2, {1: 2}]
    with pytest.raises(Exception):
        fury.serialize(obj1)
    unsupported_objects = []
    binary1 = fury.serialize(obj1, unsupported_callback=unsupported_objects.append)
    assert len(unsupported_objects) == 2
    assert unsupported_objects == [f1, f2]
    new_obj1 = fury.deserialize(binary1, unsupported_objects=unsupported_objects)
    assert new_obj1 == obj1


def test_slice():
    fury = Fury(language=Language.PYTHON, ref_tracking=True)
    assert fury.deserialize(fury.serialize(slice(1, 100, 10))) == slice(1, 100, 10)
    assert fury.deserialize(fury.serialize(slice(1, None, 10))) == slice(1, None, 10)
    assert fury.deserialize(fury.serialize(slice(10, 10, None))) == slice(10, 10, None)
    assert fury.deserialize(fury.serialize(slice(None, None, 10))) == slice(
        None, None, 10
    )
    assert fury.deserialize(fury.serialize(slice(None, None, None))) == slice(
        None, None, None
    )
    assert fury.deserialize(
        fury.serialize([1, 2, slice(1, 100, 10), slice(1, 100, 10)])
    ) == [1, 2, slice(1, 100, 10), slice(1, 100, 10)]
    assert fury.deserialize(
        fury.serialize([1, slice(1, None, 10), False, [], slice(1, 100, 10)])
    ) == [1, slice(1, None, 10), False, [], slice(1, 100, 10)]
    assert fury.deserialize(
        fury.serialize([1, slice(1, None, "10"), False, [], slice(1, 100, "10")])
    ) == [1, slice(1, None, "10"), False, [], slice(1, 100, "10")]


class TestEnum(Enum):
    E1 = 1
    E2 = 2
    E3 = "E3"
    E4 = "E4"


def test_enum():
    fury = Fury(language=Language.PYTHON, ref_tracking=True)
    assert ser_de(fury, TestEnum.E1) == TestEnum.E1
    assert ser_de(fury, TestEnum.E2) == TestEnum.E2
    assert ser_de(fury, TestEnum.E3) == TestEnum.E3
    assert ser_de(fury, TestEnum.E4) == TestEnum.E4
    assert isinstance(fury.class_resolver.get_serializer(TestEnum), EnumSerializer)
    assert isinstance(
        fury.class_resolver.get_serializer(obj=TestEnum.E1), EnumSerializer
    )
    assert isinstance(
        fury.class_resolver.get_serializer(obj=TestEnum.E4), EnumSerializer
    )


def test_duplicate_serialize():
    fury = Fury(language=Language.PYTHON, ref_tracking=True)
    assert ser_de(fury, TestEnum.E1) == TestEnum.E1
    assert ser_de(fury, TestEnum.E2) == TestEnum.E2
    assert ser_de(fury, TestEnum.E4) == TestEnum.E4
    assert ser_de(fury, TestEnum.E2) == TestEnum.E2
    assert ser_de(fury, TestEnum.E1) == TestEnum.E1
    assert ser_de(fury, TestEnum.E4) == TestEnum.E4


@dataclass(unsafe_hash=True)
class TestCacheClass1:
    f1: int


def test_cache_serializer():
    fury = Fury(language=Language.PYTHON, ref_tracking=True)
    fury.register_serializer(TestCacheClass1, pyfury.PickleStrongCacheSerializer(fury))
    assert ser_de(fury, TestCacheClass1(1)) == TestCacheClass1(1)
    fury.register_serializer(TestCacheClass1, pyfury.PickleCacheSerializer(fury))
    assert ser_de(fury, TestCacheClass1(1)) == TestCacheClass1(1)

    classinfo = pyfury.PickleStrongCacheSerializer.new_classinfo(fury)
    buffer = Buffer.allocate(32)
    fury.serialize_ref(buffer, TestCacheClass1(1), classinfo)
    assert fury.deserialize_ref(buffer) == TestCacheClass1(1)
    classinfo = pyfury.PickleCacheSerializer.new_classinfo(fury)
    fury.serialize_ref(buffer, TestCacheClass1(1), classinfo)
    assert fury.deserialize_ref(buffer) == TestCacheClass1(1)


def test_pandas_range_index():
    fury = Fury(
        language=Language.PYTHON, ref_tracking=True, require_class_registration=False
    )
    fury.register_serializer(pd.RangeIndex, pyfury.PandasRangeIndexSerializer(fury))
    index = pd.RangeIndex(1, 100, 2, name="a")
    new_index = ser_de(fury, index)
    pd.testing.assert_index_equal(new_index, new_index)


@dataclass(unsafe_hash=True)
class TestPyDataClass1:
    f1: int
    f2: float
    f3: str
    f4: bool
    f5: Any
    f6: List
    f7: Dict


def test_py_serialize_dataclass():
    fury = Fury(language=Language.PYTHON, ref_tracking=True)
    obj1 = TestPyDataClass1(
        f1=1, f2=-2.0, f3="abc", f4=True, f5="xyz", f6=[1, 2], f7={"k1": "v1"}
    )
    assert ser_de(fury, obj1) == obj1
    obj2 = TestPyDataClass1(
        f1=None, f2=-2.0, f3="abc", f4=None, f5="xyz", f6=None, f7=None
    )
    assert ser_de(fury, obj2) == obj2
