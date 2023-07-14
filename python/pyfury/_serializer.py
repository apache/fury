import array
import datetime
import logging
from abc import ABC, abstractmethod
from typing import Dict, Iterable, Any

from pyfury.buffer import Buffer
from pyfury.resolver import NOT_NULL_VALUE_FLAG, NULL_FLAG
from pyfury.type import (
    FuryType,
    is_primitive_type,
    # Int8ArrayType,
    Int16ArrayType,
    Int32ArrayType,
    Int64ArrayType,
    Float32ArrayType,
    Float64ArrayType,
)

try:
    import numpy as np
except ImportError:
    np = None

logger = logging.getLogger(__name__)


NOT_SUPPORT_CROSS_LANGUAGE = 0
USE_CLASSNAME = 0
USE_CLASS_ID = 1
# preserve 0 as flag for class id not set in ClassInfo`
NO_CLASS_ID = 0
PYINT_CLASS_ID = 1
PYFLOAT_CLASS_ID = 2
PYBOOL_CLASS_ID = 3
STRING_CLASS_ID = 4
PICKLE_CLASS_ID = 5
PICKLE_STRONG_CACHE_CLASS_ID = 6
PICKLE_CACHE_CLASS_ID = 7
# `NOT_NULL_VALUE_FLAG` + `CLASS_ID` in little-endian order
NOT_NULL_PYINT_FLAG = NOT_NULL_VALUE_FLAG & 0b11111111 | (PYINT_CLASS_ID << 8)
NOT_NULL_PYFLOAT_FLAG = NOT_NULL_VALUE_FLAG & 0b11111111 | (PYFLOAT_CLASS_ID << 8)
NOT_NULL_PYBOOL_FLAG = NOT_NULL_VALUE_FLAG & 0b11111111 | (PYBOOL_CLASS_ID << 8)
NOT_NULL_STRING_FLAG = NOT_NULL_VALUE_FLAG & 0b11111111 | (STRING_CLASS_ID << 8)


class _PickleStub:
    pass


class PickleStrongCacheStub:
    pass


class PickleCacheStub:
    pass


class BufferObject(ABC):
    """
    Fury binary representation of an object.
    Note: This class is used for zero-copy out-of-band serialization and shouldn't
     be used for any other cases.
    """

    @abstractmethod
    def total_bytes(self) -> int:
        """total size for serialized bytes of an object"""

    @abstractmethod
    def write_to(self, buffer: "Buffer"):
        """Write serialized object to a buffer."""

    @abstractmethod
    def to_buffer(self) -> "Buffer":
        """Write serialized data as Buffer."""


class BytesBufferObject(BufferObject):
    __slots__ = ("binary",)

    def __init__(self, binary: bytes):
        self.binary = binary

    def total_bytes(self) -> int:
        return len(self.binary)

    def write_to(self, buffer: "Buffer"):
        buffer.write_bytes(self.binary)

    def to_buffer(self) -> "Buffer":
        return Buffer(self.binary)


class Serializer(ABC):
    __slots__ = "fury", "type_", "need_to_write_ref"

    def __init__(self, fury, type_: type):
        self.fury = fury
        self.type_: type = type_
        self.need_to_write_ref = not is_primitive_type(type_)

    def get_xtype_id(self):
        """
        Returns
        -------
            Returns NOT_SUPPORT_CROSS_LANGUAGE if the serializer doesn't
            support cross-language serialization.
            Return a number in range (0, 32767) if the serializer support
            cross-language serialization and native serialization data is the
            same with cross-language serialization.
            Return a negative short in range [-32768, 0) if the serializer
            support cross-language serialization and native serialization data
            is not the same with cross-language serialization.
        """
        return NOT_SUPPORT_CROSS_LANGUAGE

    @abstractmethod
    def get_xtype_tag(self):
        """
        Returns
        -------
            a type tag used for setup type mapping between languages.
        """

    def write(self, buffer, value):
        raise NotImplementedError

    def read(self, buffer):
        raise NotImplementedError

    @abstractmethod
    def xwrite(self, buffer, value):
        pass

    @abstractmethod
    def xread(self, buffer):
        pass

    @classmethod
    def support_subclass(cls) -> bool:
        return False


class CrossLanguageCompatibleSerializer(Serializer):
    def __init__(self, fury, type_):
        super().__init__(fury, type_)

    def xwrite(self, buffer, value):
        self.write(buffer, value)

    def xread(self, buffer):
        return self.read(buffer)


class NoneSerializer(Serializer):
    def xwrite(self, buffer, value):
        raise NotImplementedError

    def xread(self, buffer):
        raise NotImplementedError

    def write(self, buffer, value):
        pass

    def read(self, buffer):
        return None


class BooleanSerializer(CrossLanguageCompatibleSerializer):
    def get_xtype_id(self):
        return FuryType.BOOL.value

    def write(self, buffer, value):
        buffer.write_bool(value)

    def read(self, buffer):
        return buffer.read_bool()


class ByteSerializer(CrossLanguageCompatibleSerializer):
    def get_xtype_id(self):
        return FuryType.INT8.value

    def write(self, buffer, value):
        buffer.write_int8(value)

    def read(self, buffer):
        return buffer.read_int8()


class Int16Serializer(CrossLanguageCompatibleSerializer):
    def get_xtype_id(self):
        return FuryType.INT16.value

    def write(self, buffer, value):
        buffer.write_int16(value)

    def read(self, buffer):
        return buffer.read_int16()


class Int32Serializer(CrossLanguageCompatibleSerializer):
    def get_xtype_id(self):
        return FuryType.INT32.value

    def write(self, buffer, value):
        buffer.write_int32(value)

    def read(self, buffer):
        return buffer.read_int32()


class Int64Serializer(Serializer):
    def get_xtype_id(self):
        return FuryType.INT64.value

    def xwrite(self, buffer, value):
        buffer.write_int64(value)

    def xread(self, buffer):
        return buffer.read_int64()

    def write(self, buffer, value):
        buffer.write_varint64(value)

    def read(self, buffer):
        return buffer.read_varint64()


class FloatSerializer(CrossLanguageCompatibleSerializer):
    def get_xtype_id(self):
        return FuryType.FLOAT.value

    def write(self, buffer, value):
        buffer.write_float(value)

    def read(self, buffer):
        return buffer.read_float()


class DoubleSerializer(CrossLanguageCompatibleSerializer):
    def get_xtype_id(self):
        return FuryType.DOUBLE.value

    def write(self, buffer, value):
        buffer.write_double(value)

    def read(self, buffer):
        return buffer.read_double()


class StringSerializer(CrossLanguageCompatibleSerializer):
    def get_xtype_id(self):
        return FuryType.STRING.value

    def write(self, buffer, value: str):
        buffer.write_string(value)

    def read(self, buffer):
        return buffer.read_string()


_base_date = datetime.date(1970, 1, 1)


class DateSerializer(CrossLanguageCompatibleSerializer):
    def get_xtype_id(self):
        return FuryType.DATE32.value

    def write(self, buffer, value: datetime.date):
        if not isinstance(value, datetime.date):
            raise TypeError(
                "{} should be {} instead of {}".format(
                    value, datetime.date, type(value)
                )
            )
        days = (value - _base_date).days
        buffer.write_int32(days)

    def read(self, buffer):
        days = buffer.read_int32()
        return _base_date + datetime.timedelta(days=days)


class TimestampSerializer(CrossLanguageCompatibleSerializer):
    def get_xtype_id(self):
        return FuryType.TIMESTAMP.value

    def write(self, buffer, value: datetime.datetime):
        if not isinstance(value, datetime.datetime):
            raise TypeError(
                "{} should be {} instead of {}".format(value, datetime, type(value))
            )
        # TimestampType represent micro seconds
        timestamp = int(value.timestamp() * 1000000)
        buffer.write_int64(timestamp)

    def read(self, buffer):
        ts = buffer.read_int64() / 1000000
        # TODO support timezone
        return datetime.datetime.fromtimestamp(ts)


class BytesSerializer(CrossLanguageCompatibleSerializer):
    def get_xtype_id(self):
        return FuryType.BINARY.value

    def write(self, buffer, value: bytes):
        assert isinstance(value, bytes)
        self.fury.write_buffer_object(buffer, BytesBufferObject(value))

    def read(self, buffer):
        fury_buf = self.fury.read_buffer_object(buffer)
        return fury_buf.to_pybytes()


# Use numpy array or python array module.
typecode_dict = {
    # use bytes serializer for byte array.
    "h": (2, FuryType.FURY_PRIMITIVE_SHORT_ARRAY.value),
    "i": (4, FuryType.FURY_PRIMITIVE_INT_ARRAY.value),
    "l": (8, FuryType.FURY_PRIMITIVE_LONG_ARRAY.value),
    "f": (4, FuryType.FURY_PRIMITIVE_FLOAT_ARRAY.value),
    "d": (8, FuryType.FURY_PRIMITIVE_DOUBLE_ARRAY.value),
}
if np:
    typecode_dict = {
        k: (itemsize, -type_id) for k, (itemsize, type_id) in typecode_dict.items()
    }


class PyArraySerializer(CrossLanguageCompatibleSerializer):
    typecode_dict = typecode_dict
    typecodearray_type = {
        "h": Int16ArrayType,
        "i": Int32ArrayType,
        "l": Int64ArrayType,
        "f": Float32ArrayType,
        "d": Float64ArrayType,
    }

    def __init__(self, fury, type_, typecode):
        super().__init__(fury, type_)
        self.typecode = typecode
        self.itemsize, self.type_id = PyArraySerializer.typecode_dict[self.typecode]

    def get_xtype_id(self):
        return self.type_id

    def xwrite(self, buffer, value):
        assert value.itemsize == self.itemsize
        view = memoryview(value)
        assert view.format == self.typecode
        assert view.itemsize == self.itemsize
        assert view.c_contiguous  # TODO handle contiguous
        nbytes = len(value) * self.itemsize
        buffer.write_varint32(nbytes)
        buffer.write_buffer(value)

    def xread(self, buffer):
        data = buffer.read_bytes_and_size()
        arr = array.array(self.typecode, [])
        arr.frombytes(data)
        return arr

    def write(self, buffer, value: array.array):
        nbytes = len(value) * value.itemsize
        buffer.write_string(value.typecode)
        buffer.write_varint32(nbytes)
        buffer.write_buffer(value)

    def read(self, buffer):
        typecode = buffer.read_string()
        data = buffer.read_bytes_and_size()
        arr = array.array(typecode, [])
        arr.frombytes(data)
        return arr


if np:
    _np_dtypes_dict = {
        # use bytes serializer for byte array.
        np.dtype(np.bool_): (1, "?", FuryType.FURY_PRIMITIVE_BOOL_ARRAY.value),
        np.dtype(np.int16): (2, "h", FuryType.FURY_PRIMITIVE_SHORT_ARRAY.value),
        np.dtype(np.int32): (4, "i", FuryType.FURY_PRIMITIVE_INT_ARRAY.value),
        np.dtype(np.int64): (8, "l", FuryType.FURY_PRIMITIVE_LONG_ARRAY.value),
        np.dtype(np.float32): (4, "f", FuryType.FURY_PRIMITIVE_FLOAT_ARRAY.value),
        np.dtype(np.float64): (8, "d", FuryType.FURY_PRIMITIVE_DOUBLE_ARRAY.value),
    }
else:
    _np_dtypes_dict = {}


class Numpy1DArraySerializer(CrossLanguageCompatibleSerializer):
    dtypes_dict = _np_dtypes_dict

    def __init__(self, fury, type_, dtype):
        super().__init__(fury, type_)
        self.dtype = dtype
        self.itemsize, self.typecode, self.type_id = _np_dtypes_dict[self.dtype]

    def get_xtype_id(self):
        return self.type_id

    def xwrite(self, buffer, value):
        assert value.itemsize == self.itemsize
        view = memoryview(value)
        assert view.format == self.typecode
        assert view.itemsize == self.itemsize
        nbytes = len(value) * self.itemsize
        buffer.write_varint32(nbytes)
        if self.dtype == np.dtype("bool") or not view.c_contiguous:
            buffer.write_bytes(value.tobytes())
        else:
            buffer.write_buffer(value)

    def xread(self, buffer):
        data = buffer.read_bytes_and_size()
        return np.frombuffer(data, dtype=self.dtype)

    def write(self, buffer, value):
        self.fury.handle_unsupported_write(buffer, value)

    def read(self, buffer):
        return self.fury.handle_unsupported_read(buffer)


class CollectionSerializer(Serializer):
    __slots__ = "class_resolver", "ref_resolver", "elem_serializer"

    def __init__(self, fury, type_, elem_serializer=None):
        super().__init__(fury, type_)
        self.class_resolver = fury.class_resolver
        self.ref_resolver = fury.ref_resolver
        self.elem_serializer = elem_serializer

    def get_xtype_id(self):
        return -FuryType.LIST.value

    def write(self, buffer, value: Iterable[Any]):
        buffer.write_varint32(len(value))
        for s in value:
            cls = type(s)
            if cls is str:
                buffer.write_int24(NOT_NULL_STRING_FLAG)
                buffer.write_string(s)
            elif cls is int:
                buffer.write_int24(NOT_NULL_PYINT_FLAG)
                buffer.write_varint64(s)
            elif cls is bool:
                buffer.write_int24(NOT_NULL_PYBOOL_FLAG)
                buffer.write_bool(s)
            else:
                if not self.ref_resolver.write_ref_or_null(buffer, s):
                    classinfo = self.class_resolver.get_or_create_classinfo(cls)
                    self.class_resolver.write_classinfo(buffer, classinfo)
                    classinfo.serializer.write(buffer, s)

    def read(self, buffer):
        len_ = buffer.read_varint32()
        collection_ = self.new_instance(self.type_)
        for i in range(len_):
            self.handle_read_elem(self.fury.deserialize_ref(buffer), collection_)
        return collection_

    def new_instance(self, type_):
        # TODO support iterable subclass
        instance = []
        self.fury.ref_resolver.reference(instance)
        return instance

    def handle_read_elem(self, elem, collection_):
        collection_.append(elem)

    def xwrite(self, buffer, value):
        try:
            len_ = len(value)
        except AttributeError:
            value = list(value)
            len_ = len(value)
        buffer.write_varint32(len_)
        for s in value:
            self.fury.xserialize_ref(buffer, s, serializer=self.elem_serializer)
            len_ += 1

    def xread(self, buffer):
        len_ = buffer.read_varint32()
        collection_ = self.new_instance(self.type_)
        for i in range(len_):
            self.handle_read_elem(
                self.fury.xdeserialize_ref(buffer, serializer=self.elem_serializer),
                collection_,
            )
        return collection_


class ListSerializer(CollectionSerializer):
    def get_xtype_id(self):
        return FuryType.LIST.value

    def read(self, buffer):
        len_ = buffer.read_varint32()
        instance = []
        self.fury.ref_resolver.reference(instance)
        for i in range(len_):
            instance.append(self.fury.deserialize_ref(buffer))
        return instance


class TupleSerializer(CollectionSerializer):
    def read(self, buffer):
        len_ = buffer.read_varint32()
        collection_ = []
        for i in range(len_):
            collection_.append(self.fury.deserialize_ref(buffer))
        return tuple(collection_)


class StringArraySerializer(ListSerializer):
    def __init__(self, fury, type_):
        super().__init__(fury, type_, StringSerializer(fury, str))

    def get_xtype_id(self):
        return FuryType.FURY_STRING_ARRAY.value


class SetSerializer(CollectionSerializer):
    def get_xtype_id(self):
        return FuryType.FURY_SET.value

    def new_instance(self, type_):
        instance = set()
        self.fury.ref_resolver.reference(instance)
        return instance

    def handle_read_elem(self, elem, set_: set):
        set_.add(elem)


class MapSerializer(Serializer):
    __slots__ = (
        "class_resolver",
        "ref_resolver",
        "key_serializer",
        "value_serializer",
    )

    def __init__(self, fury, type_, key_serializer=None, value_serializer=None):
        super().__init__(fury, type_)
        self.class_resolver = fury.class_resolver
        self.ref_resolver = fury.ref_resolver
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer

    def get_xtype_id(self):
        return FuryType.MAP.value

    def write(self, buffer, value: Dict):
        buffer.write_varint32(len(value))
        for k, v in value.items():
            key_cls = type(k)
            if key_cls is str:
                buffer.write_int24(NOT_NULL_STRING_FLAG)
                buffer.write_string(k)
            else:
                if not self.ref_resolver.write_ref_or_null(buffer, k):
                    classinfo = self.class_resolver.get_or_create_classinfo(key_cls)
                    self.class_resolver.write_classinfo(buffer, classinfo)
                    classinfo.serializer.write(buffer, k)
            value_cls = type(v)
            if value_cls is str:
                buffer.write_int24(NOT_NULL_STRING_FLAG)
                buffer.write_string(v)
            elif value_cls is int:
                buffer.write_int24(NOT_NULL_PYINT_FLAG)
                buffer.write_varint64(v)
            else:
                if not self.ref_resolver.write_ref_or_null(buffer, v):
                    classinfo = self.class_resolver.get_or_create_classinfo(value_cls)
                    self.class_resolver.write_classinfo(buffer, classinfo)
                    classinfo.serializer.write(buffer, v)

    def read(self, buffer):
        len_ = buffer.read_varint32()
        map_ = self.type_()
        self.fury.ref_resolver.reference(map_)
        for i in range(len_):
            k = self.fury.deserialize_ref(buffer)
            v = self.fury.deserialize_ref(buffer)
            map_[k] = v
        return map_

    def xwrite(self, buffer, value: Dict):
        buffer.write_varint32(len(value))
        for k, v in value.items():
            self.fury.xserialize_ref(buffer, k, serializer=self.key_serializer)
            self.fury.xserialize_ref(buffer, v, serializer=self.value_serializer)

    def xread(self, buffer):
        len_ = buffer.read_varint32()
        map_ = {}
        self.fury.ref_resolver.reference(map_)
        for i in range(len_):
            k = self.fury.xdeserialize_ref(buffer, serializer=self.key_serializer)
            v = self.fury.xdeserialize_ref(buffer, serializer=self.value_serializer)
            map_[k] = v
        return map_


SubMapSerializer = MapSerializer


class EnumSerializer(Serializer):
    @classmethod
    def support_subclass(cls) -> bool:
        return True

    def write(self, buffer, value):
        buffer.write_string(value.name)

    def read(self, buffer):
        name = buffer.read_string()
        return getattr(self.type_, name)

    def xwrite(self, buffer, value):
        raise NotImplementedError

    def xread(self, buffer):
        raise NotImplementedError


class SliceSerializer(Serializer):
    def write(self, buffer, value: slice):
        start, stop, step = value.start, value.stop, value.step
        if type(start) is int:
            # TODO support varint128
            buffer.write_int24(NOT_NULL_PYINT_FLAG)
            buffer.write_varint64(start)
        else:
            if start is None:
                buffer.write_int8(NULL_FLAG)
            else:
                buffer.write_int8(NOT_NULL_VALUE_FLAG)
                self.fury.serialize_nonref(buffer, start)
        if type(stop) is int:
            # TODO support varint128
            buffer.write_int24(NOT_NULL_PYINT_FLAG)
            buffer.write_varint64(stop)
        else:
            if stop is None:
                buffer.write_int8(NULL_FLAG)
            else:
                buffer.write_int8(NOT_NULL_VALUE_FLAG)
                self.fury.serialize_nonref(buffer, stop)
        if type(step) is int:
            # TODO support varint128
            buffer.write_int24(NOT_NULL_PYINT_FLAG)
            buffer.write_varint64(step)
        else:
            if step is None:
                buffer.write_int8(NULL_FLAG)
            else:
                buffer.write_int8(NOT_NULL_VALUE_FLAG)
                self.fury.serialize_nonref(buffer, step)

    def read(self, buffer):
        if buffer.read_int8() == NULL_FLAG:
            start = None
        else:
            start = self.fury.deserialize_nonref(buffer)
        if buffer.read_int8() == NULL_FLAG:
            stop = None
        else:
            stop = self.fury.deserialize_nonref(buffer)
        if buffer.read_int8() == NULL_FLAG:
            step = None
        else:
            step = self.fury.deserialize_nonref(buffer)
        return slice(start, stop, step)

    def xwrite(self, buffer, value):
        raise NotImplementedError

    def xread(self, buffer):
        raise NotImplementedError


class PickleSerializer(Serializer):
    def xwrite(self, buffer, value):
        raise NotImplementedError

    def xread(self, buffer):
        raise NotImplementedError

    def write(self, buffer, value):
        self.fury.handle_unsupported_write(buffer, value)

    def read(self, buffer):
        return self.fury.handle_unsupported_read(buffer)


class SerializationContext:
    """
    A context is used to add some context-related information, so that the
    serializers can setup relation between serializing different objects.
    The context will be reset after finished serializing/deserializing the
    object tree.
    """

    __slots__ = ("objects",)

    def __init__(self):
        self.objects = dict()

    def add(self, key, obj):
        self.objects[id(key)] = obj

    def __contains__(self, key):
        return id(key) in self.objects

    def __getitem__(self, key):
        return self.objects[id(key)]

    def get(self, key):
        return self.objects.get(id(key))

    def reset(self):
        if len(self.objects) > 0:
            self.objects.clear()
