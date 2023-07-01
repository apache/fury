import itertools
import os
import pickle
from weakref import WeakValueDictionary

import typing

import pyfury.lib.mmh3
from pyfury.buffer import Buffer
from pyfury.codegen import (
    gen_write_nullable_basic_stmts,
    gen_read_nullable_basic_stmts,
    compile_function,
)
from pyfury.error import ClassNotCompatibleError
from pyfury.lib.collection import WeakIdentityKeyDictionary
from pyfury.resolver import NULL_FLAG, NOT_NULL_VALUE_FLAG

try:
    import numpy as np
except ImportError:
    np = None

from pyfury._serializer import (  # noqa: F401 # pylint: disable=unused-import
    BufferObject,
    BytesBufferObject,
    Serializer,
    CrossLanguageCompatibleSerializer,
    BooleanSerializer,
    ByteSerializer,
    Int16Serializer,
    Int32Serializer,
    Int64Serializer,
    FloatSerializer,
    DoubleSerializer,
    StringSerializer,
    DateSerializer,
    TimestampSerializer,
    BytesSerializer,
    PyArraySerializer,
    Numpy1DArraySerializer,
    CollectionSerializer,
    ListSerializer,
    TupleSerializer,
    StringArraySerializer,
    SetSerializer,
    MapSerializer,
    SubMapSerializer,
    EnumSerializer,
    SliceSerializer,
    PickleSerializer,
    NOT_NULL_PYINT_FLAG,
    PickleStrongCacheStub,
    PickleCacheStub,
    PICKLE_STRONG_CACHE_CLASS_ID,
    PICKLE_CACHE_CLASS_ID,
)

try:
    from pyfury._serialization import ENABLE_FURY_CYTHON_SERIALIZATION

    if ENABLE_FURY_CYTHON_SERIALIZATION:
        from pyfury._serialization import (  # noqa: F401, F811
            BufferObject,
            BytesBufferObject,
            Serializer,
            CrossLanguageCompatibleSerializer,
            BooleanSerializer,
            ByteSerializer,
            Int16Serializer,
            Int32Serializer,
            Int64Serializer,
            FloatSerializer,
            DoubleSerializer,
            StringSerializer,
            DateSerializer,
            TimestampSerializer,
            BytesSerializer,
            PyArraySerializer,
            Numpy1DArraySerializer,
            CollectionSerializer,
            ListSerializer,
            TupleSerializer,
            StringArraySerializer,
            SetSerializer,
            MapSerializer,
            SubMapSerializer,
            EnumSerializer,
            SliceSerializer,
            PickleSerializer,
            PickleStrongCacheStub,
            PickleCacheStub,
        )
except ImportError:
    pass


class PickleStrongCacheSerializer(Serializer):
    """If we can't create weak ref to object, use this cache serializer instead.
    clear cache by threshold to avoid memory leak."""

    __slots__ = "_cached", "_clear_threshold", "_counter"

    def __init__(self, fury_, clear_threshold: int = 1000):
        super().__init__(fury_, PickleStrongCacheStub)
        self._cached = {}
        self._clear_threshold = clear_threshold

    def write(self, buffer, value):
        serialized = self._cached.get(value)
        if serialized is None:
            serialized = pickle.dumps(value)
            self._cached[value] = serialized
        buffer.write_bytes_and_size(serialized)
        if len(self._cached) == self._clear_threshold:
            self._cached.clear()

    def read(self, buffer):
        return pickle.loads(buffer.read_bytes_and_size())

    def cross_language_write(self, buffer, value):
        raise NotImplementedError

    def cross_language_read(self, buffer):
        raise NotImplementedError

    @staticmethod
    def new_classinfo(fury, clear_threshold: int = 1000):
        from pyfury import ClassInfo

        return ClassInfo(
            PickleStrongCacheStub,
            PICKLE_STRONG_CACHE_CLASS_ID,
            serializer=PickleStrongCacheSerializer(
                fury, clear_threshold=clear_threshold
            ),
            class_name_bytes=b"PickleStrongCacheStub",
        )


class PickleCacheSerializer(Serializer):
    __slots__ = "_cached", "_reverse_cached"

    def __init__(self, fury_):
        super().__init__(fury_, PickleCacheStub)
        self._cached = WeakIdentityKeyDictionary()
        self._reverse_cached = WeakValueDictionary()

    def write(self, buffer, value):
        cache = self._cached.get(value)
        if cache is None:
            serialized = pickle.dumps(value)
            value_hash = pyfury.lib.mmh3.hash_buffer(serialized)[0]
            cache = value_hash, serialized
            self._cached[value] = cache
        buffer.write_int64(cache[0])
        buffer.write_bytes_and_size(cache[1])

    def read(self, buffer):
        value_hash = buffer.read_int64()
        value = self._reverse_cached.get(value_hash)
        if value is None:
            value = pickle.loads(buffer.read_bytes_and_size())
            self._reverse_cached[value_hash] = value
        else:
            size = buffer.read_int32()
            buffer.skip(size)
        return value

    def cross_language_write(self, buffer, value):
        raise NotImplementedError

    def cross_language_read(self, buffer):
        raise NotImplementedError

    @staticmethod
    def new_classinfo(fury):
        from pyfury import ClassInfo

        return ClassInfo(
            PickleCacheStub,
            PICKLE_CACHE_CLASS_ID,
            serializer=PickleCacheSerializer(fury),
            class_name_bytes=b"PickleCacheStub",
        )


class PandasRangeIndexSerializer(Serializer):
    __slots__ = "_cached"

    def __init__(self, fury_):
        import pandas as pd

        super().__init__(fury_, pd.RangeIndex)

    def write(self, buffer, value):
        fury = self.fury_
        start = value.start
        stop = value.stop
        step = value.step
        if type(start) is int:
            buffer.write_int24(NOT_NULL_PYINT_FLAG)
            buffer.write_varint64(start)
        else:
            if start is None:
                buffer.write_int8(NULL_FLAG)
            else:
                buffer.write_int8(NOT_NULL_VALUE_FLAG)
                fury.serialize_non_referencable_to_py(buffer, start)
        if type(stop) is int:
            buffer.write_int24(NOT_NULL_PYINT_FLAG)
            buffer.write_varint64(stop)
        else:
            if stop is None:
                buffer.write_int8(NULL_FLAG)
            else:
                buffer.write_int8(NOT_NULL_VALUE_FLAG)
                fury.serialize_non_referencable_to_py(buffer, stop)
        if type(step) is int:
            buffer.write_int24(NOT_NULL_PYINT_FLAG)
            buffer.write_varint64(step)
        else:
            if step is None:
                buffer.write_int8(NULL_FLAG)
            else:
                buffer.write_int8(NOT_NULL_VALUE_FLAG)
                fury.serialize_non_referencable_to_py(buffer, step)
        fury.serialize_referencable_to_py(buffer, value.dtype)
        fury.serialize_referencable_to_py(buffer, value.name)

    def read(self, buffer):
        if buffer.read_int8() == NULL_FLAG:
            start = None
        else:
            start = self.fury_.deserialize_non_reference_from_py(buffer)
        if buffer.read_int8() == NULL_FLAG:
            stop = None
        else:
            stop = self.fury_.deserialize_non_reference_from_py(buffer)
        if buffer.read_int8() == NULL_FLAG:
            step = None
        else:
            step = self.fury_.deserialize_non_reference_from_py(buffer)
        dtype = self.fury_.deserialize_referencable_from_py(buffer)
        name = self.fury_.deserialize_referencable_from_py(buffer)
        return self.type_(start, stop, step, dtype=dtype, name=name)

    def cross_language_write(self, buffer, value):
        raise NotImplementedError

    def cross_language_read(self, buffer):
        raise NotImplementedError
