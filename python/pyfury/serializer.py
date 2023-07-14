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

    def __init__(self, fury, clear_threshold: int = 1000):
        super().__init__(fury, PickleStrongCacheStub)
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

    def xwrite(self, buffer, value):
        raise NotImplementedError

    def xread(self, buffer):
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

    def __init__(self, fury):
        super().__init__(fury, PickleCacheStub)
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

    def xwrite(self, buffer, value):
        raise NotImplementedError

    def xread(self, buffer):
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

    def __init__(self, fury):
        import pandas as pd

        super().__init__(fury, pd.RangeIndex)

    def write(self, buffer, value):
        fury = self.fury
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
                fury.serialize_nonref(buffer, start)
        if type(stop) is int:
            buffer.write_int24(NOT_NULL_PYINT_FLAG)
            buffer.write_varint64(stop)
        else:
            if stop is None:
                buffer.write_int8(NULL_FLAG)
            else:
                buffer.write_int8(NOT_NULL_VALUE_FLAG)
                fury.serialize_nonref(buffer, stop)
        if type(step) is int:
            buffer.write_int24(NOT_NULL_PYINT_FLAG)
            buffer.write_varint64(step)
        else:
            if step is None:
                buffer.write_int8(NULL_FLAG)
            else:
                buffer.write_int8(NOT_NULL_VALUE_FLAG)
                fury.serialize_nonref(buffer, step)
        fury.serialize_ref(buffer, value.dtype)
        fury.serialize_ref(buffer, value.name)

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
        dtype = self.fury.deserialize_ref(buffer)
        name = self.fury.deserialize_ref(buffer)
        return self.type_(start, stop, step, dtype=dtype, name=name)

    def xwrite(self, buffer, value):
        raise NotImplementedError

    def xread(self, buffer):
        raise NotImplementedError


_jit_context = locals()


_ENABLE_FURY_PYTHON_JIT = os.environ.get("ENABLE_FURY_PYTHON_JIT", "True").lower() in (
    "true",
    "1",
)


class DataClassSerializer(Serializer):
    def __init__(self, fury, clz: type):
        super().__init__(fury, clz)
        # This will get superclass type hints too.
        self._type_hints = typing.get_type_hints(clz)
        self._field_names = sorted(self._type_hints.keys())
        # TODO compute hash
        self._hash = len(self._field_names)
        self._generated_write_method = self._gen_write_method()
        self._generated_read_method = self._gen_read_method()
        if _ENABLE_FURY_PYTHON_JIT:
            # don't use `__slots__`, which will make instance method readonly
            self.write = self._gen_write_method()
            self.read = self._gen_read_method()

    def _gen_write_method(self):
        context = {}
        counter = itertools.count(0)
        buffer, fury, value = "buffer", "fury", "value"
        context[fury] = self.fury
        stmts = [
            f'"""write method for {self.type_}"""',
            f"{buffer}.write_int32({self._hash})",
        ]
        for field_name in self._field_names:
            field_type = self._type_hints[field_name]
            field_value = f"field_value{next(counter)}"
            stmts.append(f"{field_value} = {value}.{field_name}")
            if field_type is bool:
                stmts.extend(gen_write_nullable_basic_stmts(buffer, field_value, bool))
            elif field_type == int:
                stmts.extend(gen_write_nullable_basic_stmts(buffer, field_value, int))
            elif field_type == float:
                stmts.extend(gen_write_nullable_basic_stmts(buffer, field_value, float))
            elif field_type == str:
                stmts.extend(gen_write_nullable_basic_stmts(buffer, field_value, str))
            else:
                stmts.append(f"{fury}.write_ref_pyobject({buffer}, {field_value})")
        self._write_method_code, func = compile_function(
            f"write_{self.type_.__module__}_{self.type_.__qualname__}".replace(
                ".", "_"
            ),
            [buffer, value],
            stmts,
            context,
        )
        return func

    def _gen_read_method(self):
        context = dict(_jit_context)
        buffer, fury, obj_class, obj = "buffer", "fury", "obj_class", "obj"
        ref_resolver = "ref_resolver"
        context[fury] = self.fury
        context[obj_class] = self.type_
        context[ref_resolver] = self.fury.ref_resolver
        stmts = [
            f'"""read method for {self.type_}"""',
            f"{obj} = {obj_class}.__new__({obj_class})",
            f"{ref_resolver}.reference({obj})",
            f"read_hash = {buffer}.read_int32()",
            f"if read_hash != {self._hash}:",
            f"""   raise ClassNotCompatibleError(
            "Hash read_hash is not consistent with {self._hash} for {self.type_}")""",
        ]

        def set_action(value: str):
            return f"{obj}.{field_name} = {value}"

        for field_name in self._field_names:
            field_type = self._type_hints[field_name]
            if field_type is bool:
                stmts.extend(gen_read_nullable_basic_stmts(buffer, bool, set_action))
            elif field_type == int:
                stmts.extend(gen_read_nullable_basic_stmts(buffer, int, set_action))
            elif field_type == float:
                stmts.extend(gen_read_nullable_basic_stmts(buffer, float, set_action))
            elif field_type == str:
                stmts.extend(gen_read_nullable_basic_stmts(buffer, str, set_action))
            else:
                stmts.append(f"{obj}.{field_name} = {fury}.read_ref_pyobject({buffer})")
        stmts.append(f"return {obj}")
        self._read_method_code, func = compile_function(
            f"read_{self.type_.__module__}_{self.type_.__qualname__}".replace(".", "_"),
            [buffer],
            stmts,
            context,
        )
        return func

    def write(self, buffer, value):
        buffer.write_int32(self._hash)
        for field_name in self._field_names:
            field_value = getattr(value, field_name)
            self.fury.serialize_ref(buffer, field_value)

    def read(self, buffer):
        hash_ = buffer.read_int32()
        if hash_ != self._hash:
            raise ClassNotCompatibleError(
                f"Hash {hash_} is not consistent with {self._hash} "
                f"for class {self.type_}",
            )
        obj = self.type_.__new__(self.type_)
        self.fury.ref_resolver.reference(obj)
        for field_name in self._field_names:
            field_value = self.fury.deserialize_ref(buffer)
            setattr(
                obj,
                field_name,
                field_value,
            )
        return obj

    def xwrite(self, buffer: Buffer, value):
        raise NotImplementedError

    def xread(self, buffer):
        raise NotImplementedError
