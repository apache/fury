# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3
# cython: annotate = True
import array
import dataclasses
import datetime
import enum
import logging
import os
import sys
import warnings
from typing import TypeVar, Union, Iterable, get_type_hints

from pyfury._util import get_bit, set_bit, clear_bit
from pyfury._fury import Language, OpaqueObject
from pyfury._fury import _PicklerStub, _UnpicklerStub
from pyfury._fury import _ENABLE_CLASS_REGISTRATION_FORCIBLY
from pyfury.error import ClassNotCompatibleError
from pyfury.lib import mmh3
from pyfury.type import is_primitive_type, FuryType, Int8Type, Int16Type, Int32Type, \
    Int64Type, Float32Type, Float64Type, Int16ArrayType, Int32ArrayType, \
    Int64ArrayType, Float32ArrayType, Float64ArrayType, infer_field, load_class
from pyfury.util import is_little_endian

from libc.stdint cimport *
from libcpp.vector cimport vector
from cpython cimport PyObject
from cpython.ref cimport *
from libcpp cimport bool as c_bool
from cython.operator cimport dereference as deref
from pyfury._util cimport Buffer
from pyfury.includes.libabsl cimport flat_hash_map

try:
    import numpy as np
except ImportError:
    np = None

if sys.version_info[:2] < (3, 8):  # pragma: no cover
    import pickle5 as pickle  # nosec  # pylint: disable=import_pickle
else:
    import pickle  # nosec  # pylint: disable=import_pickle

cimport cython

logger = logging.getLogger(__name__)
ENABLE_FURY_CYTHON_SERIALIZATION = os.environ.get(
    "ENABLE_FURY_CYTHON_SERIALIZATION", "True").lower() in ("true", "1")


cdef int8_t NULL_FLAG = -3
# This flag indicates that object is a not-null value.
# We don't use another byte to indicate REF, so that we can save one byte.
cdef int8_t REF_FLAG = -2
# this flag indicates that the object is a non-null value.
cdef int8_t NOT_NULL_VALUE_FLAG = -1
# this flag indicates that the object is a referencable and first read.
cdef int8_t REF_VALUE_FLAG = 0


@cython.final
cdef class MapRefResolver:
    cdef flat_hash_map[uint64_t, int32_t] written_objects_id  # id(obj) -> ref_id
    # Hold object to avoid tmp object gc when serialize nested fields/objects.
    cdef vector[PyObject*] written_objects
    cdef vector[PyObject*] read_objects
    cdef vector[int32_t] read_ref_ids
    cdef object read_object
    cdef c_bool ref_tracking

    def __cinit__(self, c_bool ref_tracking):
        self.read_object = None
        self.ref_tracking = ref_tracking

    # Special methods of extension types must be declared with def, not cdef.
    def __dealloc__(self):
        self.reset()

    cpdef inline c_bool write_ref_or_null(self, Buffer buffer, obj):
        if not self.ref_tracking:
            if obj is None:
                buffer.write_int8(NULL_FLAG)
                return True
            else:
                buffer.write_int8(NOT_NULL_VALUE_FLAG)
                return False
        if obj is None:
            buffer.write_int8(NULL_FLAG)
            return True
        cdef uint64_t object_id = <uintptr_t><PyObject*>obj
        cdef int32_t next_id
        cdef flat_hash_map[uint64_t, int32_t].iterator it = \
            self.written_objects_id.find(object_id)
        if it == self.written_objects_id.end():
            next_id = self.written_objects_id.size()
            self.written_objects_id[object_id] = next_id
            self.written_objects.push_back(<PyObject*>obj)
            Py_INCREF(obj)
            buffer.write_int8(REF_VALUE_FLAG)
            return False
        else:
            # The obj has been written previously.
            buffer.write_int8(REF_FLAG)
            buffer.write_varint32(<uint64_t>deref(it).second)
            return True

    cpdef inline int8_t read_ref_or_null(self, Buffer buffer):
        cdef int8_t head_flag = buffer.read_int8()
        if not self.ref_tracking:
            return head_flag
        cdef int32_t ref_id
        if head_flag == REF_FLAG:
            # read reference id and get object from reference resolver
            ref_id = buffer.read_varint32()
            self.read_object = <object>(self.read_objects[ref_id])
            return REF_FLAG
        else:
            self.read_object = None
            return head_flag

    cpdef inline int32_t preserve_ref_id(self):
        if not self.ref_tracking:
            return -1
        next_read_ref_id = self.read_objects.size()
        self.read_objects.push_back(NULL)
        self.read_ref_ids.push_back(next_read_ref_id)
        return next_read_ref_id

    cpdef inline int32_t try_preserve_ref_id(self, Buffer buffer):
        if not self.ref_tracking:
            # `NOT_NULL_VALUE_FLAG` can be used as stub reference id because we use
            # `refId >= NOT_NULL_VALUE_FLAG` to read data.
            return buffer.read_int8()
        head_flag = buffer.read_int8()
        if head_flag == REF_FLAG:
            # read reference id and get object from reference resolver
            ref_id = buffer.read_varint32()
            self.read_object = <object>(self.read_objects[ref_id])
            # `head_flag` except `REF_FLAG` can be used as stub reference id because
            # we use `refId >= NOT_NULL_VALUE_FLAG` to read data.
            return head_flag
        else:
            self.read_object = None
            if head_flag == REF_VALUE_FLAG:
                return self.preserve_ref_id()
            return head_flag

    cpdef inline reference(self, obj):
        if not self.ref_tracking:
            return
        cdef int32_t ref_id = self.read_ref_ids.back()
        self.read_ref_ids.pop_back()
        cdef c_bool need_inc = self.read_objects[ref_id] == NULL
        if need_inc:
            Py_INCREF(obj)
        self.read_objects[ref_id] = <PyObject*>obj

    cpdef inline get_read_object(self, id_=None):
        if not self.ref_tracking:
            return None
        if id_ is None:
            return self.read_object
        cdef int32_t ref_id = id_
        return <object>(self.read_objects[ref_id])

    cpdef inline set_read_object(self, int32_t ref_id, obj):
        if not self.ref_tracking:
            return
        if ref_id >= 0:
            need_inc = self.read_objects[ref_id] == NULL
            if need_inc:
                Py_INCREF(obj)
            self.read_objects[ref_id] = <PyObject*>obj

    cpdef inline reset(self):
        self.reset_write()
        self.reset_read()

    cpdef inline reset_write(self):
        self.written_objects_id.clear()
        for item in self.written_objects:
            Py_XDECREF(item)
        self.written_objects.clear()

    cpdef inline reset_read(self):
        if not self.ref_tracking:
            return
        for item in self.read_objects:
            Py_XDECREF(item)
        self.read_objects.clear()
        self.read_ref_ids.clear()
        self.read_object = None


cdef int8_t NOT_SUPPORT_CROSS_LANGUAGE = 0
cdef int8_t USE_CLASSNAME = 0
cdef int8_t USE_CLASS_ID = 1
# preserve 0 as flag for class id not set in ClassInfo`
cdef int8_t NO_CLASS_ID = 0
cdef int8_t DEFAULT_DYNAMIC_WRITE_STRING_ID = -1
cdef int8_t PYINT_CLASS_ID = 1
cdef int8_t PYFLOAT_CLASS_ID = 2
cdef int8_t PYBOOL_CLASS_ID = 3
cdef int8_t STRING_CLASS_ID = 4
cdef int8_t PICKLE_CLASS_ID = 5
cdef int8_t PICKLE_STRONG_CACHE_CLASS_ID = 6
cdef int8_t PICKLE_CACHE_CLASS_ID = 7
# `NOT_NULL_VALUE_FLAG` + `CLASS_ID` in little-endian order
cdef int32_t NOT_NULL_PYINT_FLAG = NOT_NULL_VALUE_FLAG & 0b11111111 | \
                                   (PYINT_CLASS_ID << 8)
cdef int32_t NOT_NULL_PYFLOAT_FLAG = NOT_NULL_VALUE_FLAG & 0b11111111 | \
                                     (PYFLOAT_CLASS_ID << 8)
cdef int32_t NOT_NULL_PYBOOL_FLAG = NOT_NULL_VALUE_FLAG & 0b11111111 | \
                                    (PYBOOL_CLASS_ID << 8)
cdef int32_t NOT_NULL_STRING_FLAG = NOT_NULL_VALUE_FLAG & 0b11111111 | \
                                    (STRING_CLASS_ID << 8)


cdef class BufferObject:
    """
    Fury binary representation of an object.
    Note: This class is used for zero-copy out-of-band serialization and shouldn't be
    used for any other cases.
    """

    cpdef int32_t total_bytes(self):
        """total size for serialized bytes of an object"""
        raise NotImplementedError

    cpdef write_to(self, Buffer buffer):
        """Write serialized object to a buffer."""
        raise NotImplementedError

    cpdef Buffer to_buffer(self):
        """Write serialized data as Buffer."""
        raise NotImplementedError


@cython.final
cdef class BytesBufferObject(BufferObject):
    cdef public bytes binary

    def __init__(self, bytes binary):
        self.binary = binary

    cpdef inline int32_t total_bytes(self):
        return len(self.binary)

    cpdef inline write_to(self, Buffer buffer):
        buffer.write_bytes(self.binary)

    cpdef inline Buffer to_buffer(self):
        return Buffer(self.binary)


@cython.final
cdef class _PickleStub:
    pass


@cython.final
cdef class PickleStrongCacheStub:
    pass


@cython.final
cdef class PickleCacheStub:
    pass


@cython.final
cdef class ClassResolver:
    cdef:
        readonly Fury fury
        dict _type_id_to_class  # Dict[int, type]
        dict _type_id_to_serializer  # Dict[int, Serializer]
        dict _type_id_and_cls_to_serializer  # Dict[Tuple[int, type], Serializer]
        dict _type_tag_to_class_x_lang_map
        int16_t _class_id_counter
        set _used_classes_id

        public list _registered_id2_class_info
        vector[PyObject*] _c_registered_id2_class_info

        # cls -> ClassInfo
        flat_hash_map[uint64_t, PyObject*] _c_classes_info
        # hash -> ClassInfo
        flat_hash_map[int64_t, PyObject*] _c_hash_to_classinfo
        # hash -> EnumStringBytes
        flat_hash_map[int64_t, PyObject*] _c_hash_to_enum_string_bytes
        # classname EnumStringBytes address -> class
        flat_hash_map[uint64_t, PyObject*] _c_str_bytes_to_class
        # classname EnumStringBytes address -> str
        flat_hash_map[uint64_t, PyObject*] _c_enum_str_to_str

        int16_t dynamic_write_string_id
        vector[PyObject*] _c_dynamic_written_enum_string
        vector[PyObject*] _c_dynamic_id_to_enum_string_vec
        vector[PyObject*] _c_dynamic_id_to_classinfo_vec
        Serializer _serializer

        # hold objects to avoid gc, since flat_hash_map/vector doesn't
        # hold python reference.
        public dict _classes_info  # Dict[type, "ClassInfo"]
        set _class_set
        set _classname_set
        set _enum_str_set

    def __init__(self, fury):
        self.fury = fury
        self._type_id_to_class = dict()
        self._type_id_to_serializer = dict()
        self._type_id_and_cls_to_serializer = dict()
        self._type_tag_to_class_x_lang_map = dict()
        self._class_id_counter = PICKLE_CACHE_CLASS_ID + 1
        self._used_classes_id = set()
        self._registered_id2_class_info = list()

        self.dynamic_write_string_id = 0
        self._serializer = None

        self._classes_info = dict()
        self._class_set = set()
        self._classname_set = set()
        self._enum_str_set = set()

    def initialize(self):
        self.register_class(int, PYINT_CLASS_ID)
        self.register_class(float, PYFLOAT_CLASS_ID)
        self.register_class(bool, PYBOOL_CLASS_ID)
        self.register_class(str, STRING_CLASS_ID)
        self.register_class(_PickleStub, PICKLE_CLASS_ID)
        self.register_class(PickleStrongCacheStub, PICKLE_STRONG_CACHE_CLASS_ID)
        self.register_class(PickleCacheStub, PICKLE_CACHE_CLASS_ID)
        self._add_default_serializers()

    def register_serializer(self, cls: Union[type, TypeVar], serializer):
        assert isinstance(cls, (type, TypeVar)), cls
        type_id = serializer.get_xtype_id()
        if type_id != NOT_SUPPORT_CROSS_LANGUAGE:
            self._add_x_lang_serializer(cls, serializer=serializer)
        else:
            self.register_class(cls)
            self._classes_info[cls].serializer = serializer

    def register_class(self, cls: Union[type, TypeVar], class_id: int = None):
        classinfo = self._classes_info.get(cls)
        if classinfo is None:
            if isinstance(cls, TypeVar):
                class_name_bytes = (cls.__module__ + "#" + cls.__name__).encode("utf-8")
            else:
                class_name_bytes = (cls.__module__ + "#" + cls.__qualname__) \
                    .encode("utf-8")
            class_id = class_id if class_id is not None else self._next_class_id()
            assert class_id not in self._used_classes_id, (
                self._used_classes_id,
                self._classes_info,
            )
            classinfo = ClassInfo(
                cls=cls, class_name_bytes=class_name_bytes, class_id=class_id
            )
            self._classes_info[cls] = classinfo
            self._c_classes_info[<uintptr_t><PyObject*>cls] = <PyObject*>classinfo
            if len(self._registered_id2_class_info) <= class_id:
                self._registered_id2_class_info.extend(
                    [None] * (class_id - len(self._registered_id2_class_info) + 1)
                )
            self._registered_id2_class_info[class_id] = classinfo
            self._c_registered_id2_class_info.resize(class_id + 1)
            self._c_registered_id2_class_info[class_id] = <PyObject*>classinfo
        else:
            if classinfo.class_id == NO_CLASS_ID:
                class_id = class_id if class_id is not None else self._next_class_id()
                assert class_id not in self._used_classes_id, (
                    self._used_classes_id,
                    self._classes_info,
                )
                classinfo.class_id = class_id
                if len(self._registered_id2_class_info) <= class_id:
                    self._registered_id2_class_info.extend(
                        [None] * (class_id - len(self._registered_id2_class_info) + 1)
                    )
                self._registered_id2_class_info[class_id] = classinfo
                self._c_registered_id2_class_info.resize(class_id + 1)
                self._c_registered_id2_class_info[class_id] = <PyObject*>classinfo
            else:
                if class_id is not None and classinfo.class_id != class_id:
                    raise ValueError(
                        f"Inconsistent class id {class_id} vs {classinfo.class_id} "
                        f"for class {cls}"
                    )

    def _next_class_id(self):
        class_id = self._class_id_counter = self._class_id_counter + 1
        while class_id in self._used_classes_id:
            class_id = self._class_id_counter = self._class_id_counter + 1
        return class_id

    def register_class_tag(self, cls: Union[type, TypeVar], type_tag: str = None):
        """Register class with given type tag which will be used for cross-language
        serialization."""
        self.register_serializer(
            cls, ComplexObjectSerializer(self.fury, cls, type_tag)
        )

    def _add_serializer(
            self,
            cls: Union[type, TypeVar],
            serializer=None,
            serializer_cls=None):
        if serializer_cls:
            serializer = serializer_cls(self.fury, cls)
        self.register_serializer(cls, serializer)

    def _add_x_lang_serializer(self,
                               cls: Union[type, TypeVar],
                               serializer=None,
                               serializer_cls=None):
        if serializer_cls:
            serializer = serializer_cls(self.fury, cls)
        type_id = serializer.get_xtype_id()

        assert type_id != NOT_SUPPORT_CROSS_LANGUAGE
        self._type_id_and_cls_to_serializer[(type_id, cls)] = serializer
        self.register_class(cls)
        classinfo = self._classes_info[cls]
        classinfo.serializer = serializer
        if type_id == FuryType.FURY_TYPE_TAG.value:
            type_tag = serializer.get_xtype_tag()
            assert type(type_tag) is str
            assert type_tag not in self._type_tag_to_class_x_lang_map
            classinfo.type_tag_bytes = EnumStringBytes(type_tag.encode("utf-8"))
            self._type_tag_to_class_x_lang_map[type_tag] = cls
        else:
            self._type_id_to_serializer[type_id] = serializer
            if type_id > NOT_SUPPORT_CROSS_LANGUAGE:
                self._type_id_to_class[type_id] = cls

    def _add_default_serializers(self):
        self._add_x_lang_serializer(int, serializer_cls=ByteSerializer)
        self._add_x_lang_serializer(int, serializer_cls=Int16Serializer)
        self._add_x_lang_serializer(int, serializer_cls=Int32Serializer)
        self._add_x_lang_serializer(int, serializer_cls=Int64Serializer)
        self._add_x_lang_serializer(float, serializer_cls=FloatSerializer)
        self._add_x_lang_serializer(float, serializer_cls=DoubleSerializer)
        self._add_serializer(type(None), serializer_cls=NoneSerializer)
        self._add_serializer(bool, serializer_cls=BooleanSerializer)
        self._add_serializer(Int8Type, serializer_cls=ByteSerializer)
        self._add_serializer(Int16Type, serializer_cls=Int16Serializer)
        self._add_serializer(Int32Type, serializer_cls=Int32Serializer)
        self._add_serializer(Int64Type, serializer_cls=Int64Serializer)
        self._add_serializer(Float32Type, serializer_cls=FloatSerializer)
        self._add_serializer(Float64Type, serializer_cls=DoubleSerializer)
        self._add_serializer(str, serializer_cls=StringSerializer)
        self._add_serializer(datetime.date, serializer_cls=DateSerializer)
        self._add_serializer(
            datetime.datetime, serializer_cls=TimestampSerializer
        )
        self._add_serializer(bytes, serializer_cls=BytesSerializer)
        self._add_serializer(list, serializer_cls=ListSerializer)
        self._add_serializer(tuple, serializer_cls=TupleSerializer)
        self._add_serializer(dict, serializer_cls=MapSerializer)
        self._add_serializer(set, serializer_cls=SetSerializer)
        self._add_serializer(enum.Enum, serializer_cls=EnumSerializer)
        self._add_serializer(slice, serializer_cls=SliceSerializer)
        from pyfury import PickleCacheSerializer, PickleStrongCacheSerializer

        self._add_serializer(PickleStrongCacheStub,
                             serializer=PickleStrongCacheSerializer(self.fury))
        self._add_serializer(PickleCacheStub,
                             serializer=PickleCacheSerializer(self.fury))

        try:
            import pyarrow as pa
            from pyfury.format.serializer import (
                ArrowRecordBatchSerializer,
                ArrowTableSerializer,
            )

            self._add_serializer(
                pa.RecordBatch, serializer_cls=ArrowRecordBatchSerializer
            )
            self._add_serializer(pa.Table, serializer_cls=ArrowTableSerializer)
        except Exception:
            pass
        for typecode in PyArraySerializer.typecode_dict.keys():
            self._add_serializer(
                array.array,
                serializer=PyArraySerializer(self.fury, array.array, typecode),
            )
            self._add_serializer(
                PyArraySerializer.typecodearray_type[typecode],
                serializer=PyArraySerializer(self.fury, array.array, typecode),
            )
        if np:
            for dtype in Numpy1DArraySerializer.dtypes_dict.keys():
                self._add_serializer(
                    np.ndarray,
                    serializer=Numpy1DArraySerializer(self.fury, array.array, dtype),
                )

    cpdef inline Serializer get_serializer(self, cls=None, type_id=None, obj=None):
        """
        Returns
        -------
            Returns or create serializer for the provided class
        """
        assert cls is not None or type_id is not None or obj is not None

        cdef Serializer serializer_
        if obj is not None:
            cls = type(obj)
            if cls is int and 2**63 - 1 >= obj >= -(2**63):
                type_id = FuryType.INT64.value
            elif cls is float:
                type_id = FuryType.DOUBLE.value
            elif cls is array.array:
                info = PyArraySerializer.typecode_dict.get(obj.typecode)
                if info is not None:
                    type_id = info[1]
            elif np and cls is np.ndarray and obj.ndim == 1:
                info = Numpy1DArraySerializer.dtypes_dict.get(obj.dtype)
                if info:
                    type_id = info[2]
        if type_id is not None:
            if cls is not None:
                serializer_ = self._type_id_and_cls_to_serializer[(type_id, cls)]
            else:
                serializer_ = self._type_id_to_serializer[type_id]
        else:
            class_info = self._classes_info.get(cls)
            if class_info is not None:
                serializer_ = class_info.serializer
            else:
                self._add_serializer(cls, serializer=self.get_or_create_serializer(cls))
                serializer_ = self._classes_info.get(cls).serializer
        self._serializer = serializer_
        return serializer_

    cpdef inline Serializer get_or_create_serializer(self, cls):
        return self.get_or_create_classinfo(cls).serializer

    cpdef inline ClassInfo get_or_create_classinfo(self, cls):
        cdef PyObject* classinfo_ptr = self._c_classes_info[<uintptr_t><PyObject*>cls]
        cdef ClassInfo class_info
        if classinfo_ptr != NULL:
            class_info = <object>classinfo_ptr
            if class_info.serializer is not None:
                return class_info
            else:
                class_info.serializer = self._create_serializer(cls)
                return class_info
        else:
            serializer = self._create_serializer(cls)
            if type(serializer) is PickleSerializer:
                class_id = PICKLE_CLASS_ID
            else:
                class_id = NO_CLASS_ID
            class_name_bytes = (cls.__module__ + "#" + cls.__qualname__).encode("utf-8")
            class_info = ClassInfo(
                cls=cls, class_name_bytes=class_name_bytes,
                serializer=serializer, class_id=class_id
            )
            self._classes_info[cls] = class_info
            self._c_classes_info[<uintptr_t><PyObject*>cls] = <PyObject*>class_info
            return class_info

    cdef _create_serializer(self, cls):
        mro = cls.__mro__
        classinfo_ = self._classes_info.get(cls)
        for clz in mro:
            class_info = self._classes_info.get(clz)
            if (
                    class_info
                    and class_info.serializer
                    and class_info.serializer.support_subclass()
            ):
                if classinfo_ is None or classinfo_.class_id == NO_CLASS_ID:
                    logger.info("Class %s not registered", cls)
                serializer = type(class_info.serializer)(self.fury, cls)
                break
        else:
            if dataclasses.is_dataclass(cls):
                if classinfo_ is None or classinfo_.class_id == NO_CLASS_ID:
                    logger.info("Class %s not registered", cls)
                from pyfury import DataClassSerializer

                serializer = DataClassSerializer(self.fury, cls)
            else:
                serializer = PickleSerializer(self.fury, cls)
        return serializer

    cpdef inline write_classinfo(self, Buffer buffer, ClassInfo classinfo):
        cdef int16_t class_id = classinfo.class_id
        if class_id != NO_CLASS_ID:
            buffer.write_int16(class_id)
            return
        buffer.write_int16(NO_CLASS_ID)
        self._write_enum_string_bytes(buffer, classinfo.class_name_bytes)

    cpdef inline ClassInfo read_classinfo(self, Buffer buffer):
        cdef int16_t class_id = buffer.read_int16()
        cdef ClassInfo classinfo
        cdef PyObject* classinfo_ptr
        # registered class id are greater than `NO_CLASS_ID`.
        if class_id > NO_CLASS_ID:
            classinfo_ptr = self._c_registered_id2_class_info[class_id]
            if classinfo_ptr == NULL:
                raise ValueError(f"Unexpected class_id {class_id} "
                                 f"{self._registered_id2_class_info}")
            classinfo = <ClassInfo>classinfo_ptr
            if classinfo.serializer is None:
                classinfo.serializer = self._create_serializer(classinfo.cls)
            return classinfo
        if buffer.read_int8() == USE_CLASS_ID:
            return <ClassInfo>self._c_dynamic_id_to_classinfo_vec[buffer.read_int16()]
        cdef int64_t class_name_bytes_hash = buffer.read_int64()
        cdef int16_t class_name_bytes_length = buffer.read_int16()
        cdef int32_t reader_index = buffer.reader_index
        buffer.check_bound(reader_index, class_name_bytes_length)
        buffer.reader_index = reader_index + class_name_bytes_length
        classinfo_ptr = self._c_hash_to_classinfo[class_name_bytes_hash]
        if classinfo_ptr != NULL:
            self._c_dynamic_id_to_classinfo_vec.push_back(classinfo_ptr)
            return <ClassInfo>classinfo_ptr
        cdef bytes classname_bytes = buffer.get_bytes(
            reader_index, class_name_bytes_length)
        cdef str full_class_name = classname_bytes.decode(encoding="utf-8")
        cls = load_class(full_class_name)
        classinfo = self.get_or_create_classinfo(cls)
        classinfo_ptr = <PyObject*>classinfo
        self._c_hash_to_classinfo[class_name_bytes_hash] = classinfo_ptr
        self._c_dynamic_id_to_classinfo_vec.push_back(classinfo_ptr)
        return classinfo

    cdef inline _write_enum_string_bytes(
            self, Buffer buffer, EnumStringBytes enum_string_bytes):
        cdef int16_t dynamic_class_id = enum_string_bytes.dynamic_write_string_id
        if dynamic_class_id == DEFAULT_DYNAMIC_WRITE_STRING_ID:
            dynamic_class_id = self.dynamic_write_string_id
            enum_string_bytes.dynamic_write_string_id = dynamic_class_id
            self.dynamic_write_string_id += 1
            self._c_dynamic_written_enum_string.push_back(<PyObject*>enum_string_bytes)
            buffer.write_int8(USE_CLASSNAME)
            buffer.write_int64(enum_string_bytes.hashcode)
            buffer.write_int16(enum_string_bytes.length)
            buffer.write_bytes(enum_string_bytes.data)
        else:
            buffer.write_int8(USE_CLASS_ID)
            buffer.write_int16(dynamic_class_id)

    cdef inline EnumStringBytes _read_enum_string_bytes(self, Buffer buffer):
        if buffer.read_int8() != USE_CLASSNAME:
            return <EnumStringBytes>self._c_dynamic_id_to_enum_string_vec[
                buffer.read_int16()]
        cdef int64_t hashcode = buffer.read_int64()
        cdef int16_t length = buffer.read_int16()
        cdef int32_t reader_index = buffer.reader_index
        buffer.check_bound(reader_index, length)
        buffer.reader_index = reader_index + length
        cdef PyObject* enum_str_ptr = self._c_hash_to_enum_string_bytes[hashcode]
        if enum_str_ptr != NULL:
            self._c_dynamic_id_to_enum_string_vec.push_back(enum_str_ptr)
            return <EnumStringBytes>enum_str_ptr
        cdef bytes str_bytes = buffer.get_bytes(reader_index, length)
        cdef EnumStringBytes enum_str = EnumStringBytes(str_bytes, hashcode=hashcode)
        self._enum_str_set.add(enum_str)
        enum_str_ptr = <PyObject*>enum_str
        self._c_hash_to_enum_string_bytes[hashcode] = enum_str_ptr
        self._c_dynamic_id_to_enum_string_vec.push_back(enum_str_ptr)
        return enum_str

    cpdef inline xwrite_class(self, Buffer buffer, cls):
        cdef PyObject* classinfo_ptr = self._c_classes_info[<uintptr_t><PyObject*>cls]
        assert classinfo_ptr != NULL
        cdef EnumStringBytes class_name_bytes = (<object>classinfo_ptr).class_name_bytes
        self._write_enum_string_bytes(buffer, class_name_bytes)

    cpdef inline xwrite_type_tag(self, Buffer buffer, cls):
        cdef PyObject* classinfo_ptr = self._c_classes_info[<uintptr_t><PyObject*>cls]
        assert classinfo_ptr != NULL
        cdef EnumStringBytes type_tag_bytes = (<object>classinfo_ptr).type_tag_bytes
        self._write_enum_string_bytes(buffer, type_tag_bytes)

    cpdef inline read_class_by_type_tag(self, Buffer buffer):
        tag = self.xread_classname(buffer)
        return self._type_tag_to_class_x_lang_map[tag]

    cpdef inline xread_class(self, Buffer buffer):
        cdef EnumStringBytes str_bytes = self._read_enum_string_bytes(buffer)
        cdef uint64_t object_id = <uintptr_t><PyObject*>str_bytes
        cdef PyObject* cls_ptr = self._c_str_bytes_to_class[object_id]
        if cls_ptr != NULL:
            return <object>cls_ptr
        cdef str full_class_name = str_bytes.data.decode(encoding="utf-8")
        cls = load_class(full_class_name)
        self._c_str_bytes_to_class[object_id] = <PyObject*>cls
        self._class_set.add(cls)
        return cls

    cpdef inline str xread_classname(self, Buffer buffer):
        cdef EnumStringBytes str_bytes = self._read_enum_string_bytes(buffer)
        cdef uint64_t object_id = <uintptr_t><PyObject*>str_bytes
        cdef PyObject* classname_ptr = self._c_enum_str_to_str[object_id]
        if classname_ptr != NULL:
            return <object>classname_ptr
        cdef str full_class_name = str_bytes.data.decode(encoding="utf-8")
        self._c_enum_str_to_str[object_id] = <PyObject*>full_class_name
        self._classname_set.add(full_class_name)
        return full_class_name

    cpdef inline get_class_by_type_id(self, int32_t type_id):
        return self._type_id_to_class[type_id]

    cpdef inline reset(self):
        self.reset_write()
        self.reset_read()

    cpdef inline reset_read(self):
        self._c_dynamic_id_to_enum_string_vec.clear()
        self._c_dynamic_id_to_classinfo_vec.clear()

    cpdef inline reset_write(self):
        if self.dynamic_write_string_id != 0:
            self.dynamic_write_string_id = 0
            for ptr in self._c_dynamic_written_enum_string:
                (<EnumStringBytes>ptr).dynamic_write_string_id = \
                    DEFAULT_DYNAMIC_WRITE_STRING_ID
            self._c_dynamic_written_enum_string.clear()


@cython.final
cdef class EnumStringBytes:
    cdef bytes data
    cdef int16_t length
    cdef int64_t hashcode
    cdef int16_t dynamic_write_string_id

    def __init__(self, data, hashcode=None):
        self.data = data
        self.length = len(data)
        self.hashcode = hashcode or mmh3.hash_buffer(data, 47)[0]
        self.dynamic_write_string_id = DEFAULT_DYNAMIC_WRITE_STRING_ID

    def __eq__(self, other):
        return type(other) is EnumStringBytes and other.hashcode == self.hashcode

    def __hash__(self):
        return self.hashcode


@cython.final
cdef class ClassInfo:
    cdef public object cls
    cdef public int16_t class_id
    cdef public Serializer serializer
    cdef public EnumStringBytes class_name_bytes
    cdef public EnumStringBytes type_tag_bytes

    def __init__(
            self,
            cls: Union[type, TypeVar] = None,
            class_id: int = NO_CLASS_ID,
            serializer: Serializer = None,
            class_name_bytes: bytes = None,
            type_tag_bytes: bytes = None,
    ):
        self.cls = cls
        self.class_id = class_id
        self.serializer = serializer
        self.class_name_bytes = EnumStringBytes(class_name_bytes)
        if type_tag_bytes is None:
            self.type_tag_bytes = None
        else:
            self.type_tag_bytes = EnumStringBytes(type_tag_bytes)

    def __repr__(self):
        return f"ClassInfo(cls={self.cls}, class_id={self.class_id}, " \
               f"serializer={self.serializer})"


@cython.final
cdef class Fury:
    cdef readonly object language
    cdef readonly c_bool ref_tracking
    cdef readonly c_bool require_class_registration
    cdef readonly MapRefResolver ref_resolver
    cdef readonly ClassResolver class_resolver
    cdef readonly SerializationContext serialization_context
    cdef Buffer buffer
    cdef public object pickler  # pickle.Pickler
    cdef public object unpickler  # Optional[pickle.Unpickler]
    cdef object _buffer_callback
    cdef object _buffers  # iterator
    cdef object _unsupported_callback
    cdef object _unsupported_objects  # iterator
    cdef object _peer_language
    cdef list _native_objects

    def __init__(
        self,
        language=Language.XLANG,
        ref_tracking: bool = False,
        require_class_registration: bool = True,
     ):
        """
       :param require_class_registration:
        Whether to require registering classes for serialization, enabled by default.
         If disabled, unknown insecure classes can be deserialized, which can be
         insecure and cause remote code execution attack if the classes
         `__new__`/`__init__`/`__eq__`/`__hash__` method contain malicious code.
         Do not disable class registration if you can't ensure your environment are
         *indeed secure*. We are not responsible for security risks if
         you disable this option.
       """
        self.language = language
        if _ENABLE_CLASS_REGISTRATION_FORCIBLY or require_class_registration:
            self.require_class_registration = True
        else:
            self.require_class_registration = False
        self.ref_tracking = ref_tracking
        self.ref_resolver = MapRefResolver(ref_tracking)
        self.class_resolver = ClassResolver(self)
        self.class_resolver.initialize()
        self.serialization_context = SerializationContext()
        self.buffer = Buffer.allocate(32)
        if not require_class_registration:
            warnings.warn(
                "Class registration is disabled, unknown classes can be deserialized "
                "which may be insecure.",
                RuntimeWarning,
                stacklevel=2,
            )
            self.pickler = pickle.Pickler(self.buffer)
        else:
            self.pickler = _PicklerStub(self.buffer)
        self.unpickler = None
        self._buffer_callback = None
        self._buffers = None
        self._unsupported_callback = None
        self._unsupported_objects = None
        self._peer_language = None
        self._native_objects = []

    def register_serializer(self, cls: Union[type, TypeVar], Serializer serializer):
        self.class_resolver.register_serializer(cls, serializer)

    def register_class(self, cls: Union[type, TypeVar], class_id: int = None):
        self.class_resolver.register_class(cls, class_id=class_id)

    def register_class_tag(self, cls: Union[type, TypeVar], type_tag: str = None):
        self.class_resolver.register_class_tag(cls, type_tag)

    def serialize(
            self, obj,
            Buffer buffer=None,
            buffer_callback=None,
            unsupported_callback=None
    ) -> Union[Buffer, bytes]:
        try:
            return self._serialize(
                obj,
                buffer,
                buffer_callback=buffer_callback,
                unsupported_callback=unsupported_callback)
        finally:
            self.reset_write()

    cpdef inline _serialize(
            self, obj, Buffer buffer, buffer_callback=None, unsupported_callback=None):
        self._buffer_callback = buffer_callback
        self._unsupported_callback = unsupported_callback
        if buffer is not None:
            self.pickler = pickle.Pickler(self.buffer)
        else:
            self.buffer.writer_index = 0
            buffer = self.buffer
        cdef int32_t mask_index = buffer.writer_index
        # 1byte used for bit mask
        buffer.grow(1)
        buffer.writer_index = mask_index + 1
        if obj is None:
            set_bit(buffer, mask_index, 0)
        else:
            clear_bit(buffer, mask_index, 0)
        # set endian
        if is_little_endian:
            set_bit(buffer, mask_index, 1)
        else:
            clear_bit(buffer, mask_index, 1)

        if self.language == Language.XLANG:
            # set reader as x_lang.
            set_bit(buffer, mask_index, 2)
            # set writer language.
            buffer.write_int8(Language.PYTHON.value)
        else:
            # set reader as native.
            clear_bit(buffer, mask_index, 2)
        if self._buffer_callback is not None:
            set_bit(buffer, mask_index, 3)
        else:
            clear_bit(buffer, mask_index, 3)
        cdef int32_t start_offset
        if self.language == Language.PYTHON:
            self.serialize_ref(buffer, obj)
        else:
            start_offset = buffer.writer_index
            # preserve 4-byte for nativeObjects start offsets.
            buffer.write_int32(<int32_t>-1)
            # preserve 4-byte for nativeObjects size
            buffer.write_int32(<int32_t>-1)
            self.xserialize_ref(buffer, obj)
            buffer.put_int32(start_offset, buffer.writer_index)
            buffer.put_int32(start_offset + 4, len(self._native_objects))
            self.ref_resolver.reset_write()
            # fury write opaque object classname which cause later write of classname
            # only write an id.
            self.class_resolver.reset_write()
            for native_object in self._native_objects:
                self.serialize_ref(buffer, native_object)
        self.reset_write()
        if buffer is not self.buffer:
            return buffer
        else:
            return buffer.to_bytes(0, buffer.writer_index)

    cpdef inline serialize_ref(
            self, Buffer buffer, obj, ClassInfo classinfo=None):
        cls = type(obj)
        if cls is str:
            buffer.write_int24(NOT_NULL_STRING_FLAG)
            buffer.write_string(obj)
            return
        elif cls is int:
            buffer.write_int24(NOT_NULL_PYINT_FLAG)
            buffer.write_varint64(obj)
            return
        elif cls is bool:
            buffer.write_int24(NOT_NULL_PYBOOL_FLAG)
            buffer.write_bool(obj)
            return
        elif cls is float:
            buffer.write_int24(NOT_NULL_PYFLOAT_FLAG)
            buffer.write_double(obj)
            return
        if self.ref_resolver.write_ref_or_null(buffer, obj):
            return
        if classinfo is None:
            classinfo = self.class_resolver.get_or_create_classinfo(cls)
        self.class_resolver.write_classinfo(buffer, classinfo)
        classinfo.serializer.write(buffer, obj)

    cpdef inline serialize_nonref(self, Buffer buffer, obj):
        cls = type(obj)
        if cls is str:
            buffer.write_int16(STRING_CLASS_ID)
            buffer.write_string(obj)
            return
        elif cls is int:
            buffer.write_int16(PYINT_CLASS_ID)
            buffer.write_varint64(obj)
            return
        elif cls is bool:
            buffer.write_int16(PYBOOL_CLASS_ID)
            buffer.write_bool(obj)
            return
        elif cls is float:
            buffer.write_int16(PYFLOAT_CLASS_ID)
            buffer.write_double(obj)
            return
        cdef ClassInfo classinfo = self.class_resolver.get_or_create_classinfo(cls)
        self.class_resolver.write_classinfo(buffer, classinfo)
        classinfo.serializer.write(buffer, obj)

    cpdef inline xserialize_ref(
            self, Buffer buffer, obj, Serializer serializer=None):
        if serializer is None or serializer.need_to_write_ref:
            if not self.ref_resolver.write_ref_or_null(buffer, obj):
                self.xserialize_nonref(
                    buffer, obj, serializer=serializer
                )
        else:
            if obj is None:
                buffer.write_int8(NULL_FLAG)
            else:
                buffer.write_int8(NOT_NULL_VALUE_FLAG)
                self.xserialize_nonref(
                    buffer, obj, serializer=serializer
                )

    cpdef inline xserialize_nonref(
            self, Buffer buffer, obj, Serializer serializer=None):
        cls = type(obj)
        serializer = serializer or self.class_resolver.get_serializer(obj=obj)
        cdef int16_t type_id = serializer.get_xtype_id()
        buffer.write_int16(type_id)
        if type_id != NOT_SUPPORT_CROSS_LANGUAGE:
            if type_id == FuryType.FURY_TYPE_TAG.value:
                self.class_resolver.xwrite_type_tag(buffer, cls)
            if type_id < NOT_SUPPORT_CROSS_LANGUAGE:
                self.class_resolver.xwrite_class(buffer, cls)
            serializer.xwrite(buffer, obj)
        else:
            # Write classname so it can be used for debugging which object doesn't
            # support cross-language.
            # TODO add a config to disable this to reduce space cost.
            self.class_resolver.xwrite_class(buffer, cls)
            # serializer may increase reference id multi times internally, thus peer
            # cross-language later fields/objects deserialization will use wrong
            # reference id since we skip opaque objects deserialization.
            # So we stash native objects and serialize all those object at the last.
            buffer.write_varint32(len(self._native_objects))
            self._native_objects.append(obj)

    def deserialize(
            self,
            buffer: Union[Buffer, bytes],
            buffers: Iterable = None,
            unsupported_objects: Iterable = None,
    ):
        try:
            if type(buffer) == bytes:
                buffer = Buffer(buffer)
            return self._deserialize(buffer, buffers, unsupported_objects)
        finally:
            self.reset_read()

    cpdef inline _deserialize(
            self, Buffer buffer, buffers=None, unsupported_objects=None):
        if self.require_class_registration:
            self.unpickler = _UnpicklerStub(buffer)
        else:
            self.unpickler = pickle.Unpickler(buffer)
        if unsupported_objects is not None:
            self._unsupported_objects = iter(unsupported_objects)
        cdef int32_t reader_index = buffer.reader_index
        buffer.reader_index = reader_index + 1
        if get_bit(buffer, reader_index, 0):
            return None
        cdef c_bool is_little_endian_ = get_bit(buffer, reader_index, 1)
        assert is_little_endian_, (
            "Big endian is not supported for now, "
            "please ensure peer machine is little endian."
        )
        cdef c_bool is_target_x_lang = get_bit(buffer, reader_index, 2)
        if is_target_x_lang:
            self._peer_language = Language(buffer.read_int8())
        else:
            self._peer_language = Language.PYTHON
        cdef c_bool is_out_of_band_serialization_enabled = \
            get_bit(buffer, reader_index, 3)
        if is_out_of_band_serialization_enabled:
            assert buffers is not None, (
                "buffers shouldn't be null when the serialized stream is "
                "produced with buffer_callback not null."
            )
            self._buffers = iter(buffers)
        else:
            assert buffers is None, (
                "buffers should be null when the serialized stream is "
                "produced with buffer_callback null."
            )
        if not is_target_x_lang:
            return self.deserialize_ref(buffer)
        cdef int32_t native_objects_start_offset = buffer.read_int32()
        cdef int32_t native_objects_size = buffer.read_int32()
        if self._peer_language == Language.PYTHON:
            if native_objects_size > 0:
                native_objects_buffer = buffer.slice(native_objects_start_offset)
                for i in range(native_objects_size):
                    self._native_objects.append(
                        self.deserialize_ref(native_objects_buffer)
                    )
                self.ref_resolver.reset_read()
                self.class_resolver.reset_read()
        return self.xdeserialize_ref(buffer)

    cpdef inline deserialize_ref(self, Buffer buffer):
        cdef MapRefResolver ref_resolver = self.ref_resolver
        cdef int32_t ref_id = ref_resolver.try_preserve_ref_id(buffer)
        if ref_id < NOT_NULL_VALUE_FLAG:
            return ref_resolver.get_read_object()
        # indicates that the object is first read.
        cdef ClassInfo classinfo = self.class_resolver.read_classinfo(buffer)
        cls = classinfo.cls
        if cls is str:
            return buffer.read_string()
        elif cls is int:
            return buffer.read_varint64()
        elif cls is bool:
            return buffer.read_bool()
        elif cls is float:
            return buffer.read_double()
        o = classinfo.serializer.read(buffer)
        ref_resolver.set_read_object(ref_id, o)
        return o

    cpdef inline deserialize_nonref(self, Buffer buffer):
        """Deserialize not-null and non-reference object from buffer."""
        cdef ClassInfo classinfo = self.class_resolver.read_classinfo(buffer)
        cls = classinfo.cls
        if cls is str:
            return buffer.read_string()
        elif cls is int:
            return buffer.read_varint64()
        elif cls is bool:
            return buffer.read_bool()
        elif cls is float:
            return buffer.read_double()
        return classinfo.serializer.read(buffer)

    cpdef inline xdeserialize_ref(
            self, Buffer buffer, Serializer serializer=None):
        cdef MapRefResolver ref_resolver
        cdef int32_t red_id
        if serializer is None or serializer.need_to_write_ref:
            ref_resolver = self.ref_resolver
            red_id = ref_resolver.try_preserve_ref_id(buffer)

            # indicates that the object is first read.
            if red_id >= NOT_NULL_VALUE_FLAG:
                o = self.xdeserialize_nonref(
                    buffer, serializer=serializer
                )
                ref_resolver.set_read_object(red_id, o)
                return o
            else:
                return ref_resolver.get_read_object()
        cdef int8_t head_flag = buffer.read_int8()
        if head_flag == NULL_FLAG:
            return None
        return self.xdeserialize_nonref(
            buffer, serializer=serializer
        )

    cpdef inline xdeserialize_nonref(
            self, Buffer buffer, Serializer serializer=None):
        cdef int16_t type_id = buffer.read_int16()
        cls = None
        if type_id != NOT_SUPPORT_CROSS_LANGUAGE:
            if type_id == FuryType.FURY_TYPE_TAG.value:
                cls = self.class_resolver.read_class_by_type_tag(buffer)
            if type_id < NOT_SUPPORT_CROSS_LANGUAGE:
                if self._peer_language is not Language.PYTHON:
                    self.class_resolver.xread_classname(buffer)
                    cls = self.class_resolver.get_class_by_type_id(-type_id)
                    serializer = serializer or self.class_resolver.get_serializer(
                        type_id=-type_id
                    )
                else:
                    cls = self.class_resolver.xread_class(buffer)
                    serializer = serializer or self.class_resolver.get_serializer(
                        cls=cls, type_id=type_id
                    )
            else:
                if type_id != FuryType.FURY_TYPE_TAG.value:
                    cls = self.class_resolver.get_class_by_type_id(type_id)
                serializer = serializer or self.class_resolver.get_serializer(
                    cls=cls, type_id=type_id
                )
            assert cls is not None
            return serializer.xread(buffer)
        cdef str class_name = self.class_resolver.xread_classname(buffer)
        cdef int32_t ordinal = buffer.read_varint32()
        if self._peer_language != Language.PYTHON:
            return OpaqueObject(self._peer_language, class_name, ordinal)
        else:
            return self._native_objects[ordinal]

    cpdef inline write_buffer_object(self, Buffer buffer, BufferObject buffer_object):
        if self._buffer_callback is not None and self._buffer_callback(buffer_object):
            buffer.write_bool(False)
            return
        buffer.write_bool(True)
        cdef int32_t size = buffer_object.total_bytes()
        # writer length.
        buffer.write_varint32(size)
        cdef int32_t writer_index = buffer.writer_index
        buffer.ensure(writer_index + size)
        cdef Buffer buf = buffer.slice(buffer.writer_index, size)
        buffer_object.write_to(buf)
        buffer.writer_index += size

    cpdef inline Buffer read_buffer_object(self, Buffer buffer):
        cdef c_bool in_band = buffer.read_bool()
        if not in_band:
            assert self._buffers is not None
            return next(self._buffers)
        cdef int32_t size = buffer.read_varint32()
        cdef Buffer buf = buffer.slice(buffer.reader_index, size)
        buffer.reader_index += size
        return buf

    cpdef inline handle_unsupported_write(self, Buffer buffer, obj):
        if self._unsupported_callback is None or self._unsupported_callback(obj):
            buffer.write_bool(True)
            self.pickler.dump(obj)
        else:
            buffer.write_bool(False)

    cpdef inline handle_unsupported_read(self, Buffer buffer):
        cdef c_bool in_band = buffer.read_bool()
        if in_band:
            return self.unpickler.load()
        else:
            assert self._unsupported_objects is not None
            return next(self._unsupported_objects)

    cpdef inline write_ref_pyobject(
            self, Buffer buffer, value, ClassInfo classinfo=None):
        if self.ref_resolver.write_ref_or_null(buffer, value):
            return
        if classinfo is None:
            classinfo = self.class_resolver.get_or_create_classinfo(type(value))
        self.class_resolver.write_classinfo(buffer, classinfo)
        classinfo.serializer.write(buffer, value)

    cpdef inline read_ref_pyobject(self, Buffer buffer):
        cdef MapRefResolver ref_resolver = self.ref_resolver
        cdef int32_t ref_id = ref_resolver.try_preserve_ref_id(buffer)
        if ref_id < NOT_NULL_VALUE_FLAG:
            return ref_resolver.get_read_object()
        # indicates that the object is first read.
        cdef ClassInfo classinfo = self.class_resolver.read_classinfo(buffer)
        o = classinfo.serializer.read(buffer)
        ref_resolver.set_read_object(ref_id, o)
        return o

    cpdef inline reset_write(self):
        self.ref_resolver.reset_write()
        self.class_resolver.reset_write()
        self.serialization_context.reset()
        self._native_objects.clear()
        self.pickler.clear_memo()
        self._unsupported_callback = None

    cpdef inline reset_read(self):
        self.ref_resolver.reset_read()
        self.class_resolver.reset_read()
        self.serialization_context.reset()
        self._native_objects.clear()
        self._buffers = None
        self.unpickler = None
        self._unsupported_objects = None

    cpdef inline reset(self):
        self.reset_write()
        self.reset_read()


cpdef inline write_nullable_pybool(Buffer buffer, value):
    if value is None:
        buffer.write_int8(NULL_FLAG)
    else:
        buffer.write_int8(NOT_NULL_VALUE_FLAG)
        buffer.write_bool(value)

cpdef inline write_nullable_pyint64(Buffer buffer, value):
    if value is None:
        buffer.write_int8(NULL_FLAG)
    else:
        buffer.write_int8(NOT_NULL_VALUE_FLAG)
        buffer.write_varint64(value)

cpdef inline write_nullable_pyfloat64(Buffer buffer, value):
    if value is None:
        buffer.write_int8(NULL_FLAG)
    else:
        buffer.write_int8(NOT_NULL_VALUE_FLAG)
        buffer.write_double(value)

cpdef inline write_nullable_pystr(Buffer buffer, value):
    if value is None:
        buffer.write_int8(NULL_FLAG)
    else:
        buffer.write_int8(NOT_NULL_VALUE_FLAG)
        buffer.write_string(value)


cpdef inline read_nullable_pybool(Buffer buffer):
    if buffer.read_int8() == NOT_NULL_VALUE_FLAG:
        return buffer.read_bool()
    else:
        return None

cpdef inline read_nullable_pyint64(Buffer buffer):
    if buffer.read_int8() == NOT_NULL_VALUE_FLAG:
        return buffer.read_varint64()
    else:
        return None

cpdef inline read_nullable_pyfloat64(Buffer buffer):
    if buffer.read_int8() == NOT_NULL_VALUE_FLAG:
        return buffer.read_double()
    else:
        return None

cpdef inline read_nullable_pystr(Buffer buffer):
    if buffer.read_int8() == NOT_NULL_VALUE_FLAG:
        return buffer.read_string()
    else:
        return None


@cython.final
cdef class SerializationContext:
    cdef dict objects

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


cdef class Serializer:
    cdef readonly Fury fury
    cdef readonly object type_
    cdef public c_bool need_to_write_ref

    def __init__(self, fury, type_: Union[type, TypeVar]):
        self.fury = fury
        self.type_ = type_
        self.need_to_write_ref = not is_primitive_type(type_)

    cpdef int16_t get_xtype_id(self):
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

    cpdef str get_xtype_tag(self):
        """
        Returns
        -------
            a type tag used for setup type mapping between languages.
        """

    cpdef write(self, Buffer buffer, value):
        raise NotImplementedError

    cpdef read(self, Buffer buffer):
        raise NotImplementedError

    cpdef xwrite(self, Buffer buffer, value):
        raise NotImplemented

    cpdef xread(self, Buffer buffer):
        raise NotImplemented

    @classmethod
    def support_subclass(cls) -> bool:
        return False


cdef class CrossLanguageCompatibleSerializer(Serializer):
    cpdef write(self, Buffer buffer, value):
        raise NotImplementedError

    cpdef read(self, Buffer buffer):
        raise NotImplementedError

    def __init__(self, fury, type_):
        super().__init__(fury, type_)

    cpdef xwrite(self, Buffer buffer, value):
        self.write(buffer, value)

    cpdef xread(self, Buffer buffer):
        return self.read(buffer)


@cython.final
cdef class BooleanSerializer(CrossLanguageCompatibleSerializer):
    cpdef inline int16_t get_xtype_id(self):
        return FuryType.BOOL.value

    cpdef inline write(self, Buffer buffer, value):
        buffer.write_bool(value)

    cpdef inline read(self, Buffer buffer):
        return buffer.read_bool()


@cython.final
cdef class NoneSerializer(Serializer):

    cpdef inline xwrite(self, Buffer buffer, value):
        raise NotImplementedError

    cpdef inline xread(self, Buffer buffer):
        raise NotImplementedError

    cpdef inline write(self, Buffer buffer, value):
        pass

    cpdef inline read(self, Buffer buffer):
        return None


@cython.final
cdef class ByteSerializer(CrossLanguageCompatibleSerializer):
    cpdef inline int16_t get_xtype_id(self):
        return FuryType.INT8.value

    cpdef inline write(self, Buffer buffer, value):
        buffer.write_int8(value)

    cpdef inline read(self, Buffer buffer):
        return buffer.read_int8()


@cython.final
cdef class Int16Serializer(CrossLanguageCompatibleSerializer):
    cpdef inline int16_t get_xtype_id(self):
        return FuryType.INT16.value

    cpdef inline write(self, Buffer buffer, value):
        buffer.write_int16(value)

    cpdef inline read(self, Buffer buffer):
        return buffer.read_int16()


@cython.final
cdef class Int32Serializer(CrossLanguageCompatibleSerializer):
    cpdef inline int16_t get_xtype_id(self):
        return FuryType.INT32.value

    cpdef inline write(self, Buffer buffer, value):
        buffer.write_int32(value)

    cpdef inline read(self, Buffer buffer):
        return buffer.read_int32()


@cython.final
cdef class Int64Serializer(CrossLanguageCompatibleSerializer):
    cpdef inline int16_t get_xtype_id(self):
        return FuryType.INT64.value

    cpdef inline xwrite(self, Buffer buffer, value):
        buffer.write_int64(value)

    cpdef inline xread(self, Buffer buffer):
        return buffer.read_int64()

    cpdef inline write(self, Buffer buffer, value):
        buffer.write_varint64(value)

    cpdef inline read(self, Buffer buffer):
        return buffer.read_varint64()


@cython.final
cdef class FloatSerializer(CrossLanguageCompatibleSerializer):
    cpdef inline int16_t get_xtype_id(self):
        return FuryType.FLOAT.value

    cpdef inline write(self, Buffer buffer, value):
        buffer.write_float(value)

    cpdef inline read(self, Buffer buffer):
        return buffer.read_float()


@cython.final
cdef class DoubleSerializer(CrossLanguageCompatibleSerializer):
    cpdef inline int16_t get_xtype_id(self):
        return FuryType.DOUBLE.value

    cpdef inline write(self, Buffer buffer, value):
        buffer.write_double(value)

    cpdef inline read(self, Buffer buffer):
        return buffer.read_double()


@cython.final
cdef class StringSerializer(CrossLanguageCompatibleSerializer):
    cpdef inline int16_t get_xtype_id(self):
        return FuryType.STRING.value

    cpdef inline write(self, Buffer buffer, value):
        buffer.write_string(value)

    cpdef inline read(self, Buffer buffer):
        return buffer.read_string()


cdef _base_date = datetime.date(1970, 1, 1)


@cython.final
cdef class DateSerializer(CrossLanguageCompatibleSerializer):
    cpdef inline int16_t get_xtype_id(self):
        return FuryType.DATE32.value

    cpdef inline write(self, Buffer buffer, value):
        if type(value) is not datetime.date:
            raise TypeError(
                "{} should be {} instead of {}".format(
                    value, datetime.date, type(value)
                )
            )
        days = (value - _base_date).days
        buffer.write_int32(days)

    cpdef inline read(self, Buffer buffer):
        days = buffer.read_int32()
        return _base_date + datetime.timedelta(days=days)


@cython.final
cdef class TimestampSerializer(CrossLanguageCompatibleSerializer):
    cpdef inline int16_t get_xtype_id(self):
        return FuryType.TIMESTAMP.value

    cpdef inline write(self, Buffer buffer, value):
        if type(value) is not datetime.datetime:
            raise TypeError(
                "{} should be {} instead of {}".format(value, datetime, type(value))
            )
        # TimestampType represent micro seconds
        timestamp = int(value.timestamp() * 1000000)
        buffer.write_int64(timestamp)

    cpdef inline read(self, Buffer buffer):
        ts = buffer.read_int64() / 1000000
        # TODO support timezone
        return datetime.datetime.fromtimestamp(ts)


@cython.final
cdef class BytesSerializer(CrossLanguageCompatibleSerializer):
    cpdef inline int16_t get_xtype_id(self):
        return FuryType.BINARY.value

    cpdef inline write(self, Buffer buffer, value):
        self.fury.write_buffer_object(buffer, BytesBufferObject(value))

    cpdef inline read(self, Buffer buffer):
        fury_buf = self.fury.read_buffer_object(buffer)
        return fury_buf.to_pybytes()


cdef class CollectionSerializer(Serializer):
    cdef ClassResolver class_resolver
    cdef MapRefResolver ref_resolver
    cdef Serializer elem_serializer

    def __init__(self, fury, type_, elem_serializer=None):
        super().__init__(fury, type_)
        self.class_resolver = fury.class_resolver
        self.ref_resolver = fury.ref_resolver
        self.elem_serializer = elem_serializer

    cpdef int16_t get_xtype_id(self):
        return -FuryType.LIST.value

    cpdef write(self, Buffer buffer, value):
        buffer.write_varint32(len(value))
        cdef MapRefResolver ref_resolver = self.ref_resolver
        cdef ClassResolver class_resolver = self.class_resolver
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
            elif cls is float:
                buffer.write_int24(NOT_NULL_PYFLOAT_FLAG)
                buffer.write_double(s)
            else:
                if not ref_resolver.write_ref_or_null(buffer, s):
                    classinfo = class_resolver.get_or_create_classinfo(cls)
                    class_resolver.write_classinfo(buffer, classinfo)
                    classinfo.serializer.write(buffer, s)

    cpdef xwrite(self, Buffer buffer, value):
        cdef int32_t len_ = 0
        try:
            len_ = len(value)
        except AttributeError:
            value = list(value)
            len_ = len(value)
        buffer.write_varint32(len_)
        for s in value:
            self.fury.xserialize_ref(
                buffer, s, serializer=self.elem_serializer
            )
            len_ += 1


cdef class ListSerializer(CollectionSerializer):
    cpdef int16_t get_xtype_id(self):
        return FuryType.LIST.value

    cpdef read(self, Buffer buffer):
        cdef MapRefResolver ref_resolver = self.fury.ref_resolver
        cdef ClassResolver class_resolver = self.fury.class_resolver
        cdef list list_ = []
        ref_resolver.reference(list_)
        populate_list(buffer, list_, ref_resolver, class_resolver)
        return list_

    cpdef xread(self, Buffer buffer):
        cdef int32_t len_ = buffer.read_varint32()
        cdef list collection_ = []
        self.fury.ref_resolver.reference(collection_)
        for i in range(len_):
            collection_.append(self.fury.xdeserialize_ref(
                buffer, serializer=self.elem_serializer
            ))
        return collection_


cdef populate_list(
        Buffer buffer,
        list list_,
        MapRefResolver ref_resolver,
        ClassResolver class_resolver):
    cdef int32_t ref_id
    cdef ClassInfo classinfo
    cdef int32_t len_ = buffer.read_varint32()
    for i in range(len_):
        ref_id = ref_resolver.try_preserve_ref_id(buffer)
        if ref_id < NOT_NULL_VALUE_FLAG:
            list_.append(ref_resolver.get_read_object())
            continue
        # indicates that the object is first read.
        classinfo = class_resolver.read_classinfo(buffer)
        cls = classinfo.cls
        # Note that all read operations in fast paths of list/tuple/set/dict/sub_dict
        # ust match corresponding writing operations. Otherwise, ref tracking will
        # error.
        if cls is str:
            list_.append(buffer.read_string())
        elif cls is int:
            list_.append(buffer.read_varint64())
        elif cls is bool:
            list_.append(buffer.read_bool())
        elif cls is float:
            list_.append(buffer.read_double())
        else:
            o = classinfo.serializer.read(buffer)
            ref_resolver.set_read_object(ref_id, o)
            list_.append(o)


@cython.final
cdef class TupleSerializer(CollectionSerializer):
    cpdef inline read(self, Buffer buffer):
        cdef MapRefResolver ref_resolver = self.fury.ref_resolver
        cdef ClassResolver class_resolver = self.fury.class_resolver
        cdef list list_ = []
        populate_list(buffer, list_, ref_resolver, class_resolver)
        return tuple(list_)

    cpdef inline xread(self, Buffer buffer):
        cdef int32_t len_ = buffer.read_varint32()
        cdef list collection_ = []
        for i in range(len_):
            collection_.append(self.fury.xdeserialize_ref(
                buffer, serializer=self.elem_serializer
            ))
        return tuple(collection_)


@cython.final
cdef class StringArraySerializer(ListSerializer):
    def __init__(self, fury, type_):
        super().__init__(fury, type_, StringSerializer(fury, str))

    cpdef inline int16_t get_xtype_id(self):
        return FuryType.FURY_STRING_ARRAY.value


@cython.final
cdef class SetSerializer(CollectionSerializer):
    cpdef inline int16_t get_xtype_id(self):
        return FuryType.FURY_SET.value

    cpdef inline read(self, Buffer buffer):
        cdef MapRefResolver ref_resolver = self.fury.ref_resolver
        cdef ClassResolver class_resolver = self.fury.class_resolver
        cdef set instance = set()
        ref_resolver.reference(instance)
        cdef int32_t len_ = buffer.read_varint32()
        cdef int32_t ref_id
        cdef ClassInfo classinfo
        for i in range(len_):
            ref_id = ref_resolver.try_preserve_ref_id(buffer)
            if ref_id < NOT_NULL_VALUE_FLAG:
                instance.add(ref_resolver.get_read_object())
                continue
            # indicates that the object is first read.
            classinfo = class_resolver.read_classinfo(buffer)
            cls = classinfo.cls
            if cls is str:
                instance.add(buffer.read_string())
            elif cls is int:
                instance.add(buffer.read_varint64())
            elif cls is bool:
                instance.add(buffer.read_bool())
            elif cls is float:
                instance.add(buffer.read_double())
            else:
                o = classinfo.serializer.read(buffer)
                ref_resolver.set_read_object(ref_id, o)
                instance.add(o)
        return instance

    cpdef inline xread(self, Buffer buffer):
        cdef int32_t len_ = buffer.read_varint32()
        cdef set instance = set()
        self.fury.ref_resolver.reference(instance)
        for i in range(len_):
            instance.add(self.fury.xdeserialize_ref(
                buffer, serializer=self.elem_serializer
            ))
        return instance


@cython.final
cdef class MapSerializer(Serializer):
    cdef ClassResolver class_resolver
    cdef MapRefResolver ref_resolver
    cdef Serializer key_serializer
    cdef Serializer value_serializer

    def __init__(self, fury, type_, key_serializer=None, value_serializer=None):
        super().__init__(fury, type_)
        self.class_resolver = fury.class_resolver
        self.ref_resolver = fury.ref_resolver
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer

    cpdef inline int16_t get_xtype_id(self):
        return FuryType.MAP.value

    cpdef inline write(self, Buffer buffer, o):
        cdef dict value = o
        buffer.write_varint32(len(value))
        cdef ClassInfo key_classinfo
        cdef ClassInfo value_classinfo
        for k, v in value.items():
            key_cls = type(k)
            if key_cls is str:
                buffer.write_int24(NOT_NULL_STRING_FLAG)
                buffer.write_string(k)
            else:
                if not self.ref_resolver.write_ref_or_null(buffer, k):
                    key_classinfo = self.class_resolver.get_or_create_classinfo(key_cls)
                    self.class_resolver.write_classinfo(buffer, key_classinfo)
                    key_classinfo.serializer.write(buffer, k)
            value_cls = type(v)
            if value_cls is str:
                buffer.write_int24(NOT_NULL_STRING_FLAG)
                buffer.write_string(v)
            elif value_cls is int:
                buffer.write_int24(NOT_NULL_PYINT_FLAG)
                buffer.write_varint64(v)
            elif value_cls is bool:
                buffer.write_int24(NOT_NULL_PYBOOL_FLAG)
                buffer.write_bool(v)
            elif value_cls is float:
                buffer.write_int24(NOT_NULL_PYFLOAT_FLAG)
                buffer.write_double(v)
            else:
                if not self.ref_resolver.write_ref_or_null(buffer, v):
                    value_classinfo = self.class_resolver. \
                        get_or_create_classinfo(value_cls)
                    self.class_resolver.write_classinfo(buffer, value_classinfo)
                    value_classinfo.serializer.write(buffer, v)

    cpdef inline read(self, Buffer buffer):
        cdef MapRefResolver ref_resolver = self.ref_resolver
        cdef ClassResolver class_resolver = self.class_resolver
        cdef dict map_ = {}
        ref_resolver.reference(map_)
        cdef int32_t len_ = buffer.read_varint32()
        cdef int32_t ref_id
        cdef ClassInfo key_classinfo
        cdef ClassInfo value_classinfo
        for i in range(len_):
            ref_id = ref_resolver.try_preserve_ref_id(buffer)
            if ref_id < NOT_NULL_VALUE_FLAG:
                key = ref_resolver.get_read_object()
            else:
                key_classinfo = class_resolver.read_classinfo(buffer)
                if key_classinfo.cls is str:
                    key = buffer.read_string()
                else:
                    key = key_classinfo.serializer.read(buffer)
                    ref_resolver.set_read_object(ref_id, key)
            ref_id = ref_resolver.try_preserve_ref_id(buffer)
            if ref_id < NOT_NULL_VALUE_FLAG:
                value = ref_resolver.get_read_object()
            else:
                value_classinfo = class_resolver.read_classinfo(buffer)
                cls = value_classinfo.cls
                if cls is str:
                    value = buffer.read_string()
                elif cls is int:
                    value = buffer.read_varint64()
                elif cls is bool:
                    value = buffer.read_bool()
                elif cls is float:
                    value = buffer.read_double()
                else:
                    value = value_classinfo.serializer.read(buffer)
                    ref_resolver.set_read_object(ref_id, value)
            map_[key] = value
        return map_

    cpdef inline xwrite(self, Buffer buffer, o):
        cdef dict value = o
        buffer.write_varint32(len(value))
        for k, v in value.items():
            self.fury.xserialize_ref(
                buffer, k, serializer=self.key_serializer
            )
            self.fury.xserialize_ref(
                buffer, v, serializer=self.value_serializer
            )

    cpdef inline xread(self, Buffer buffer):
        cdef int32_t len_ = buffer.read_varint32()
        cdef dict map_ = {}
        self.fury.ref_resolver.reference(map_)
        for i in range(len_):
            k = self.fury.xdeserialize_ref(
                buffer, serializer=self.key_serializer
            )
            v = self.fury.xdeserialize_ref(
                buffer, serializer=self.value_serializer
            )
            map_[k] = v
        return map_


@cython.final
cdef class SubMapSerializer(Serializer):
    cdef ClassResolver class_resolver
    cdef MapRefResolver ref_resolver
    cdef Serializer key_serializer
    cdef Serializer value_serializer

    def __init__(self, fury, type_, key_serializer=None, value_serializer=None):
        super().__init__(fury, type_)
        self.class_resolver = fury.class_resolver
        self.ref_resolver = fury.ref_resolver
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer

    cpdef inline write(self, Buffer buffer, value):
        buffer.write_varint32(len(value))
        cdef ClassInfo key_classinfo
        cdef ClassInfo value_classinfo
        for k, v in value.items():
            key_cls = type(k)
            if key_cls is str:
                buffer.write_int24(NOT_NULL_STRING_FLAG)
                buffer.write_string(k)
            else:
                if not self.ref_resolver.write_ref_or_null(buffer, k):
                    key_classinfo = self.class_resolver.get_or_create_classinfo(key_cls)
                    self.class_resolver.write_classinfo(buffer, key_classinfo)
                    key_classinfo.serializer.write(buffer, k)
            value_cls = type(v)
            if value_cls is str:
                buffer.write_int24(NOT_NULL_STRING_FLAG)
                buffer.write_string(v)
            elif value_cls is int:
                buffer.write_int24(NOT_NULL_PYINT_FLAG)
                buffer.write_varint64(v)
            elif value_cls is bool:
                buffer.write_int24(NOT_NULL_PYBOOL_FLAG)
                buffer.write_bool(v)
            elif value_cls is float:
                buffer.write_int24(NOT_NULL_PYFLOAT_FLAG)
                buffer.write_double(v)
            else:
                if not self.ref_resolver.write_ref_or_null(buffer, v):
                    value_classinfo = self.class_resolver. \
                        get_or_create_classinfo(value_cls)
                    self.class_resolver.write_classinfo(buffer, value_classinfo)
                    value_classinfo.serializer.write(buffer, v)

    cpdef inline read(self, Buffer buffer):
        cdef MapRefResolver ref_resolver = self.fury.ref_resolver
        cdef ClassResolver class_resolver = self.fury.class_resolver
        map_ = self.type_()
        ref_resolver.reference(map_)
        cdef int32_t len_ = buffer.read_varint32()
        cdef int32_t ref_id
        cdef ClassInfo key_classinfo
        cdef ClassInfo value_classinfo
        for i in range(len_):
            ref_id = ref_resolver.try_preserve_ref_id(buffer)
            if ref_id < NOT_NULL_VALUE_FLAG:
                key = ref_resolver.get_read_object()
            else:
                key_classinfo = class_resolver.read_classinfo(buffer)
                if key_classinfo.cls is str:
                    key = buffer.read_string()
                else:
                    key = key_classinfo.serializer.read(buffer)
                    ref_resolver.set_read_object(ref_id, key)
            ref_id = ref_resolver.try_preserve_ref_id(buffer)
            if ref_id < NOT_NULL_VALUE_FLAG:
                value = ref_resolver.get_read_object()
            else:
                value_classinfo = class_resolver.read_classinfo(buffer)
                cls = value_classinfo.cls
                if cls is str:
                    value = buffer.read_string()
                elif cls is int:
                    value = buffer.read_varint64()
                elif cls is bool:
                    value = buffer.read_bool()
                elif cls is float:
                    value = buffer.read_double()
                else:
                    value = value_classinfo.serializer.read(buffer)
                    ref_resolver.set_read_object(ref_id, value)
            map_[key] = value
        return map_


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


@cython.final
cdef class PyArraySerializer(CrossLanguageCompatibleSerializer):
    typecode_dict = typecode_dict
    typecodearray_type = {
        "h": Int16ArrayType,
        "i": Int32ArrayType,
        "l": Int64ArrayType,
        "f": Float32ArrayType,
        "d": Float64ArrayType,
    }
    cdef str typecode
    cdef int8_t itemsize
    cdef int16_t type_id

    def __init__(self, fury, type_, str typecode):
        super().__init__(fury, type_)
        self.typecode = typecode
        self.itemsize, self.type_id = PyArraySerializer.typecode_dict[self.typecode]

    cpdef int16_t get_xtype_id(self):
        return self.type_id

    cpdef inline xwrite(self, Buffer buffer, value):
        assert value.itemsize == self.itemsize
        view = memoryview(value)
        assert view.format == self.typecode
        assert view.itemsize == self.itemsize
        assert view.c_contiguous  # TODO handle contiguous
        cdef int32_t nbytes = len(value) * self.itemsize
        buffer.write_varint32(nbytes)
        buffer.write_buffer(value)

    cpdef inline xread(self, Buffer buffer):
        data = buffer.read_bytes_and_size()
        arr = array.array(self.typecode, [])
        arr.frombytes(data)
        return arr

    cpdef inline write(self, Buffer buffer, value: array.array):
        cdef int32_t nbytes = len(value) * value.itemsize
        buffer.write_string(value.typecode)
        buffer.write_varint32(nbytes)
        buffer.write_buffer(value)

    cpdef inline read(self, Buffer buffer):
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


@cython.final
cdef class Numpy1DArraySerializer(CrossLanguageCompatibleSerializer):
    dtypes_dict = _np_dtypes_dict
    cdef object dtype
    cdef str typecode
    cdef int8_t itemsize
    cdef int16_t type_id

    def __init__(self, fury, type_, dtype):
        super().__init__(fury, type_)
        self.dtype = dtype
        self.itemsize, self.typecode, self.type_id = _np_dtypes_dict[self.dtype]

    cpdef int16_t get_xtype_id(self):
        return self.type_id

    cpdef inline xwrite(self, Buffer buffer, value):
        assert value.itemsize == self.itemsize
        view = memoryview(value)
        try:
            assert view.format == self.typecode
        except AssertionError as e:
            raise e
        assert view.itemsize == self.itemsize
        cdef int32_t nbytes = len(value) * self.itemsize
        buffer.write_varint32(nbytes)
        if self.dtype == np.dtype("bool") or not view.c_contiguous:
            buffer.write_bytes(value.tobytes())
        else:
            buffer.write_buffer(value)

    cpdef inline xread(self, Buffer buffer):
        data = buffer.read_bytes_and_size()
        return np.frombuffer(data, dtype=self.dtype)

    cpdef inline write(self, Buffer buffer, value):
        self.fury.handle_unsupported_write(buffer, value)

    cpdef inline read(self, Buffer buffer):
        return self.fury.handle_unsupported_read(buffer)


cdef _get_hash(Fury fury, list field_names, dict type_hints):
    from pyfury._struct import StructHashVisitor

    visitor = StructHashVisitor(fury)
    for index, key in enumerate(field_names):
        infer_field(key, type_hints[key], visitor, types_path=[])
    hash_ = visitor.get_hash()
    assert hash_ != 0
    return hash_


cdef class ComplexObjectSerializer(Serializer):
    cdef str _type_tag
    cdef object _type_hints
    cdef list _field_names
    cdef list _serializers
    cdef int32_t _hash

    def __init__(self, fury, clz: type, type_tag: str):
        super().__init__(fury, clz)
        self._type_tag = type_tag
        self._type_hints = get_type_hints(clz)
        self._field_names = sorted(self._type_hints.keys())
        self._serializers = [None] * len(self._field_names)
        from pyfury._struct import ComplexTypeVisitor

        visitor = ComplexTypeVisitor(fury)
        for index, key in enumerate(self._field_names):
            serializer = infer_field(key, self._type_hints[key], visitor, types_path=[])
            self._serializers[index] = serializer
        if self.fury.language == Language.PYTHON:
            logger.warning(
                "Type of class %s shouldn't be serialized using cross-language "
                "serializer",
                clz,
            )
        self._hash = 0

    cpdef int16_t get_xtype_id(self):
        return FuryType.FURY_TYPE_TAG.value

    cpdef str get_xtype_tag(self):
        return self._type_tag

    cpdef write(self, Buffer buffer, value):
        return self.xwrite(buffer, value)

    cpdef read(self, Buffer buffer):
        return self.xread(buffer)

    cpdef xwrite(self, Buffer buffer, value):
        if self._hash == 0:
            self._hash = _get_hash(self.fury, self._field_names, self._type_hints)
        buffer.write_int32(self._hash)
        cdef Serializer serializer
        cdef int32_t index
        for index, field_name in enumerate(self._field_names):
            field_value = getattr(value, field_name)
            serializer = self._serializers[index]
            self.fury.xserialize_ref(
                buffer, field_value, serializer=serializer
            )

    cpdef xread(self, Buffer buffer):
        cdef int32_t hash_ = buffer.read_int32()
        if self._hash == 0:
            self._hash = _get_hash(self.fury, self._field_names, self._type_hints)
        if hash_ != self._hash:
            raise ClassNotCompatibleError(
                f"Hash {hash_} is not consistent with {self._hash} "
                f"for class {self.type_}",
            )
        obj = self.type_.__new__(self.type_)
        self.fury.ref_resolver.reference(obj)
        cdef Serializer serializer
        cdef int32_t index
        for index, field_name in enumerate(self._field_names):
            serializer = self._serializers[index]
            field_value = self.fury.xdeserialize_ref(
                buffer, serializer=serializer
            )
            setattr(
                obj,
                field_name,
                field_value,
            )
        return obj


@cython.final
cdef class EnumSerializer(Serializer):
    @classmethod
    def support_subclass(cls) -> bool:
        return True

    cpdef inline write(self, Buffer buffer, value):
        buffer.write_string(value.name)

    cpdef inline read(self, Buffer buffer):
        name = buffer.read_string()
        return getattr(self.type_, name)

    cpdef inline xwrite(self, Buffer buffer, value):
        raise NotImplementedError

    cpdef inline xread(self, Buffer buffer):
        raise NotImplementedError


@cython.final
cdef class SliceSerializer(Serializer):
    cpdef inline write(self, Buffer buffer, v):
        cdef slice value = v
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

    cpdef inline read(self, Buffer buffer):
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

    cpdef xwrite(self, Buffer buffer, value):
        raise NotImplementedError

    cpdef xread(self, Buffer buffer):
        raise NotImplementedError


@cython.final
cdef class PickleSerializer(Serializer):
    cpdef inline xwrite(self, Buffer buffer, value):
        raise NotImplementedError

    cpdef inline xread(self, Buffer buffer):
        raise NotImplementedError

    cpdef inline write(self, Buffer buffer, value):
        self.fury.handle_unsupported_write(buffer, value)

    cpdef inline read(self, Buffer buffer):
        return self.fury.handle_unsupported_read(buffer)
