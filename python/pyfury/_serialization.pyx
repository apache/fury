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
from typing import TypeVar, Union, Iterable, get_type_hints

from pyfury._util import get_bit, set_bit, clear_bit
from pyfury._fury import Language, OpaqueObject
from pyfury.error import ClassNotCompatibleError
from pyfury.lib import mmh3
from pyfury.type import is_primitive_type, FuryType, Int8Type, Int16Type, Int32Type,\
    Int64Type, Float32Type, Float64Type, Int16ArrayType, Int32ArrayType,\
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
cdef class MapReferenceResolver:
    cdef flat_hash_map[uint64_t, int32_t] written_objects_id  # id(obj) -> ref_id
    # Hold object to avoid tmp object gc when serialize nested fields/objects.
    cdef vector[PyObject*] written_objects
    cdef vector[PyObject*] read_objects
    cdef vector[int32_t] read_reference_ids
    cdef object read_object
    cdef c_bool ref_tracking

    def __cinit__(self, c_bool ref_tracking):
        self.read_object = None
        self.ref_tracking = ref_tracking

    # Special methods of extension types must be declared with def, not cdef.
    def __dealloc__(self):
        self.reset()

    cpdef inline c_bool write_reference_or_null(self, Buffer buffer, obj):
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

    cpdef inline int8_t read_reference_or_null(self, Buffer buffer):
        cdef int8_t head_flag = buffer.read_int8()
        if not self.ref_tracking:
            return head_flag
        cdef int32_t reference_id
        if head_flag == REF_FLAG:
            # read reference id and get object from reference resolver
            reference_id = buffer.read_varint32()
            self.read_object = <object>(self.read_objects[reference_id])
            return REF_FLAG
        else:
            self.read_object = None
            return head_flag

    cpdef inline int32_t preserve_reference_id(self):
        if not self.ref_tracking:
            return -1
        next_read_ref_id = self.read_objects.size()
        self.read_objects.push_back(NULL)
        self.read_reference_ids.push_back(next_read_ref_id)
        return next_read_ref_id

    cpdef inline int32_t try_preserve_reference_id(self, Buffer buffer):
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
                return self.preserve_reference_id()
            return head_flag

    cpdef inline reference(self, obj):
        if not self.ref_tracking:
            return
        cdef int32_t ref_id = self.read_reference_ids.back()
        self.read_reference_ids.pop_back()
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
        self.read_reference_ids.clear()
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
cdef int32_t NOT_NULL_PYINT_FLAG = NOT_NULL_VALUE_FLAG & 0b11111111 |\
                                   (PYINT_CLASS_ID << 8)
cdef int32_t NOT_NULL_PYFLOAT_FLAG = NOT_NULL_VALUE_FLAG & 0b11111111 |\
                                     (PYFLOAT_CLASS_ID << 8)
cdef int32_t NOT_NULL_PYBOOL_FLAG = NOT_NULL_VALUE_FLAG & 0b11111111 |\
                                    (PYBOOL_CLASS_ID << 8)
cdef int32_t NOT_NULL_STRING_FLAG = NOT_NULL_VALUE_FLAG & 0b11111111 |\
                                    (STRING_CLASS_ID << 8)

@cython.final
cdef class ClassResolver:
    cdef:
        readonly Fury fury_
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

    def __init__(self, fury_):
        self.fury_ = fury_
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
        type_id = serializer.get_cross_language_type_id()
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
                class_name_bytes = (cls.__module__ + "#" + cls.__qualname__)\
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
            cls, ComplexObjectSerializer(self.fury_, cls, type_tag)
        )

    def _add_serializer(
            self,
            cls: Union[type, TypeVar],
            serializer=None,
            serializer_cls=None):
        if serializer_cls:
            serializer = serializer_cls(self.fury_, cls)
        self.register_serializer(cls, serializer)

    def _add_x_lang_serializer(self,
                               cls: Union[type, TypeVar],
                               serializer=None,
                               serializer_cls=None):
        if serializer_cls:
            serializer = serializer_cls(self.fury_, cls)
        type_id = serializer.get_cross_language_type_id()

        assert type_id != NOT_SUPPORT_CROSS_LANGUAGE
        self._type_id_and_cls_to_serializer[(type_id, cls)] = serializer
        self.register_class(cls)
        classinfo = self._classes_info[cls]
        classinfo.serializer = serializer
        if type_id == FuryType.FURY_TYPE_TAG.value:
            type_tag = serializer.get_cross_language_type_tag()
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
                             serializer=PickleStrongCacheSerializer(self.fury_))
        self._add_serializer(PickleCacheStub,
                             serializer=PickleCacheSerializer(self.fury_))

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
                serializer=PyArraySerializer(self.fury_, array.array, typecode),
            )
            self._add_serializer(
                PyArraySerializer.typecode_to_pyarray_type[typecode],
                serializer=PyArraySerializer(self.fury_, array.array, typecode),
            )
        if np:
            for dtype in Numpy1DArraySerializer.dtypes_dict.keys():
                self._add_serializer(
                    np.ndarray,
                    serializer=Numpy1DArraySerializer(self.fury_, array.array, dtype),
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
                serializer = type(class_info.serializer)(self.fury_, cls)
                break
        else:
            if dataclasses.is_dataclass(cls):
                if classinfo_ is None or classinfo_.class_id == NO_CLASS_ID:
                    logger.info("Class %s not registered", cls)
                from pyfury import DataClassSerializer

                serializer = DataClassSerializer(self.fury_, cls)
            else:
                serializer = PickleSerializer(self.fury_, cls)
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

    cpdef inline cross_language_write_class(self, Buffer buffer, cls):
        cdef PyObject* classinfo_ptr = self._c_classes_info[<uintptr_t><PyObject*>cls]
        assert classinfo_ptr != NULL
        cdef EnumStringBytes class_name_bytes = (<object>classinfo_ptr).class_name_bytes
        self._write_enum_string_bytes(buffer, class_name_bytes)

    cpdef inline cross_language_write_type_tag(self, Buffer buffer, cls):
        cdef PyObject* classinfo_ptr = self._c_classes_info[<uintptr_t><PyObject*>cls]
        assert classinfo_ptr != NULL
        cdef EnumStringBytes type_tag_bytes = (<object>classinfo_ptr).type_tag_bytes
        self._write_enum_string_bytes(buffer, type_tag_bytes)

    cpdef inline read_class_by_type_tag(self, Buffer buffer):
        tag = self.cross_language_read_classname(buffer)
        return self._type_tag_to_class_x_lang_map[tag]

    cpdef inline cross_language_read_class(self, Buffer buffer):
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

    cpdef inline str cross_language_read_classname(self, Buffer buffer):
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
                (<EnumStringBytes>ptr).dynamic_write_string_id =\
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
