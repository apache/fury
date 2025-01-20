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

# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3
# cython: annotate = True
import datetime
import logging
import os
import warnings
from typing import TypeVar, Union, Iterable

from pyfury._util import get_bit, set_bit, clear_bit
from pyfury._fury import Language
from pyfury._fury import _PicklerStub, _UnpicklerStub, Pickler, Unpickler
from pyfury._fury import _ENABLE_CLASS_REGISTRATION_FORCIBLY
from pyfury.lib import mmh3
from pyfury.meta.metastring import Encoding
from pyfury.type import is_primitive_type
from pyfury.util import is_little_endian
from pyfury.includes.libserialization cimport TypeId, IsNamespacedType

from libc.stdint cimport int8_t, int16_t, int32_t, int64_t, uint64_t
from libc.stdint cimport *
from libcpp.vector cimport vector
from cpython cimport PyObject
from cpython.dict cimport PyDict_Next
from cpython.ref cimport *
from cpython.list cimport PyList_New, PyList_SET_ITEM
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from libcpp cimport bool as c_bool
from libcpp.utility cimport pair
from cython.operator cimport dereference as deref
from pyfury._util cimport Buffer
from pyfury.includes.libabsl cimport flat_hash_map

try:
    import numpy as np
except ImportError:
    np = None

cimport cython

logger = logging.getLogger(__name__)
ENABLE_FURY_CYTHON_SERIALIZATION = os.environ.get(
    "ENABLE_FURY_CYTHON_SERIALIZATION", "True").lower() in ("true", "1")

cdef extern from *:
    """
    #define int2obj(obj_addr) ((PyObject *)(obj_addr))
    #define obj2int(obj_ref) (Py_INCREF(obj_ref), ((int64_t)(obj_ref)))
    """
    object int2obj(int64_t obj_addr)
    int64_t obj2int(object obj_ref)
    dict _PyDict_NewPresized(Py_ssize_t minused)


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
    cdef vector[PyObject *] written_objects
    cdef vector[PyObject *] read_objects
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
        cdef uint64_t object_id = <uintptr_t> <PyObject *> obj
        cdef int32_t next_id
        cdef flat_hash_map[uint64_t, int32_t].iterator it = \
            self.written_objects_id.find(object_id)
        if it == self.written_objects_id.end():
            next_id = self.written_objects_id.size()
            self.written_objects_id[object_id] = next_id
            self.written_objects.push_back(<PyObject *> obj)
            Py_INCREF(obj)
            buffer.write_int8(REF_VALUE_FLAG)
            return False
        else:
            # The obj has been written previously.
            buffer.write_int8(REF_FLAG)
            buffer.write_varuint32(<uint64_t> deref(it).second)
            return True

    cpdef inline int8_t read_ref_or_null(self, Buffer buffer):
        cdef int8_t head_flag = buffer.read_int8()
        if not self.ref_tracking:
            return head_flag
        cdef int32_t ref_id
        if head_flag == REF_FLAG:
            # read reference id and get object from reference resolver
            ref_id = buffer.read_varuint32()
            self.read_object = <object> (self.read_objects[ref_id])
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
            ref_id = buffer.read_varuint32()
            self.read_object = <object> (self.read_objects[ref_id])
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
        self.read_objects[ref_id] = <PyObject *> obj

    cpdef inline get_read_object(self, id_=None):
        if not self.ref_tracking:
            return None
        if id_ is None:
            return self.read_object
        cdef int32_t ref_id = id_
        return <object> (self.read_objects[ref_id])

    cpdef inline set_read_object(self, int32_t ref_id, obj):
        if not self.ref_tracking:
            return
        if ref_id >= 0:
            need_inc = self.read_objects[ref_id] == NULL
            if need_inc:
                Py_INCREF(obj)
            self.read_objects[ref_id] = <PyObject *> obj

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
cdef int16_t MAGIC_NUMBER = 0x62D4
# `NOT_NULL_VALUE_FLAG` + `CLASS_ID<<1` in little-endian order
cdef int32_t NOT_NULL_PYINT_FLAG = NOT_NULL_VALUE_FLAG & 0b11111111 | \
                                   (PYINT_CLASS_ID << 9)
cdef int32_t NOT_NULL_PYFLOAT_FLAG = NOT_NULL_VALUE_FLAG & 0b11111111 | \
                                     (PYFLOAT_CLASS_ID << 9)
cdef int32_t NOT_NULL_PYBOOL_FLAG = NOT_NULL_VALUE_FLAG & 0b11111111 | \
                                    (PYBOOL_CLASS_ID << 9)
cdef int32_t NOT_NULL_STRING_FLAG = NOT_NULL_VALUE_FLAG & 0b11111111 | \
                                    (STRING_CLASS_ID << 9)
cdef int32_t SMALL_STRING_THRESHOLD = 16


@cython.final
cdef class MetaStringBytes:
    cdef public bytes data
    cdef int16_t length
    cdef public int8_t encoding
    cdef public int64_t hashcode
    cdef public int16_t dynamic_write_string_id

    def __init__(self, data, hashcode):
        self.data = data
        self.length = len(data)
        self.hashcode = hashcode
        self.encoding = hashcode & 0xff
        self.dynamic_write_string_id = DEFAULT_DYNAMIC_WRITE_STRING_ID

    def __eq__(self, other):
        return type(other) is MetaStringBytes and other.hashcode == self.hashcode

    def __hash__(self):
        return self.hashcode

    def decode(self, decoder):
        return decoder.decode(self.data, Encoding(self.encoding))

    def __repr__(self):
        return f"MetaStringBytes(data={self.data}, hashcode={self.hashcode})"


@cython.final
cdef class MetaStringResolver:
    cdef:
        int16_t dynamic_write_string_id
        vector[PyObject *] _c_dynamic_written_enum_string
        vector[PyObject *] _c_dynamic_id_to_enum_string_vec
        # hash -> MetaStringBytes
        flat_hash_map[int64_t, PyObject *] _c_hash_to_metastr_bytes
        flat_hash_map[pair[int64_t, int64_t], PyObject *] _c_hash_to_small_metastring_bytes
        set _enum_str_set
        dict _metastr_to_metastr_bytes

    def __init__(self):
        self._enum_str_set = set()
        self._metastr_to_metastr_bytes = dict()

    cpdef inline write_meta_string_bytes(
            self, Buffer buffer, MetaStringBytes metastr_bytes):
        cdef int16_t dynamic_type_id = metastr_bytes.dynamic_write_string_id
        cdef int32_t length = metastr_bytes.length
        if dynamic_type_id == DEFAULT_DYNAMIC_WRITE_STRING_ID:
            dynamic_type_id = self.dynamic_write_string_id
            metastr_bytes.dynamic_write_string_id = dynamic_type_id
            self.dynamic_write_string_id += 1
            self._c_dynamic_written_enum_string.push_back(<PyObject *> metastr_bytes)
            buffer.write_varuint32(length << 1)
            if length <= SMALL_STRING_THRESHOLD:
                buffer.write_int8(metastr_bytes.encoding)
            else:
                buffer.write_int64(metastr_bytes.hashcode)
            buffer.write_bytes(metastr_bytes.data)
        else:
            buffer.write_varuint32(((dynamic_type_id + 1) << 1) | 1)

    cpdef inline MetaStringBytes read_meta_string_bytes(self, Buffer buffer):
        cdef int32_t header = buffer.read_varuint32()
        cdef int32_t length = header >> 1
        if header & 0b1 != 0:
            return <MetaStringBytes> self._c_dynamic_id_to_enum_string_vec[length - 1]
        cdef int64_t v1 = 0, v2 = 0, hashcode
        cdef PyObject * enum_str_ptr
        cdef int32_t reader_index
        cdef encoding = 0
        if length <= SMALL_STRING_THRESHOLD:
            encoding = buffer.read_int8()
            if length <= 8:
                v1 = buffer.read_bytes_as_int64(length)
            else:
                v1 = buffer.read_int64()
                v2 = buffer.read_bytes_as_int64(length - 8)
            hashcode = ((v1 * 31 + v2) >> 8 << 8) | encoding
            enum_str_ptr = self._c_hash_to_small_metastring_bytes[pair[int64_t, int64_t](v1, v2)]
            if enum_str_ptr == NULL:
                reader_index = buffer.reader_index
                str_bytes = buffer.get_bytes(reader_index - length, length)
                enum_str = MetaStringBytes(str_bytes, hashcode=hashcode)
                self._enum_str_set.add(enum_str)
                enum_str_ptr = <PyObject *> enum_str
                self._c_hash_to_small_metastring_bytes[pair[int64_t, int64_t](v1, v2)] = enum_str_ptr
        else:
            hashcode = buffer.read_int64()
            reader_index = buffer.reader_index
            buffer.check_bound(reader_index, length)
            buffer.reader_index = reader_index + length
            enum_str_ptr = self._c_hash_to_metastr_bytes[hashcode]
            if enum_str_ptr == NULL:
                str_bytes = buffer.get_bytes(reader_index, length)
                enum_str = MetaStringBytes(str_bytes, hashcode=hashcode)
                self._enum_str_set.add(enum_str)
                enum_str_ptr = <PyObject *> enum_str
                self._c_hash_to_metastr_bytes[hashcode] = enum_str_ptr
        self._c_dynamic_id_to_enum_string_vec.push_back(enum_str_ptr)
        return <MetaStringBytes> enum_str_ptr

    def get_metastr_bytes(self, metastr):
        metastr_bytes = self._metastr_to_metastr_bytes.get(metastr)
        if metastr_bytes is not None:
            return metastr_bytes
        cdef int64_t v1 = 0, v2 = 0, hashcode
        length = len(metastr.encoded_data)
        if length <= SMALL_STRING_THRESHOLD:
            data_buf = Buffer(metastr.encoded_data)
            if length <= 8:
                v1 = data_buf.read_bytes_as_int64(length)
            else:
                v1 = data_buf.read_int64()
                v2 = data_buf.read_bytes_as_int64(length - 8)
            value_hash = ((v1 * 31 + v2) >> 8 << 8) | metastr.encoding.value
        else:
            value_hash = mmh3.hash_buffer(metastr.encoded_data, seed=47)[0]
            value_hash = value_hash >> 8 << 8
            value_hash |= metastr.encoding.value & 0xFF
        self._metastr_to_metastr_bytes[metastr] = metastr_bytes = MetaStringBytes(metastr.encoded_data, value_hash)
        return metastr_bytes

    cpdef inline reset_read(self):
        self._c_dynamic_id_to_enum_string_vec.clear()

    cpdef inline reset_write(self):
        if self.dynamic_write_string_id != 0:
            self.dynamic_write_string_id = 0
            for ptr in self._c_dynamic_written_enum_string:
                (<MetaStringBytes> ptr).dynamic_write_string_id = \
                    DEFAULT_DYNAMIC_WRITE_STRING_ID
            self._c_dynamic_written_enum_string.clear()


@cython.final
cdef class ClassInfo:
    """
    If dynamic_type is true, the serializer will be a dynamic typed serializer
    and it will write type info when writing the data.
    In such cases, the `write_classinfo` should not write typeinfo.
    In general, if we have 4 type for one class, we will have 5 serializers.
    For example, we have int8/16/32/64/128 for python `int` type, then we have 6 serializers
    for python `int`: `Int8/1632/64/128Serializer` for `int8/16/32/64/128` each, and another
    `IntSerializer` for `int` which will dispatch to different `int8/16/32/64/128` type
    according the actual value.
    We do not get the acutal type here, because it will introduce extra computing.
    For example, we have want to get actual `Int8/16/32/64Serializer`, we must check and
    extract the actutal here which will introduce cost, and we will do same thing again
    when serializing the actual data.
    """
    cdef public object cls
    cdef public int16_t type_id
    cdef public Serializer serializer
    cdef public MetaStringBytes namespace_bytes
    cdef public MetaStringBytes typename_bytes
    cdef public c_bool dynamic_type

    def __init__(
            self,
            cls: Union[type, TypeVar] = None,
            type_id: int = NO_CLASS_ID,
            serializer: Serializer = None,
            namespace_bytes: MetaStringBytes = None,
            typename_bytes: MetaStringBytes = None,
            dynamic_type: bool = False,
    ):
        self.cls = cls
        self.type_id = type_id
        self.serializer = serializer
        self.namespace_bytes = namespace_bytes
        self.typename_bytes = typename_bytes
        self.dynamic_type = dynamic_type

    def __repr__(self):
        return f"ClassInfo(cls={self.cls}, type_id={self.type_id}, " \
               f"serializer={self.serializer})"


@cython.final
cdef class ClassResolver:
    cdef:
        readonly Fury fury
        readonly MetaStringResolver metastring_resolver
        object _resolver
        vector[PyObject *] _c_registered_id_to_class_info
        # cls -> ClassInfo
        flat_hash_map[uint64_t, PyObject *] _c_classes_info
        # hash -> ClassInfo
        flat_hash_map[pair[int64_t, int64_t], PyObject *] _c_meta_hash_to_classinfo
        MetaStringResolver meta_string_resolver

    def __init__(self, fury):
        self.fury = fury
        self.metastring_resolver = fury.metastring_resolver
        from pyfury._registry import ClassResolver
        self._resolver = ClassResolver(fury)

    def initialize(self):
        self._resolver.initialize()
        for classinfo in self._resolver._classes_info.values():
            self._populate_typeinfo(classinfo)

    def register_type(
            self,
            cls: Union[type, TypeVar],
            *,
            type_id: int = None,
            namespace: str = None,
            typename: str = None,
            serializer=None,
    ):
        typeinfo = self._resolver.register_type(
            cls,
            type_id=type_id,
            namespace=namespace,
            typename=typename,
            serializer=serializer,
        )
        self._populate_typeinfo(typeinfo)

    cdef _populate_typeinfo(self, typeinfo):
        type_id = typeinfo.type_id
        if type_id >= self._c_registered_id_to_class_info.size():
            self._c_registered_id_to_class_info.resize(type_id * 2, NULL)
        if type_id > 0 and (self.fury.language == Language.PYTHON or not IsNamespacedType(type_id)):
            self._c_registered_id_to_class_info[type_id] = <PyObject *> typeinfo
        self._c_classes_info[<uintptr_t> <PyObject *> typeinfo.cls] = <PyObject *> typeinfo
        if typeinfo.typename_bytes is not None:
            self._load_bytes_to_classinfo(type_id, typeinfo.namespace_bytes, typeinfo.typename_bytes)

    def register_serializer(self, cls: Union[type, TypeVar], serializer):
        classinfo1 = self._resolver.get_classinfo(cls)
        self._resolver.register_serializer(cls, serializer)
        classinfo2 = self._resolver.get_classinfo(cls)
        if classinfo1.type_id != classinfo2.type_id:
            self._c_registered_id_to_class_info[classinfo1.type_id] = NULL
            self._populate_typeinfo(classinfo2)

    cpdef inline Serializer get_serializer(self, cls):
        """
        Returns
        -------
            Returns or create serializer for the provided class
        """
        return self.get_classinfo(cls).serializer

    cpdef inline ClassInfo get_classinfo(self, cls, create=True):
        cdef PyObject * classinfo_ptr = self._c_classes_info[<uintptr_t> <PyObject *> cls]
        cdef ClassInfo class_info
        if classinfo_ptr != NULL:
            class_info = <object> classinfo_ptr
            if class_info.serializer is not None:
                return class_info
            else:
                class_info.serializer = self._resolver._create_serializer(cls)
                return class_info
        elif not create:
            return None
        else:
            class_info = self._resolver.get_classinfo(cls, create=create)
            self._c_classes_info[<uintptr_t> <PyObject *> cls] = <PyObject *> class_info
            self._populate_typeinfo(class_info)
            return class_info

    cpdef inline write_classinfo(self, Buffer buffer, ClassInfo classinfo):
        if classinfo.dynamic_type:
            return
        cdef int32_t type_id = classinfo.type_id
        if type_id != NO_CLASS_ID:
            buffer.write_varuint32((type_id << 1))
            return
        buffer.write_varuint32(1)
        self.metastring_resolver.write_meta_string_bytes(
            buffer, classinfo.namespace_bytes
        )
        self.metastring_resolver.write_meta_string_bytes(
            buffer, classinfo.typename_bytes
        )

    cpdef inline ClassInfo read_classinfo(self, Buffer buffer):
        cdef int32_t h1 = buffer.read_varuint32()
        cdef int32_t type_id = h1 >> 1
        cdef ClassInfo classinfo
        cdef PyObject * classinfo_ptr
        # registered class id are greater than `NO_CLASS_ID`.
        if h1 & 0b1 == 0:
            if type_id < 0 or type_id >= self._c_registered_id_to_class_info.size():
                raise ValueError(f"Unexpected type_id {type_id}")
            classinfo_ptr = self._c_registered_id_to_class_info[type_id]
            if classinfo_ptr == NULL:
                raise ValueError(f"Unexpected type_id {type_id}")
            classinfo = <ClassInfo> classinfo_ptr
            if classinfo.serializer is None:
                classinfo.serializer = self._resolver._create_serializer(classinfo.cls)
            return classinfo
        cdef MetaStringBytes ns_metabytes = self.metastring_resolver.read_meta_string_bytes(buffer)
        cdef MetaStringBytes type_metabytes = self.metastring_resolver.read_meta_string_bytes(buffer)
        return self._load_bytes_to_classinfo(type_id, ns_metabytes, type_metabytes)

    cdef inline ClassInfo _load_bytes_to_classinfo(
            self, int32_t type_id, MetaStringBytes ns_metabytes, MetaStringBytes type_metabytes):
        cdef PyObject * classinfo_ptr = self._c_meta_hash_to_classinfo[
            pair[int64_t, int64_t](ns_metabytes.hashcode, type_metabytes.hashcode)]
        if classinfo_ptr != NULL:
            return <ClassInfo> classinfo_ptr
        classinfo = self._resolver._load_metabytes_to_classinfo(ns_metabytes, type_metabytes)
        classinfo_ptr = <PyObject *> classinfo
        self._c_meta_hash_to_classinfo[pair[int64_t, int64_t](
            ns_metabytes.hashcode, type_metabytes.hashcode)] = classinfo_ptr
        return classinfo

    cpdef write_typeinfo(self, Buffer buffer, ClassInfo classinfo):
        if classinfo.dynamic_type:
            return
        cdef:
            int32_t type_id = classinfo.type_id
            int32_t internal_type_id = type_id & 0xFF
        buffer.write_varuint32(type_id)
        if IsNamespacedType(internal_type_id):
            self.metastring_resolver.write_meta_string_bytes(buffer, classinfo.namespace_bytes)
            self.metastring_resolver.write_meta_string_bytes(buffer, classinfo.typename_bytes)

    cpdef inline ClassInfo read_typeinfo(self, Buffer buffer):
        cdef:
            int32_t type_id = buffer.read_varuint32()
            int32_t internal_type_id = type_id & 0xFF
        cdef MetaStringBytes namespace_bytes, typename_bytes
        if IsNamespacedType(internal_type_id):
            namespace_bytes = self.metastring_resolver.read_meta_string_bytes(buffer)
            typename_bytes = self.metastring_resolver.read_meta_string_bytes(buffer)
            return self._load_bytes_to_classinfo(type_id, namespace_bytes, typename_bytes)
        if type_id < 0 or type_id > self._c_registered_id_to_class_info.size():
            raise ValueError(f"Unexpected type_id {type_id}")
        classinfo_ptr = self._c_registered_id_to_class_info[type_id]
        if classinfo_ptr == NULL:
            raise ValueError(f"Unexpected type_id {type_id}")
        classinfo = <ClassInfo> classinfo_ptr
        return classinfo

    cpdef inline reset(self):
        pass

    cpdef inline reset_read(self):
        pass

    cpdef inline reset_write(self):
        pass


@cython.final
cdef class Fury:
    cdef readonly object language
    cdef readonly c_bool ref_tracking
    cdef readonly c_bool require_class_registration
    cdef readonly MapRefResolver ref_resolver
    cdef readonly ClassResolver class_resolver
    cdef readonly MetaStringResolver metastring_resolver
    cdef readonly SerializationContext serialization_context
    cdef Buffer buffer
    cdef public object pickler  # pickle.Pickler
    cdef public object unpickler  # Optional[pickle.Unpickler]
    cdef object _buffer_callback
    cdef object _buffers  # iterator
    cdef object _unsupported_callback
    cdef object _unsupported_objects  # iterator
    cdef object _peer_language

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
        self.metastring_resolver = MetaStringResolver()
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
            self.pickler = Pickler(self.buffer)
        else:
            self.pickler = _PicklerStub()
            self.unpickler = _UnpicklerStub()
        self.unpickler = None
        self._buffer_callback = None
        self._buffers = None
        self._unsupported_callback = None
        self._unsupported_objects = None
        self._peer_language = None

    def register_serializer(self, cls: Union[type, TypeVar], Serializer serializer):
        self.class_resolver.register_serializer(cls, serializer)

    def register_type(
            self,
            cls: Union[type, TypeVar],
            *,
            type_id: int = None,
            namespace: str = None,
            typename: str = None,
            serializer=None,
    ):
        self.class_resolver.register_type(
            cls, type_id=type_id, namespace=namespace, typename=typename, serializer=serializer)

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
            self.pickler = Pickler(self.buffer)
        else:
            self.buffer.writer_index = 0
            buffer = self.buffer
        if self.language == Language.XLANG:
            buffer.write_int16(MAGIC_NUMBER)
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
            self.xserialize_ref(buffer, obj)
        if buffer is not self.buffer:
            return buffer
        else:
            return buffer.to_bytes(0, buffer.writer_index)

    cpdef inline serialize_ref(
            self, Buffer buffer, obj, ClassInfo classinfo=None):
        cls = type(obj)
        if cls is str:
            buffer.write_int16(NOT_NULL_STRING_FLAG)
            buffer.write_string(obj)
            return
        elif cls is int:
            buffer.write_int16(NOT_NULL_PYINT_FLAG)
            buffer.write_varint64(obj)
            return
        elif cls is bool:
            buffer.write_int16(NOT_NULL_PYBOOL_FLAG)
            buffer.write_bool(obj)
            return
        elif cls is float:
            buffer.write_int16(NOT_NULL_PYFLOAT_FLAG)
            buffer.write_double(obj)
            return
        if self.ref_resolver.write_ref_or_null(buffer, obj):
            return
        if classinfo is None:
            classinfo = self.class_resolver.get_classinfo(cls)
        self.class_resolver.write_classinfo(buffer, classinfo)
        classinfo.serializer.write(buffer, obj)

    cpdef inline serialize_nonref(self, Buffer buffer, obj):
        cls = type(obj)
        if cls is str:
            buffer.write_varuint32(STRING_CLASS_ID << 1)
            buffer.write_string(obj)
            return
        elif cls is int:
            buffer.write_varuint32(PYINT_CLASS_ID << 1)
            buffer.write_varint64(obj)
            return
        elif cls is bool:
            buffer.write_varuint32(PYBOOL_CLASS_ID << 1)
            buffer.write_bool(obj)
            return
        elif cls is float:
            buffer.write_varuint32(PYFLOAT_CLASS_ID << 1)
            buffer.write_double(obj)
            return
        cdef ClassInfo classinfo = self.class_resolver.get_classinfo(cls)
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
        if serializer is None:
            classinfo = self.class_resolver.get_classinfo(type(obj))
            self.class_resolver.write_typeinfo(buffer, classinfo)
            serializer = classinfo.serializer
        serializer.xwrite(buffer, obj)

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
        if not self.require_class_registration:
            self.unpickler = Unpickler(buffer)
        if unsupported_objects is not None:
            self._unsupported_objects = iter(unsupported_objects)
        if self.language == Language.XLANG:
            magic_numer = buffer.read_int16()
            assert magic_numer == MAGIC_NUMBER, (
                f"The fury xlang serialization must start with magic number {hex(MAGIC_NUMBER)}. "
                "Please check whether the serialization is based on the xlang protocol and the "
                "data didn't corrupt."
            )
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

    cpdef inline xdeserialize_ref(self, Buffer buffer, Serializer serializer=None):
        cdef MapRefResolver ref_resolver
        cdef int32_t ref_id
        if serializer is None or serializer.need_to_write_ref:
            ref_resolver = self.ref_resolver
            ref_id = ref_resolver.try_preserve_ref_id(buffer)
            # indicates that the object is first read.
            if ref_id >= NOT_NULL_VALUE_FLAG:
                o = self.xdeserialize_nonref(
                    buffer, serializer=serializer
                )
                ref_resolver.set_read_object(ref_id, o)
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
        if serializer is None:
            serializer = self.class_resolver.read_typeinfo(buffer).serializer
        return serializer.xread(buffer)

    cpdef inline write_buffer_object(self, Buffer buffer, buffer_object):
        if self._buffer_callback is not None and self._buffer_callback(buffer_object):
            buffer.write_bool(False)
            return
        buffer.write_bool(True)
        cdef int32_t size = buffer_object.total_bytes()
        # writer length.
        buffer.write_varuint32(size)
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
        cdef int32_t size = buffer.read_varuint32()
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
            if self.unpickler is None:
                self.unpickler.buffer = Unpickler(buffer)
            return self.unpickler.load()
        else:
            assert self._unsupported_objects is not None
            return next(self._unsupported_objects)

    cpdef inline write_ref_pyobject(
            self, Buffer buffer, value, ClassInfo classinfo=None):
        if self.ref_resolver.write_ref_or_null(buffer, value):
            return
        if classinfo is None:
            classinfo = self.class_resolver.get_classinfo(type(value))
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
        self.metastring_resolver.reset_write()
        self.serialization_context.reset()
        self.pickler.clear_memo()
        self._unsupported_callback = None

    cpdef inline reset_read(self):
        self.ref_resolver.reset_read()
        self.class_resolver.reset_read()
        self.metastring_resolver.reset_read()
        self.serialization_context.reset()
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
    cpdef xwrite(self, Buffer buffer, value):
        self.write(buffer, value)

    cpdef xread(self, Buffer buffer):
        return self.read(buffer)


@cython.final
cdef class BooleanSerializer(CrossLanguageCompatibleSerializer):
    cpdef inline write(self, Buffer buffer, value):
        buffer.write_bool(value)

    cpdef inline read(self, Buffer buffer):
        return buffer.read_bool()


@cython.final
cdef class ByteSerializer(CrossLanguageCompatibleSerializer):
    cpdef inline write(self, Buffer buffer, value):
        buffer.write_int8(value)

    cpdef inline read(self, Buffer buffer):
        return buffer.read_int8()


@cython.final
cdef class Int16Serializer(CrossLanguageCompatibleSerializer):
    cpdef inline write(self, Buffer buffer, value):
        buffer.write_int16(value)

    cpdef inline read(self, Buffer buffer):
        return buffer.read_int16()


@cython.final
cdef class Int32Serializer(CrossLanguageCompatibleSerializer):
    cpdef inline write(self, Buffer buffer, value):
        buffer.write_varint32(value)

    cpdef inline read(self, Buffer buffer):
        return buffer.read_varint32()


@cython.final
cdef class Int64Serializer(CrossLanguageCompatibleSerializer):
    cpdef inline xwrite(self, Buffer buffer, value):
        buffer.write_varint64(value)

    cpdef inline xread(self, Buffer buffer):
        return buffer.read_varint64()

    cpdef inline write(self, Buffer buffer, value):
        buffer.write_varint64(value)

    cpdef inline read(self, Buffer buffer):
        return buffer.read_varint64()

cdef int64_t INT8_MIN_VALUE = -1 << 7
cdef int64_t INT8_MAX_VALUE = 1 << 7 - 1
cdef int64_t INT16_MIN_VALUE = -1 << 15
cdef int64_t INT16_MAX_VALUE = 1 << 15 - 1
cdef int64_t INT32_MIN_VALUE = -1 << 31
cdef int64_t INT32_MAX_VALUE = 1 << 31 - 1
cdef float FLOAT32_MIN_VALUE = 1.17549e-38
cdef float FLOAT32_MAX_VALUE = 3.40282e+38


@cython.final
cdef class DynamicIntSerializer(CrossLanguageCompatibleSerializer):
    cpdef inline xwrite(self, Buffer buffer, value):
        # TODO(chaokunyang) check value range and write type and value
        buffer.write_varuint32(<int32_t> TypeId.INT64)
        buffer.write_varint64(value)

    cpdef inline xread(self, Buffer buffer):
        type_id = buffer.read_varuint32()
        assert type_id == <int32_t> TypeId.INT64, type_id
        return buffer.read_varint64()


@cython.final
cdef class FloatSerializer(CrossLanguageCompatibleSerializer):
    cpdef inline write(self, Buffer buffer, value):
        buffer.write_float(value)

    cpdef inline read(self, Buffer buffer):
        return buffer.read_float()


@cython.final
cdef class DoubleSerializer(CrossLanguageCompatibleSerializer):
    cpdef inline write(self, Buffer buffer, value):
        buffer.write_double(value)

    cpdef inline read(self, Buffer buffer):
        return buffer.read_double()


@cython.final
cdef class DynamicFloatSerializer(CrossLanguageCompatibleSerializer):
    cpdef inline xwrite(self, Buffer buffer, value):
        # TODO(chaokunyang) check value range and write type and value
        buffer.write_varuint32(<int32_t> TypeId.FLOAT64)
        buffer.write_double(value)

    cpdef inline xread(self, Buffer buffer):
        cdef int32_t type_id = buffer.read_varuint32()
        assert type_id == <int32_t> TypeId.FLOAT64, type_id
        return buffer.read_double()


@cython.final
cdef class StringSerializer(CrossLanguageCompatibleSerializer):
    cpdef inline write(self, Buffer buffer, value):
        buffer.write_string(value)

    cpdef inline read(self, Buffer buffer):
        return buffer.read_string()


cdef _base_date = datetime.date(1970, 1, 1)


@cython.final
cdef class DateSerializer(CrossLanguageCompatibleSerializer):
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


"""
Collection serialization format:
https://fury.apache.org/docs/specification/fury_xlang_serialization_spec/#list
Has the following changes:
* None has an independent NonType type, so COLLECTION_NOT_SAME_TYPE can also cover the concept of being nullable.
* No flag is needed to indicate that the element type is not the declared type.
"""
cdef int8_t COLLECTION_DEFAULT_FLAG = 0b0
cdef int8_t COLLECTION_TRACKING_REF = 0b1
cdef int8_t COLLECTION_NOT_SAME_TYPE = 0b1000

cdef class CollectionSerializer(Serializer):
    cdef ClassResolver class_resolver
    cdef MapRefResolver ref_resolver
    cdef Serializer elem_serializer

    def __init__(self, fury, type_, elem_serializer=None):
        super().__init__(fury, type_)
        self.class_resolver = fury.class_resolver
        self.ref_resolver = fury.ref_resolver
        self.elem_serializer = elem_serializer

    cdef pair[int8_t, int64_t] write_header(self, Buffer buffer, value):
        cdef int8_t collect_flag = COLLECTION_DEFAULT_FLAG
        elem_type = type(next(iter(value)))
        for s in value:
            if type(s) is not elem_type:
                collect_flag |= COLLECTION_NOT_SAME_TYPE
                break
        if self.fury.ref_tracking:
            collect_flag |= COLLECTION_TRACKING_REF
        buffer.write_varint64((len(value) << 4) | collect_flag)
        return pair[int8_t, int64_t](collect_flag, obj2int(elem_type))

    cpdef write(self, Buffer buffer, value):
        if len(value) == 0:
            buffer.write_varint64(0)
            return
        cdef pair[int8_t, int64_t] header_pair = self.write_header(buffer, value)
        cdef int8_t collect_flag = header_pair.first
        cdef int64_t elem_type_ptr = header_pair.second
        cdef elem_type = <type> int2obj(elem_type_ptr)
        cdef MapRefResolver ref_resolver = self.ref_resolver
        cdef ClassResolver class_resolver = self.class_resolver
        if (collect_flag & COLLECTION_NOT_SAME_TYPE) == 0:
            if elem_type is str:
                self._write_string(buffer, value)
            elif elem_type is int:
                self._write_int(buffer, value)
            elif elem_type is bool:
                self._write_bool(buffer, value)
            elif elem_type is float:
                self._write_float(buffer, value)
            else:
                if (collect_flag & COLLECTION_TRACKING_REF) == 0:
                    self._write_same_type_no_ref(buffer, value, elem_type)
                else:
                    self._write_same_type_ref(buffer, value, elem_type)
        else:
            for s in value:
                cls = type(s)
                if cls is str:
                    buffer.write_int16(NOT_NULL_STRING_FLAG)
                    buffer.write_string(s)
                elif cls is int:
                    buffer.write_int16(NOT_NULL_PYINT_FLAG)
                    buffer.write_varint64(s)
                elif cls is bool:
                    buffer.write_int16(NOT_NULL_PYBOOL_FLAG)
                    buffer.write_bool(s)
                elif cls is float:
                    buffer.write_int16(NOT_NULL_PYFLOAT_FLAG)
                    buffer.write_double(s)
                else:
                    if not ref_resolver.write_ref_or_null(buffer, s):
                        classinfo = class_resolver.get_classinfo(cls)
                        class_resolver.write_classinfo(buffer, classinfo)
                        classinfo.serializer.write(buffer, s)

    cdef inline _write_string(self, Buffer buffer, value):
        buffer.write_int16(NOT_NULL_STRING_FLAG)
        for s in value:
            buffer.write_string(s)

    cdef inline _read_string(self, Buffer buffer, int64_t len_, object collection_):
        assert buffer.read_int16() == NOT_NULL_STRING_FLAG
        for i in range(len_):
            self._add_element(collection_, i, buffer.read_string())

    cdef inline _write_int(self, Buffer buffer, value):
        buffer.write_int16(NOT_NULL_PYINT_FLAG)
        for s in value:
            buffer.write_varint64(s)

    cdef inline _read_int(self, Buffer buffer, int64_t len_, object collection_):
        assert buffer.read_int16() == NOT_NULL_PYINT_FLAG
        for i in range(len_):
            self._add_element(collection_, i, buffer.read_varint64())

    cdef inline _write_bool(self, Buffer buffer, value):
        buffer.write_int16(NOT_NULL_PYBOOL_FLAG)
        for s in value:
            buffer.write_bool(s)

    cdef inline _read_bool(self, Buffer buffer, int64_t len_, object collection_):
        assert buffer.read_int16() == NOT_NULL_PYBOOL_FLAG
        for i in range(len_):
            self._add_element(collection_, i, buffer.read_bool())

    cdef inline _write_float(self, Buffer buffer, value):
        buffer.write_int16(NOT_NULL_PYFLOAT_FLAG)
        for s in value:
            buffer.write_double(s)

    cdef inline _read_float(self, Buffer buffer, int64_t len_, object collection_):
        assert buffer.read_int16() == NOT_NULL_PYFLOAT_FLAG
        for i in range(len_):
            self._add_element(collection_, i, buffer.read_double())

    cpdef _write_same_type_no_ref(self, Buffer buffer, value, elem_type):
        cdef MapRefResolver ref_resolver = self.ref_resolver
        cdef ClassResolver class_resolver = self.class_resolver
        classinfo = class_resolver.get_classinfo(elem_type)
        class_resolver.write_classinfo(buffer, classinfo)
        for s in value:
            classinfo.serializer.write(buffer, s)

    cpdef _read_same_type_no_ref(self, Buffer buffer, int64_t len_, object collection_):
        cdef MapRefResolver ref_resolver = self.ref_resolver
        cdef ClassResolver class_resolver = self.class_resolver
        classinfo = class_resolver.read_classinfo(buffer)
        for i in range(len_):
            obj = classinfo.serializer.read(buffer)
            self._add_element(collection_, i, obj)

    cpdef _write_same_type_ref(self, Buffer buffer, value, elem_type):
        cdef MapRefResolver ref_resolver = self.ref_resolver
        cdef ClassResolver class_resolver = self.class_resolver
        classinfo = class_resolver.get_classinfo(elem_type)
        class_resolver.write_classinfo(buffer, classinfo)
        for s in value:
            if not ref_resolver.write_ref_or_null(buffer, s):
                classinfo.serializer.write(buffer, s)

    cpdef _read_same_type_ref(self, Buffer buffer, int64_t len_, object collection_):
        cdef MapRefResolver ref_resolver = self.ref_resolver
        cdef ClassResolver class_resolver = self.class_resolver
        classinfo = class_resolver.read_classinfo(buffer)
        for i in range(len_):
            ref_id = ref_resolver.try_preserve_ref_id(buffer)
            if ref_id < NOT_NULL_VALUE_FLAG:
                obj = ref_resolver.get_read_object()
            else:
                obj = classinfo.serializer.read(buffer)
                ref_resolver.set_read_object(ref_id, obj)
            self._add_element(collection_, i, obj)

    cpdef _add_element(self, object collection_, int64_t index, object element):
        raise NotImplementedError

    cpdef xwrite(self, Buffer buffer, value):
        cdef int32_t len_ = 0
        try:
            len_ = len(value)
        except AttributeError:
            value = list(value)
            len_ = len(value)
        buffer.write_varuint32(len_)
        for s in value:
            self.fury.xserialize_ref(
                buffer, s, serializer=self.elem_serializer
            )
            len_ += 1

cdef class ListSerializer(CollectionSerializer):
    cpdef read(self, Buffer buffer):
        cdef MapRefResolver ref_resolver = self.fury.ref_resolver
        cdef ClassResolver class_resolver = self.fury.class_resolver
        cdef int64_t len_and_flag = buffer.read_varint64()
        cdef int64_t len_ = len_and_flag >> 4
        cdef int8_t collect_flag = <int8_t> (len_and_flag & 0xF)
        cdef list list_ = PyList_New(len_)
        ref_resolver.reference(list_)
        if len_ == 0:
            return list_
        if (collect_flag & COLLECTION_NOT_SAME_TYPE) == 0:
            type_flag = buffer.get_int16(buffer.reader_index)
            if type_flag == NOT_NULL_STRING_FLAG:
                self._read_string(buffer, len_, list_)
            elif type_flag == NOT_NULL_PYINT_FLAG:
                self._read_int(buffer, len_, list_)
            elif type_flag == NOT_NULL_PYBOOL_FLAG:
                self._read_bool(buffer, len_, list_)
            elif type_flag == NOT_NULL_PYFLOAT_FLAG:
                self._read_float(buffer, len_, list_)
            else:
                if (collect_flag & COLLECTION_TRACKING_REF) == 0:
                    self._read_same_type_no_ref(buffer, len_, list_)
                else:
                    self._read_same_type_ref(buffer, len_, list_)
        else:
            for i in range(len_):
                elem = get_next_elenment(buffer, ref_resolver, class_resolver)
                Py_INCREF(elem)
                PyList_SET_ITEM(list_, i, elem)
        return list_

    cpdef _add_element(self, object collection_, int64_t index, object element):
        Py_INCREF(element)
        PyList_SET_ITEM(collection_, index, element)

    cpdef xread(self, Buffer buffer):
        cdef int32_t len_ = buffer.read_varuint32()
        cdef list collection_ = PyList_New(len_)
        self.fury.ref_resolver.reference(collection_)
        for i in range(len_):
            elem = self.fury.xdeserialize_ref(
                buffer, serializer=self.elem_serializer
            )
            Py_INCREF(elem)
            PyList_SET_ITEM(collection_, i, elem)
        return collection_

cdef inline get_next_elenment(
        Buffer buffer,
        MapRefResolver ref_resolver,
        ClassResolver class_resolver):
    cdef int32_t ref_id
    cdef ClassInfo classinfo
    ref_id = ref_resolver.try_preserve_ref_id(buffer)
    if ref_id < NOT_NULL_VALUE_FLAG:
        return ref_resolver.get_read_object()
    # indicates that the object is first read.
    classinfo = class_resolver.read_classinfo(buffer)
    cls = classinfo.cls
    # Note that all read operations in fast paths of list/tuple/set/dict/sub_dict
    # ust match corresponding writing operations. Otherwise, ref tracking will
    # error.
    if cls is str:
        return buffer.read_string()
    elif cls is int:
        return buffer.read_varint64()
    elif cls is bool:
        return buffer.read_bool()
    elif cls is float:
        return buffer.read_double()
    else:
        o = classinfo.serializer.read(buffer)
        ref_resolver.set_read_object(ref_id, o)
        return o


@cython.final
cdef class TupleSerializer(CollectionSerializer):
    cpdef inline read(self, Buffer buffer):
        cdef MapRefResolver ref_resolver = self.fury.ref_resolver
        cdef ClassResolver class_resolver = self.fury.class_resolver
        cdef int64_t len_and_flag = buffer.read_varint64()
        cdef int64_t len_ = len_and_flag >> 4
        cdef int8_t collect_flag = <int8_t> (len_and_flag & 0xF)
        cdef tuple tuple_ = PyTuple_New(len_)
        if len_ == 0:
            return tuple_
        if (collect_flag & COLLECTION_NOT_SAME_TYPE) == 0:
            type_flag = buffer.get_int16(buffer.reader_index)
            if type_flag == NOT_NULL_STRING_FLAG:
                self._read_string(buffer, len_, tuple_)
            elif type_flag == NOT_NULL_PYINT_FLAG:
                self._read_int(buffer, len_, tuple_)
            elif type_flag == NOT_NULL_PYBOOL_FLAG:
                self._read_bool(buffer, len_, tuple_)
            elif type_flag == NOT_NULL_PYFLOAT_FLAG:
                self._read_float(buffer, len_, tuple_)
            else:
                if (collect_flag & COLLECTION_TRACKING_REF) == 0:
                    self._read_same_type_no_ref(buffer, len_, tuple_)
                else:
                    self._read_same_type_ref(buffer, len_, tuple_)
        else:
            for i in range(len_):
                elem = get_next_elenment(buffer, ref_resolver, class_resolver)
                Py_INCREF(elem)
                PyTuple_SET_ITEM(tuple_, i, elem)
        return tuple_

    cpdef inline _add_element(self, object collection_, int64_t index, object element):
        Py_INCREF(element)
        PyTuple_SET_ITEM(collection_, index, element)

    cpdef inline xread(self, Buffer buffer):
        cdef int32_t len_ = buffer.read_varuint32()
        cdef tuple tuple_ = PyTuple_New(len_)
        for i in range(len_):
            elem = self.fury.xdeserialize_ref(
                buffer, serializer=self.elem_serializer
            )
            Py_INCREF(elem)
            PyTuple_SET_ITEM(tuple_, i, elem)
        return tuple_


@cython.final
cdef class StringArraySerializer(ListSerializer):
    def __init__(self, fury, type_):
        super().__init__(fury, type_, StringSerializer(fury, str))


@cython.final
cdef class SetSerializer(CollectionSerializer):
    cpdef inline read(self, Buffer buffer):
        cdef MapRefResolver ref_resolver = self.fury.ref_resolver
        cdef ClassResolver class_resolver = self.fury.class_resolver
        cdef set instance = set()
        ref_resolver.reference(instance)
        cdef int64_t len_and_flag = buffer.read_varint64()
        cdef int64_t len_ = len_and_flag >> 4
        cdef int8_t collect_flag = <int8_t> (len_and_flag & 0xF)
        cdef int32_t ref_id
        cdef ClassInfo classinfo
        if len_ == 0:
            return instance
        if (collect_flag & COLLECTION_NOT_SAME_TYPE) == 0:
            type_flag = buffer.get_int16(buffer.reader_index)
            if type_flag == NOT_NULL_STRING_FLAG:
                self._read_string(buffer, len_, instance)
            elif type_flag == NOT_NULL_PYINT_FLAG:
                self._read_int(buffer, len_, instance)
            elif type_flag == NOT_NULL_PYBOOL_FLAG:
                self._read_bool(buffer, len_, instance)
            elif type_flag == NOT_NULL_PYFLOAT_FLAG:
                self._read_float(buffer, len_, instance)
            else:
                if (collect_flag & COLLECTION_TRACKING_REF) == 0:
                    self._read_same_type_no_ref(buffer, len_, instance)
                else:
                    self._read_same_type_ref(buffer, len_, instance)
        else:
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

    cpdef inline _add_element(self, object collection_, int64_t index, object element):
        collection_.add(element)

    cpdef inline xread(self, Buffer buffer):
        cdef int32_t len_ = buffer.read_varuint32()
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

    cpdef inline write(self, Buffer buffer, o):
        cdef dict value = o
        buffer.write_varuint32(len(value))
        cdef ClassInfo key_classinfo
        cdef ClassInfo value_classinfo
        cdef int64_t key_addr, value_addr
        cdef Py_ssize_t pos = 0
        while PyDict_Next(value, &pos, <PyObject **>&key_addr, <PyObject **>&value_addr) != 0:
            k = int2obj(key_addr)
            Py_INCREF(k)
            v = int2obj(value_addr)
            Py_INCREF(v)
            key_cls = type(k)
            if key_cls is str:
                buffer.write_int16(NOT_NULL_STRING_FLAG)
                buffer.write_string(k)
            else:
                if not self.ref_resolver.write_ref_or_null(buffer, k):
                    key_classinfo = self.class_resolver.get_classinfo(key_cls)
                    self.class_resolver.write_classinfo(buffer, key_classinfo)
                    key_classinfo.serializer.write(buffer, k)
            value_cls = type(v)
            if value_cls is str:
                buffer.write_int16(NOT_NULL_STRING_FLAG)
                buffer.write_string(v)
            elif value_cls is int:
                buffer.write_int16(NOT_NULL_PYINT_FLAG)
                buffer.write_varint64(v)
            elif value_cls is bool:
                buffer.write_int16(NOT_NULL_PYBOOL_FLAG)
                buffer.write_bool(v)
            elif value_cls is float:
                buffer.write_int16(NOT_NULL_PYFLOAT_FLAG)
                buffer.write_double(v)
            else:
                if not self.ref_resolver.write_ref_or_null(buffer, v):
                    value_classinfo = self.class_resolver. \
                        get_classinfo(value_cls)
                    self.class_resolver.write_classinfo(buffer, value_classinfo)
                    value_classinfo.serializer.write(buffer, v)

    cpdef inline read(self, Buffer buffer):
        cdef MapRefResolver ref_resolver = self.ref_resolver
        cdef ClassResolver class_resolver = self.class_resolver
        cdef int32_t len_ = buffer.read_varuint32()
        cdef dict map_ = _PyDict_NewPresized(len_)
        ref_resolver.reference(map_)
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
        buffer.write_varuint32(len(value))
        cdef int64_t key_addr, value_addr
        cdef Py_ssize_t pos = 0
        while PyDict_Next(value, &pos, <PyObject **>&key_addr, <PyObject **>&value_addr) != 0:
            k = int2obj(key_addr)
            Py_INCREF(k)
            v = int2obj(value_addr)
            Py_INCREF(v)
            self.fury.xserialize_ref(
                buffer, k, serializer=self.key_serializer
            )
            self.fury.xserialize_ref(
                buffer, v, serializer=self.value_serializer
            )

    cpdef inline xread(self, Buffer buffer):
        cdef int32_t len_ = buffer.read_varuint32()
        cdef dict map_ = _PyDict_NewPresized(len_)
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
        buffer.write_varuint32(len(value))
        cdef ClassInfo key_classinfo
        cdef ClassInfo value_classinfo
        for k, v in value.items():
            key_cls = type(k)
            if key_cls is str:
                buffer.write_int16(NOT_NULL_STRING_FLAG)
                buffer.write_string(k)
            else:
                if not self.ref_resolver.write_ref_or_null(buffer, k):
                    key_classinfo = self.class_resolver.get_classinfo(key_cls)
                    self.class_resolver.write_classinfo(buffer, key_classinfo)
                    key_classinfo.serializer.write(buffer, k)
            value_cls = type(v)
            if value_cls is str:
                buffer.write_int16(NOT_NULL_STRING_FLAG)
                buffer.write_string(v)
            elif value_cls is int:
                buffer.write_int16(NOT_NULL_PYINT_FLAG)
                buffer.write_varint64(v)
            elif value_cls is bool:
                buffer.write_int16(NOT_NULL_PYBOOL_FLAG)
                buffer.write_bool(v)
            elif value_cls is float:
                buffer.write_int16(NOT_NULL_PYFLOAT_FLAG)
                buffer.write_double(v)
            else:
                if not self.ref_resolver.write_ref_or_null(buffer, v):
                    value_classinfo = self.class_resolver. \
                        get_classinfo(value_cls)
                    self.class_resolver.write_classinfo(buffer, value_classinfo)
                    value_classinfo.serializer.write(buffer, v)

    cpdef inline read(self, Buffer buffer):
        cdef MapRefResolver ref_resolver = self.fury.ref_resolver
        cdef ClassResolver class_resolver = self.fury.class_resolver
        map_ = self.type_()
        ref_resolver.reference(map_)
        cdef int32_t len_ = buffer.read_varuint32()
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
            buffer.write_int16(NOT_NULL_PYINT_FLAG)
            buffer.write_varint64(start)
        else:
            if start is None:
                buffer.write_int8(NULL_FLAG)
            else:
                buffer.write_int8(NOT_NULL_VALUE_FLAG)
                self.fury.serialize_nonref(buffer, start)
        if type(stop) is int:
            # TODO support varint128
            buffer.write_int16(NOT_NULL_PYINT_FLAG)
            buffer.write_varint64(stop)
        else:
            if stop is None:
                buffer.write_int8(NULL_FLAG)
            else:
                buffer.write_int8(NOT_NULL_VALUE_FLAG)
                self.fury.serialize_nonref(buffer, stop)
        if type(step) is int:
            # TODO support varint128
            buffer.write_int16(NOT_NULL_PYINT_FLAG)
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
