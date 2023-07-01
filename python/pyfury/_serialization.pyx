# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3
# cython: annotate = True
import logging
import os
import sys

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
