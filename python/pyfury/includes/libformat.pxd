# cython: profile=False
# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3

from libc.stdint cimport *
from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr
from libcpp.string cimport string as c_string
from cpython cimport PyObject
from pyarrow.lib cimport CSchema, CField, CListType, CMapType
from pyarrow.lib cimport CStatus, CMemoryPool, CRecordBatch
from pyfury.includes.libutil cimport CBuffer

cimport cpython

cdef inline object PyObject_to_object(PyObject* o):
    # Cast to "object" increments reference count
    cdef object result = <object> o
    cpython.Py_DECREF(result)
    return result


cdef extern from "fury/row/row.h" namespace "fury" nogil:
    cdef cppclass CGetter" fury::Getter":
        shared_ptr[CBuffer] buffer() const

        int base_offset() const

        int size_bytes() const

        c_bool IsNullAt(int i)

        int8_t GetInt8(int i)

        int8_t GetUInt8(int i)

        c_bool GetBoolean(int i)

        int16_t GetInt16(int i)

        int32_t GetInt32(int i)

        int64_t GetInt64(int i)

        float GetFloat(int i)

        double GetDouble(int i)

        c_string GetString(int i)

        int GetBinary(int i, uint8_t** out)

        shared_ptr[CRow] GetStruct(int i)

        shared_ptr[CArrayData] GetArray(int i)

        shared_ptr[CMapData] GetMap(int i)

        c_string ToString()

    cdef cppclass CArrayData" fury::ArrayData"(CGetter):
        CArrayData(shared_ptr[CListType] type)

        int num_elements()

        shared_ptr[CListType] type()

    cdef cppclass CMapData" fury::MapData":
        CMapData(shared_ptr[CMapType] type)

        void PointTo(shared_ptr[CBuffer] buffer,
                     uint32_t offset, uint32_t size_in_bytes)

        int num_elements()

        shared_ptr[CBuffer] buffer() const

        int base_offset() const

        int size_bytes() const

        shared_ptr[CListType] type()

        shared_ptr[CArrayData] keys_array()

        shared_ptr[CArrayData] values_array()

        c_string ToString()

    cdef cppclass CRow" fury::Row"(CGetter):
        Row(shared_ptr[CSchema] schema)

        shared_ptr[CSchema] schema()

        int num_fields()

        void PointTo(shared_ptr[CBuffer] buffer,
                     uint32_t offset, uint32_t size_in_bytes)


cdef extern from "fury/row/writer.h" namespace "fury" nogil:
    cdef cppclass CWriter" fury::Writer":

        shared_ptr[CBuffer]& buffer()

        uint32_t cursor()

        uint32_t size()

        uint32_t starting_offset()

        void IncreaseCursor(uint32_t val)

        void Grow(uint32_t needed_size)

        void SetOffsetAndSize(int i, uint32_t size)

        void SetOffsetAndSize(int i, uint32_t absolute_offset, uint32_t size)

        void ZeroOutPaddingBytes(uint32_t num_bytes)

        void SetNullAt(int i)

        void SetNotNullAt(int i)

        c_bool IsNullAt(int i) const

        void Write(int i, int8_t value)
        void Write(int i, c_bool value)
        void Write(int i, int16_t value)
        void Write(int i, int32_t value)
        void Write(int i, int64_t value)
        void Write(int i, float value)
        void Write(int i, double value)

        void WriteString(int i, c_string &value)

        void WriteBytes(int i, const uint8_t *input, uint32_t length)

        void WriteUnaligned(int i, const uint8_t *input,
                            uint32_t offset, uint32_t num_bytes)

        void WriteDirectly(int64_t value)

        void WriteDirectly(uint32_t offset, int64_t value)

    cdef cppclass CRowWriter" fury::RowWriter"(CWriter):
        CRowWriter(shared_ptr[CSchema] schema)

        CRowWriter(shared_ptr[CSchema] schema, CWriter *writer)

        shared_ptr[CSchema] schema()

        void SetBuffer(shared_ptr[CBuffer]& buffer)

        void Reset()

        shared_ptr[CRow] ToRow()

    cdef cppclass CArrayWriter" fury::ArrayWriter"(CWriter):
        CArrayWriter(shared_ptr[CListType] type_, CWriter *writer)

        void Reset(int num_elements)

        int size()

        shared_ptr[CArrayData] CopyToArrayData()


cdef extern from "fury/columnar/arrow_writer.h" namespace\
        "fury::columnar" nogil:
    cdef cppclass CArrowWriter" fury::columnar::ArrowWriter":
        @staticmethod
        CStatus Make(shared_ptr[CSchema] schema,
                     CMemoryPool *pool,
                     shared_ptr[CArrowWriter] *writer)

        CStatus Write(const shared_ptr[CRow] &row)

        CStatus Finish(shared_ptr[CRecordBatch] *record_batch)

        void Reset()
