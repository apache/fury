# distutils: language = c++
# cython: embedsignature = True
# cython: language_level = 3
# cython: annotate = True

cimport cython
from cpython cimport *
from libcpp.memory cimport shared_ptr, make_shared
from libc.stdint cimport *
from libcpp cimport bool as c_bool
from pyfury.includes.libutil cimport(
    CBuffer, AllocateBuffer, GetBit, SetBit, ClearBit, SetBitTo
)

cdef extern from "Python.h":
    const char* PyUnicode_AsUTF8AndSize(object obj, Py_ssize_t *l) except NULL

cdef int32_t max_buffer_size = 2 ** 31 - 1


@cython.final
cdef class Buffer:
    def __init__(self,  data not None, int offset=0, length=None):
        self.data = data
        assert 0 <= offset <= len(data), f'offset {offset} length {len(data)}'
        cdef int length_
        if length is None:
            length_ = len(data) - offset
        else:
            length_ = length
        assert length_ >= 0, f'length should be >= 0 but got {length}'
        if length_ > 0:
            self._c_address = get_address(data) + offset
        else:
            self._c_address = NULL
        self._c_size = length_
        self.c_buffer = make_shared[CBuffer](self._c_address, length_, False)
        # hold c_address directly to avoid pointer indirect cost.
        self.reader_index = 0
        self.writer_index = 0

    @staticmethod
    cdef Buffer wrap(shared_ptr[CBuffer] c_buffer):
        cdef Buffer buffer = Buffer.__new__(Buffer)
        buffer.c_buffer = c_buffer
        buffer._c_address = c_buffer.get().data()
        buffer._c_size = c_buffer.get().size()
        return buffer

    @classmethod
    def allocate(cls, int32_t size):
        cdef shared_ptr[CBuffer] buf
        if not AllocateBuffer(size, &buf):
            raise MemoryError("out of memory")
        return Buffer.wrap(buf)

    cpdef c_bool own_data(self):
        return self.c_buffer.get().own_data()

    cpdef inline reserve(self, int32_t new_size):
        assert 0 < new_size < max_buffer_size
        self.c_buffer.get().Reserve(new_size)
        self._c_address = self.c_buffer.get().data()
        self._c_size = self.c_buffer.get().size()

    cpdef inline put_bool(self, uint32_t offset, c_bool v):
        self.check_bound(offset, <int32_t>1)
        self.c_buffer.get().UnsafePutByte(offset, v)

    cpdef inline put_int8(self, uint32_t offset, int8_t v):
        self.check_bound(offset, <int32_t>1)
        self.c_buffer.get().UnsafePutByte(offset, v)

    cpdef inline put_int16(self, uint32_t offset, int16_t v):
        self.check_bound(offset, <int32_t>2)
        self.c_buffer.get().UnsafePut(offset, v)

    cpdef inline put_int24(self, uint32_t offset, int32_t v):
        self.check_bound(offset, <int32_t>3)
        cdef uint8_t* arr = self._c_address + offset
        arr[0] = <uint8_t>v
        arr[1] = <uint8_t>(v >> <int32_t>8)
        arr[2] = <uint8_t>(v >> <int32_t>16)

    cpdef inline put_int32(self, uint32_t offset, int32_t v):
        self.check_bound(offset, <int32_t>4)
        self.c_buffer.get().UnsafePut(offset, v)

    cpdef inline put_int64(self, uint32_t offset, int64_t v):
        self.check_bound(offset, <int32_t>8)
        self.c_buffer.get().UnsafePut(offset, v)

    cpdef inline put_float(self, uint32_t offset, float v):
        self.check_bound(offset, <int32_t>4)
        self.c_buffer.get().UnsafePut(offset, v)

    cpdef inline put_double(self, uint32_t offset, double v):
        self.check_bound(offset, <int32_t>8)
        self.c_buffer.get().UnsafePut(offset, v)

    cpdef inline c_bool get_bool(self, uint32_t offset):
        self.check_bound(offset, <int32_t>1)
        return self.c_buffer.get().GetBool(offset)

    cpdef inline int8_t get_int8(self, uint32_t offset):
        self.check_bound(offset, <int32_t>1)
        return self.c_buffer.get().GetInt8(offset)

    cpdef inline int16_t get_int16(self, uint32_t offset):
        self.check_bound(offset, <int32_t>2)
        return self.c_buffer.get().GetInt16(offset)

    cpdef inline int32_t get_int24(self, uint32_t offset):
        self.check_bound(offset, <int32_t>3)
        cdef uint8_t* arr = self._c_address + offset
        cdef int32_t result = arr[0]
        return (result & 0xFF) | (((<int16_t>arr[1]) & 0xFF) << 8) |\
               (((<int16_t>arr[2]) & 0xFF) << 16)

    cpdef inline int32_t get_int32(self, uint32_t offset):
        self.check_bound(offset, <int32_t>4)
        return self.c_buffer.get().GetInt32(offset)

    cpdef inline int64_t get_int64(self, uint32_t offset):
        self.check_bound(offset, <int32_t>8)
        return self.c_buffer.get().GetInt64(offset)

    cpdef inline float get_float(self, uint32_t offset):
        self.check_bound(offset, <int32_t>4)
        return self.c_buffer.get().GetFloat(offset)

    cpdef inline double get_double(self, uint32_t offset):
        self.check_bound(offset, <int32_t>8)
        return self.c_buffer.get().GetDouble(offset)

    cpdef inline check_bound(self, int32_t offset, int32_t length):
        cdef int32_t size_ = self.c_buffer.get().size()
        # if offset + length > size_:
        if offset | length | (offset + length) | (size_- (offset + length)) < 0:
            raise ValueError(f"Address range {offset, offset + length} "
                             f"out of bound {0, size_}")

    cpdef inline write_bool(self, c_bool value):
        self.grow(<int32_t>1)
        (<c_bool *>(self._c_address + self.writer_index))[0] = value
        self.writer_index += <int32_t>1

    cpdef inline write_int8(self, int8_t value):
        self.grow(<int32_t>1)
        (<int8_t *>(self._c_address + self.writer_index))[0] = value
        self.writer_index += <int32_t>1

    cpdef inline write_int16(self, int16_t value):
        self.grow(<int32_t>2)
        self.c_buffer.get().UnsafePut(self.writer_index, value)
        self.writer_index += <int32_t>2

    cpdef inline write_int24(self, int32_t value):
        self.grow(<int32_t>3)
        cdef uint8_t* arr = self._c_address + self.writer_index
        arr[0] = <uint8_t>value
        arr[1] = <uint8_t>(value >> <int32_t>8)
        arr[2] = <uint8_t>(value >> <int32_t>16)
        self.writer_index += <int32_t>3

    cpdef inline write_int32(self, int32_t value):
        self.grow(<int32_t>4)
        self.c_buffer.get().UnsafePut(self.writer_index, value)
        self.writer_index += <int32_t>4

    cpdef inline write_int64(self, int64_t value):
        self.grow(<int32_t>8)
        self.c_buffer.get().UnsafePut(self.writer_index, value)
        self.writer_index += <int32_t>8

    cpdef inline write_float(self, float value):
        self.grow(<int32_t>4)
        self.c_buffer.get().UnsafePut(self.writer_index, value)
        self.writer_index += <int32_t>4

    cpdef inline write_double(self, double value):
        self.grow(<int32_t>8)
        self.c_buffer.get().UnsafePut(self.writer_index, value)
        self.writer_index += <int32_t>8

    cpdef put_buffer(self, uint32_t offset, v, int32_t src_index, int32_t length):
        if length == 0:  # access an emtpy buffer may raise out-of-bound exception.
            return
        view = memoryview(v)
        assert view.c_contiguous
        itemsize = view.itemsize
        size = (length - src_index) * itemsize
        self.check_bound(offset, size)
        src_offset = src_index * itemsize
        cdef uint8_t* ptr = get_address(v)
        self.c_buffer.get().CopyFrom(offset, ptr, src_offset, size)

    cpdef inline write_bytes_and_size(self, bytes value):
        cdef const unsigned char[:] data = value
        cdef int32_t length = data.nbytes
        self.write_varint32(length)
        if length > 0:
            self.grow(length)
            self.c_buffer.get().CopyFrom(self.writer_index, &data[0], 0, length)
            self.writer_index += length

    cpdef inline bytes read_bytes_and_size(self):
        cdef int32_t length = self.read_varint32()
        value = self.get_bytes(self.reader_index, length)
        self.reader_index += length
        return value

    cpdef inline write_bytes(self, bytes value):
        cdef const unsigned char[:] data = value
        cdef int32_t length = data.nbytes
        if length > 0:
            self.grow(length)
            self.c_buffer.get().CopyFrom(self.writer_index, &data[0], 0, length)
            self.writer_index += length

    cpdef inline bytes read_bytes(self, int32_t length):
        value = self.get_bytes(self.reader_index, length)
        self.reader_index += length
        return value

    cpdef inline put_bytes(self, uint32_t offset, bytes value):
        cdef const unsigned char[:] data = value
        cdef int32_t length = data.nbytes
        if length > 0:
            self.grow(length)
            self.c_buffer.get().CopyFrom(offset, &data[0], 0, length)

    cpdef inline bytes get_bytes(self, uint32_t offset, uint32_t nbytes):
        if nbytes == 0:
            return b""
        self.check_bound(offset, nbytes)
        cdef unsigned char* binary_data = self.c_buffer.get().data() + offset
        return binary_data[:nbytes]

    cpdef inline write_buffer(self, value, src_index=0, length_=None):
        view = memoryview(value)
        dtype = view.format
        cdef int32_t itemsize = view.itemsize
        cdef int32_t length = 0
        if length_ is None:
            length = len(value) - src_index
        else:
            length = length_
        self.grow(length * itemsize)
        self.put_buffer(self.writer_index, value, src_index, length)
        self.writer_index += length * itemsize

    cpdef inline write(self, value):
        cdef const unsigned char[:] data = value
        cdef int32_t length = data.nbytes
        if length > 0:
            self.grow(length)
            self.c_buffer.get().CopyFrom(self.writer_index, &data[0], 0, length)
            self.writer_index += length

    cpdef inline grow(self, int32_t needed_size):
        cdef int32_t length = self.writer_index + needed_size
        if length > self._c_size:
            self.reserve(length * 2)

    cpdef inline ensure(self, int32_t length):
        if length > self._c_size:
            self.reserve(length * 2)

    cpdef inline skip(self, int32_t length):
        cdef int32_t offset = self.reader_index
        self.check_bound(offset, length)
        self.reader_index = offset + length

    cpdef inline c_bool read_bool(self):
        cdef int32_t offset = self.reader_index
        self.check_bound(offset, <int32_t>1)
        self.reader_index += <int32_t>1
        return (<c_bool *>(self._c_address + offset))[0]

    cpdef inline int8_t read_int8(self):
        cdef int32_t offset = self.reader_index
        self.check_bound(offset, <int32_t>1)
        self.reader_index += <int32_t>1
        return (<int8_t *>(self._c_address + offset))[0]

    cpdef inline int16_t read_int16(self):
        value = self.get_int16(self.reader_index)
        self.reader_index += <int32_t>2
        return value

    cpdef inline int16_t read_int24(self):
        value = self.get_int24(self.reader_index)
        self.reader_index += <int32_t>3
        return value

    cpdef inline int32_t read_int32(self):
        value = self.get_int32(self.reader_index)
        self.reader_index += <int32_t>4
        return value

    cpdef inline int64_t read_int64(self):
        value = self.get_int64(self.reader_index)
        self.reader_index += <int32_t>8
        return value

    cpdef inline float read_float(self):
        value = self.get_float(self.reader_index)
        self.reader_index += <int32_t>4
        return value

    cpdef inline double read_double(self):
        value = self.get_double(self.reader_index)
        self.reader_index += <int32_t>8
        return value

    cpdef inline bytes read(self, int32_t length):
        return self.read_bytes(length)

    cpdef inline bytes readline(self, int32_t size=-1):
        if size != <int32_t>-1:
            raise ValueError(f"Specify size {size} is unsupported")
        cdef uint8_t* arr = self.c_buffer.get().data()
        cdef int32_t target_index = self.reader_index
        cdef uint8_t sep = 10  # '\n'
        cdef int32_t buffer_size = self._c_size
        while arr[target_index] != sep and target_index < buffer_size:
            target_index += <int32_t>1
        cdef bytes data = arr[self.reader_index:target_index]
        self.reader_index = target_index
        return data

    cpdef inline write_varint32(self, int32_t value):
        self.grow(<int8_t>5)
        cdef int32_t actual_bytes_written = self.c_buffer.get()\
            .PutPositiveVarInt32(self.writer_index, value)
        self.writer_index += actual_bytes_written
        return actual_bytes_written

    cpdef inline int32_t read_varint32(self):
        cdef:
            uint32_t read_length = 0
            int8_t b
            int32_t result
        if self._c_size - self.reader_index > 5:
            result = self.c_buffer.get().GetPositiveVarInt32(
                self.reader_index, &read_length)
            self.reader_index += read_length
            return result
        else:
            b = self.read_int8()
            result = b & 0x7F
            if (b & 0x80) != 0:
                b = self.read_int8()
                result |= (b & 0x7F) << 7
                if (b & 0x80) != 0:
                    b = self.read_int8()
                    result |= (b & 0x7F) << 14
                    if (b & 0x80) != 0:
                        b = self.read_int8()
                        result |= (b & 0x7F) << 21
                        if (b & 0x80) != 0:
                            b = self.read_int8()
                            result |= (b & 0x7F) << 28
            return result

    cpdef inline write_flagged_varint32(self, c_bool flag, int32_t v):
        self.grow(5)
        cdef:
            int32_t value = v
            int32_t offset = self.writer_index
            int8_t first = (value & 0x3F)
            uint8_t* arr = self.c_buffer.get().data()
        if flag:
            # Mask first 6 bits, bit 8 is the flag.
            first = first | 0x80
        if value >> 6 == 0:
            arr[offset] = first
            self.writer_index += 1
            return 1
        if value >> 13 == 0:
            arr[offset] = first | 0x40  # Set bit 7.
            arr[offset + 1] = <int8_t> (value >> 6)
            self.writer_index += 2
            return 2
        if value >> 20 == 0:
            arr[offset] = first | 0x40  # Set bit 7.
            arr[offset + 1] = <int8_t> (value >> 6 | 0x80)
            arr[offset + 2] = <int8_t> (value >> 13)
            self.writer_index += 3
            return 3
        if value >> 27 == 0:
            arr[offset] = first | 0x40  # Set bit 7.
            arr[offset + 1] = <int8_t> (value >> 6 | 0x80)
            arr[offset + 2] = <int8_t> (value >> 13 | 0x80)
            arr[offset + 3] = <int8_t> (value >> 20)
            self.writer_index += 4
            return 4
        arr[offset] = first | 0x40  # Set bit 7.
        arr[offset + 1] = <int8_t> (value >> 6 | 0x80)
        arr[offset + 2] = <int8_t> (value >> 13 | 0x80)
        arr[offset + 3] = <int8_t> (value >> 20 | 0x80)
        arr[offset + 4] = <int8_t> (value >> 27)
        self.writer_index += 5
        return 5

    cpdef inline c_bool read_varint32_flag(self):
        cdef int32_t offset = self.reader_index
        self.check_bound(offset, <int32_t>1)
        cdef int8_t head = (<int8_t *>(self._c_address + offset))[0]
        return (head & 0x80) != 0

    cpdef inline int32_t read_flagged_varint(self):
        cdef:
            uint32_t read_bytes_length = 1
            int32_t b
            int32_t result
            uint32_t position = self.reader_index
            int8_t * arr = <int8_t *> (self.c_buffer.get().data() + position)
        if self._c_size - self.reader_index > 5:
            b = arr[0]
            result = b & 0x3F  # Mask first 6 bits.
            if (b & 0x40) != 0:  # Bit 7 means another byte, bit 8 is flag bit.
                read_bytes_length += 1
                b = arr[1]
                result |= (b & 0x7F) << 6
                if (b & 0x80) != 0:
                    read_bytes_length += 1
                    b = arr[2]
                    result |= (b & 0x7F) << 13
                    if (b & 0x80) != 0:
                        read_bytes_length += 1
                        b = arr[3]
                        result |= (b & 0x7F) << 20
                        if (b & 0x80) != 0:
                            read_bytes_length += 1
                            b = arr[4]
                            result |= b << 27
            self.reader_index += read_bytes_length
            return result
        else:
            b = self.read_int8()
            result = b & 0x3F  # Mask first 6 bits.
            if (b & 0x40) != 0:
                b = self.read_int8()
                result |= (b & 0x7F) << 6
                if (b & 0x80) != 0:
                    b = self.read_int8()
                    result |= (b & 0x7F) << 13
                    if (b & 0x80) != 0:
                        b = self.read_int8()
                        result |= (b & 0x7F) << 20
                        if (b & 0x80) != 0:
                            b = self.read_int8()
                            result |= b << 27
            return result

    cpdef inline write_varint64(self, int64_t v):
        cdef:
            uint64_t value = v
            int64_t offset = self.writer_index
        self.grow(<int8_t>9)
        cdef uint8_t* arr = self.c_buffer.get().data()
        if value >> 7 == 0:
            arr[offset] = <int8_t>value
            self.writer_index += <int32_t>1
            return 1
        arr[offset] = <int8_t> ((value & 0x7F) | 0x80)
        if value >> 14 == 0:
            arr[offset+1] = <int8_t>(value >> 7)
            self.writer_index += <int32_t>2
            return 2
        arr[offset + 1] = <int8_t> (value >> 7 | 0x80)
        if value >> 21 == 0:
            arr[offset+2] = <int8_t>(value >> 14)
            self.writer_index += <int32_t>3
            return 3
        arr[offset + 2] = <int8_t> (value >> 14 | 0x80)
        if value >> 28 == 0:
            arr[offset+3] = <int8_t>(value >> 21)
            self.writer_index += <int32_t>4
            return 4
        arr[offset + 3] = <int8_t> (value >> 21 | 0x80)
        if value >> 35 == 0:
            arr[offset+4] = <int8_t>(value >> 28)
            self.writer_index += <int32_t>5
            return 5
        arr[offset + 4] = <int8_t> (value >> 28 | 0x80)
        if value >> 42 == 0:
            arr[offset+5] = <int8_t>(value >> 35)
            self.writer_index += <int32_t>6
            return 6
        arr[offset + 5] = <int8_t> (value >> 35 | 0x80)
        if value >> 49 == 0:
            arr[offset+6] = <int8_t>(value >> 42)
            self.writer_index += <int32_t>7
            return 7
        arr[offset + 6] = <int8_t> (value >> 42 | 0x80)
        if value >> 56 == 0:
            arr[offset+7] = <int8_t>(value >> 49)
            self.writer_index += <int32_t>8
            return 8
        arr[offset + 7] = <int8_t> (value >> 49 | 0x80)
        arr[offset + 8] = <int8_t> (value >> 56)
        self.writer_index += <int32_t>9
        return 9

    cpdef inline int64_t read_varint64(self):
        cdef:
            uint32_t read_length = 1
            int64_t b
            int64_t result
            uint32_t position = self.reader_index
            int8_t * arr = <int8_t *> (self.c_buffer.get().data() + position)
        if self._c_size - self.reader_index > 9:
            b = arr[0]
            result = b & 0x7F
            if (b & 0x80) != 0:
                read_length += <int32_t>1
                b = arr[1]
                result |= (b & 0x7F) << 7
                if (b & 0x80) != 0:
                    read_length += <int32_t>1
                    b = arr[2]
                    result |= (b & 0x7F) << 14
                    if (b & 0x80) != 0:
                        read_length += <int32_t>1
                        b = arr[3]
                        result |= (b & 0x7F) << 21
                        if (b & 0x80) != 0:
                            read_length += <int32_t>1
                            b = arr[4]
                            result |= (b & 0x7F) << 28
                            if (b & 0x80) != 0:
                                read_length += <int32_t>1
                                b = arr[5]
                                result |= (b & 0x7F) << 35
                                if (b & 0x80) != 0:
                                    read_length += <int32_t>1
                                    b = arr[6]
                                    result |= (b & 0x7F) << 42
                                    if (b & 0x80) != 0:
                                        read_length += <int32_t>1
                                        b = arr[7]
                                        result |= (b & 0x7F) << 49
                                        if (b & 0x80) != 0:
                                            read_length += <int32_t>1
                                            b = arr[8]
                                            # highest bit in last byte is symbols bit
                                            result |= b << 56
            self.reader_index += read_length
            return result
        else:
            b = self.read_int8()
            result = b & 0x7F
            if (b & 0x80) != 0:
                b = self.read_int8()
                result |= (b & 0x7F) << 7
                if (b & 0x80) != 0:
                    b = self.read_int8()
                    result |= (b & 0x7F) << 14
                    if (b & 0x80) != 0:
                        b = self.read_int8()
                        result |= (b & 0x7F) << 21
                        if (b & 0x80) != 0:
                            b = self.read_int8()
                            result |= (b & 0x7F) << 28
                            if (b & 0x80) != 0:
                                b = self.read_int8()
                                result |= (b & 0x7F) << 35
                                if (b & 0x80) != 0:
                                    b = self.read_int8()
                                    result |= (b & 0x7F) << 42
                                    if (b & 0x80) != 0:
                                        b = self.read_int8()
                                        result |= (b & 0x7F) << 49
                                        if (b & 0x80) != 0:
                                            b = self.read_int8()
                                            # highest bit in last byte is symbols bit
                                            result |= b << 56
            return result

    cdef inline write_c_buffer(self, const uint8_t* value, int32_t length):
        self.write_varint32(length)
        if length <= 0:  # access an emtpy buffer may raise out-of-bound exception.
            return
        self.grow(length)
        self.check_bound(self.writer_index, length)
        self.c_buffer.get().CopyFrom(self.writer_index, value, 0, length)
        self.writer_index += length

    cdef inline int32_t read_c_buffer(self, uint8_t** buf):
        cdef int32_t length = self.read_varint32()
        cdef uint8_t* binary_data = self.c_buffer.get().data()
        self.check_bound(self.reader_index, length)
        buf[0] = binary_data + self.reader_index
        self.reader_index += length
        return length

    cpdef inline write_string(self, str value):
        cdef Py_ssize_t length
        cdef const char * buf = PyUnicode_AsUTF8AndSize(value, &length)
        self.write_c_buffer(<const uint8_t *>buf, length)

    cpdef inline str read_string(self):
        cdef uint8_t* buf
        cdef int32_t length = self.read_c_buffer(&buf)
        str_obj = PyUnicode_DecodeUTF8(<const char *>buf, length, "strict")
        return str_obj

    def __len__(self):
        return self._c_size

    cpdef inline int32_t size(self):
        return self._c_size

    def to_bytes(self, int32_t offset=0, int32_t length=0) -> bytes:
        if length != 0:
            assert 0 < length <= self._c_size,\
                f"length {length} size {self._c_size}"
        else:
            length = self._c_size
        cdef:
            uint8_t* data = self.c_buffer.get().data() + offset
        return data[:length]

    def to_pybytes(self) -> bytes:
        return self.to_bytes()

    def slice(self, offset=0, length=None):
        return type(self)(self, offset, length)

    def __getitem__(self, key):
        if isinstance(key, slice):
            if (key.step or 1) != 1:
                raise IndexError('only slices with step 1 supported')
            return _normalize_slice(self, key)
        return self.getitem(_normalize_index(key, self._c_size))

    cdef getitem(self, int64_t i):
        return self.c_buffer.get().data()[i]

    def hex(self):
        """
        Compute hexadecimal representation of the buffer.

        Returns
        -------
        : bytes
        """
        return self.c_buffer.get().Hex().decode("UTF-8")

    def __getbuffer__(self, Py_buffer *buffer, int flags):
        cdef Py_ssize_t itemsize = 1
        self.shape[0] = self._c_size
        self.stride[0] = itemsize
        buffer.buf = <char *>(self.c_buffer.get().data())
        buffer.format = 'B'
        buffer.internal = NULL                  # see References
        buffer.itemsize = itemsize
        buffer.len = self._c_size  # product(shape) * itemsize
        buffer.ndim = 1
        buffer.obj = self
        buffer.readonly = 0
        buffer.shape = self.shape
        buffer.strides = self.stride
        buffer.suboffsets = NULL                # for pointer arrays only

    def __releasebuffer__(self, Py_buffer *buffer):
        pass

    def __repr__(self):
        return "Buffer(reader_index={}, writer_index={}, size={})".format(
            self.reader_index, self.writer_index, self.size()
        )


cdef inline uint8_t* get_address(v):
    view = memoryview(v)
    cdef str dtype = view.format
    cdef:
        const char[:] signed_char_data
        const unsigned char[:] unsigned_data
        const int16_t[:] signed_short_data
        const int32_t[:] signed_int_data
        const int64_t[:] signed_long_data
        const float[:] signed_float_data
        const double[:] signed_double_data
        uint8_t* ptr
    if dtype == "b":
        signed_char_data = v
        ptr = <uint8_t*>(&signed_char_data[0])
    elif dtype == "B":
        unsigned_data = v
        ptr = <uint8_t*>(&unsigned_data[0])
    elif dtype == "h":
        signed_short_data = v
        ptr = <uint8_t*>(&signed_short_data[0])
    elif dtype == "i":
        signed_int_data = v
        ptr = <uint8_t*>(&signed_int_data[0])
    elif dtype == "l":
        signed_long_data = v
        ptr = <uint8_t*>(&signed_long_data[0])
    elif dtype == "f":
        signed_float_data = v
        ptr = <uint8_t*>(&signed_float_data[0])
    elif dtype == "d":
        signed_double_data = v
        ptr = <uint8_t*>(&signed_double_data[0])
    else:
        raise Exception(f"Unsupported buffer of type {type(v)} and format {dtype}")
    return ptr


def _normalize_slice(Buffer buf, slice key):
    """
    Only support step with 1
    """
    cdef:
        Py_ssize_t start, stop, step
        Py_ssize_t n = len(buf)

    start = key.start or 0
    if start < 0:
        start += n
        if start < 0:
            start = 0
    elif start >= n:
        start = n

    stop = key.stop if key.stop is not None else n
    if stop < 0:
        stop += n
        if stop < 0:
            stop = 0
    elif stop >= n:
        stop = n
    if key.step is not None:
        assert key.step == 1, f"Step should be 1 but got {key.step}"
    length = max(stop - start, 0)
    return buf.slice(start, length)


cdef Py_ssize_t _normalize_index(Py_ssize_t index,
                                 Py_ssize_t length) except -1:
    if index < 0:
        index += length
        if index < 0:
            raise IndexError("index out of bounds")
    elif index >= length:
        raise IndexError("index out of bounds")
    return index


def get_bit(Buffer buffer, uint32_t base_offset, uint32_t index) -> bool:
    return GetBit(buffer.c_buffer.get().data() + base_offset, index)


def set_bit(Buffer buffer, uint32_t base_offset, uint32_t index):
    return SetBit(buffer.c_buffer.get().data() + base_offset, index)


def clear_bit(Buffer buffer, uint32_t base_offset, uint32_t index):
    return ClearBit(buffer.c_buffer.get().data() + base_offset, index)


def set_bit_to(Buffer buffer,
               uint32_t base_offset,
               uint32_t index,
               c_bool bit_is_set):
    return SetBitTo(
        buffer.c_buffer.get().data() + base_offset, index, bit_is_set)
