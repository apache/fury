from libc.stdint cimport *
from libcpp cimport bool as c_bool
from libcpp.memory cimport shared_ptr
from libcpp.string cimport string as c_string

cdef extern from "fury/util/buffer.h" namespace "fury" nogil:
    cdef cppclass CBuffer" fury::Buffer":
        CBuffer(uint8_t* data, uint32_t size, c_bool own_data=True)

        inline uint8_t* data()

        inline uint32_t size()

        inline c_bool own_data()

        inline c_bool Reserve(uint32_t new_size)

        inline void UnsafePutByte(uint32_t offset, c_bool)

        inline void UnsafePutByte(uint32_t offset, int8_t)

        inline void UnsafePut(uint32_t offset, int16_t)

        inline void UnsafePut(uint32_t offset, int32_t)

        inline void UnsafePut(uint32_t offset, int64_t)

        inline void UnsafePut(uint32_t offset, float)

        inline void UnsafePut(uint32_t offset, double)

        void CopyFrom(uint32_t offset, const uint8_t *src, uint32_t src_offset,
                      uint32_t nbytes)

        inline c_bool GetBool(uint32_t offset)

        inline int8_t GetInt8(uint32_t offset)

        inline int16_t GetInt16(uint32_t offset)

        inline int32_t GetInt32(uint32_t offset)

        inline int64_t GetInt64(uint32_t offset)

        inline float GetFloat(uint32_t offset)

        inline double GetDouble(uint32_t offset)

        inline uint32_t PutPositiveVarInt32(uint32_t offset, int32_t value)

        inline int32_t GetPositiveVarInt32(uint32_t offset, uint32_t *readBytesLength)

        void Copy(uint32_t start, uint32_t nbytes,
                  uint8_t* out, uint32_t offset) const

        c_string Hex()

    CBuffer* AllocateBuffer(uint32_t size)
    c_bool AllocateBuffer(uint32_t size, shared_ptr[CBuffer]* out)


cdef extern from "fury/util/util.h" namespace "fury::BitUtil" nogil:
    c_bool GetBit(const uint8_t *bits, uint32_t i)

    void SetBit(uint8_t *bits, int64_t i)

    void ClearBit(uint8_t *bits, int64_t i)

    void SetBitTo(uint8_t *bits, int64_t i, c_bool bit_is_set)

    c_string hex(uint8_t *data, int32_t length)
