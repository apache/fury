/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <string>

#include "fury/util/bit_util.h"
#include "fury/util/logging.h"

namespace fury {

// A buffer class for storing raw bytes with various methods for reading and
// writing the bytes.
class Buffer {
public:
  Buffer();

  Buffer(uint8_t *data, uint32_t size, bool own_data = true)
      : data_(data), size_(size), own_data_(own_data) {
    writer_index_ = 0;
    reader_index_ = 0;
  }

  Buffer(Buffer &&buffer) noexcept;

  Buffer &operator=(Buffer &&buffer) noexcept;

  virtual ~Buffer();

  /// \brief Return a pointer to the buffer's data
  inline uint8_t *data() const { return data_; }

  /// \brief Return the buffer's size in bytes
  inline uint32_t size() const { return size_; }

  inline bool own_data() const { return own_data_; }

  inline uint32_t writer_index() { return writer_index_; }

  inline uint32_t reader_index() { return reader_index_; }

  inline void WriterIndex(uint32_t writer_index) {
    FURY_CHECK(writer_index < std::numeric_limits<int>::max())
        << "Buffer overflow writer_index" << writer_index_
        << " target writer_index " << writer_index;
    writer_index_ = writer_index;
  }

  inline void IncreaseWriterIndex(uint32_t diff) {
    int64_t writer_index = writer_index_ + diff;
    FURY_CHECK(writer_index < std::numeric_limits<int>::max())
        << "Buffer overflow writer_index" << writer_index_ << " diff " << diff;
    writer_index_ = writer_index;
  }

  inline void ReaderIndex(uint32_t reader_index) {
    FURY_CHECK(reader_index < std::numeric_limits<int>::max())
        << "Buffer overflow reader_index" << reader_index_
        << " target reader_index " << reader_index;
    reader_index_ = reader_index;
  }

  inline void IncreaseReaderIndex(uint32_t diff) {
    int64_t reader_index = reader_index_ + diff;
    FURY_CHECK(reader_index < std::numeric_limits<int>::max())
        << "Buffer overflow reader_index" << reader_index_ << " diff " << diff;
    reader_index_ = reader_index;
  }

  // Unsafe methods don't check bound
  template <typename T> inline void UnsafePut(uint32_t offset, T value) {
    reinterpret_cast<T *>(data_ + offset)[0] = value;
  }

  template <typename T,
            typename = meta::EnableIfIsOneOf<T, int8_t, uint8_t, bool>>
  inline void UnsafePutByte(uint32_t offset, T value) {
    data_[offset] = value;
  }

  inline void UnsafePut(uint32_t offset, const void *data,
                        const uint32_t length) {
    memcpy(data_ + offset, data, (size_t)length);
  }

  template <typename T> inline T Get(uint32_t relative_offset) {
    FURY_CHECK(relative_offset < size_) << "Out of range " << relative_offset
                                        << " should be less than " << size_;
    T value = reinterpret_cast<const T *>(data_ + relative_offset)[0];
    return value;
  }

  template <typename T,
            typename = meta::EnableIfIsOneOf<T, int8_t, uint8_t, bool>>
  inline T GetByteAs(uint32_t relative_offset) {
    FURY_CHECK(relative_offset < size_) << "Out of range " << relative_offset
                                        << " should be less than " << size_;
    return data_[relative_offset];
  }

  inline bool GetBool(uint32_t offset) { return GetByteAs<bool>(offset); }

  inline int8_t GetInt8(uint32_t offset) { return GetByteAs<int8_t>(offset); }

  inline int16_t GetInt16(uint32_t offset) { return Get<int16_t>(offset); }

  inline int32_t GetInt32(uint32_t offset) { return Get<int32_t>(offset); }

  inline int64_t GetInt64(uint32_t offset) { return Get<int64_t>(offset); }

  inline float GetFloat(uint32_t offset) { return Get<float>(offset); }

  inline double GetDouble(uint32_t offset) { return Get<double>(offset); }

  inline uint32_t PutVarUint32(uint32_t offset, int32_t value) {
    if (value >> 7 == 0) {
      data_[offset] = (int8_t)value;
      return 1;
    }
    if (value >> 14 == 0) {
      data_[offset++] = (int8_t)((value & 0x7F) | 0x80);
      data_[offset++] = (int8_t)(value >> 7);
      return 2;
    }
    if (value >> 21 == 0) {
      data_[offset++] = (int8_t)((value & 0x7F) | 0x80);
      data_[offset++] = (int8_t)(value >> 7 | 0x80);
      data_[offset++] = (int8_t)(value >> 14);
      return 3;
    }
    if (value >> 28 == 0) {
      data_[offset++] = (int8_t)((value & 0x7F) | 0x80);
      data_[offset++] = (int8_t)(value >> 7 | 0x80);
      data_[offset++] = (int8_t)(value >> 14 | 0x80);
      data_[offset++] = (int8_t)(value >> 21);
      return 4;
    }
    data_[offset++] = (int8_t)((value & 0x7F) | 0x80);
    data_[offset++] = (int8_t)(value >> 7 | 0x80);
    data_[offset++] = (int8_t)(value >> 14 | 0x80);
    data_[offset++] = (int8_t)(value >> 21 | 0x80);
    data_[offset++] = (int8_t)(value >> 28);
    return 5;
  }

  inline int32_t GetVarUint32(uint32_t offset, uint32_t *readBytesLength) {
    uint32_t position = offset;
    int b = data_[position++];
    int result = b & 0x7F;
    if ((b & 0x80) != 0) {
      b = data_[position++];
      result |= (b & 0x7F) << 7;
      if ((b & 0x80) != 0) {
        b = data_[position++];
        result |= (b & 0x7F) << 14;
        if ((b & 0x80) != 0) {
          b = data_[position++];
          result |= (b & 0x7F) << 21;
          if ((b & 0x80) != 0) {
            b = data_[position++];
            result |= (b & 0x7F) << 28;
          }
        }
      }
    }
    *readBytesLength = position - offset;
    return result;
  }

  /// Return true if both buffers are the same size and contain the same bytes
  /// up to the number of compared bytes
  bool Equals(const Buffer &other, int64_t nbytes) const;

  /// Return true if both buffers are the same size and contain the same bytes
  bool Equals(const Buffer &other) const;

  inline void Grow(uint32_t min_capacity) {
    uint32_t len = writer_index_ + min_capacity;
    if (len > size_) {
      // NOTE: over allocate by 1.5 or 2 ?
      // see: Doubling isn't a great overallocation practice
      // see
      // https://github.com/facebook/folly/blob/master/folly/docs/FBVector.md
      // for discussion.
      auto new_size = util::RoundNumberOfBytesToNearestWord(len * 2);
      Reserve(new_size);
    }
  }

  /// Reserve buffer to new_size
  void Reserve(uint32_t new_size) {
    if (new_size > size_) {
      uint8_t *new_ptr;
      if (own_data_) {
        new_ptr = static_cast<uint8_t *>(
            realloc(data_, static_cast<size_t>(new_size)));
      } else {
        new_ptr = static_cast<uint8_t *>(malloc(static_cast<size_t>(new_size)));
        if (new_ptr) {
          own_data_ = true;
        }
      }
      if (new_ptr) {
        data_ = new_ptr;
        size_ = new_size;
      } else {
        FURY_CHECK(false) << "Out of memory when grow buffer, needed_size "
                          << size_;
      }
    }
  }

  /// Copy a section of the buffer into a new Buffer.
  void Copy(uint32_t start, uint32_t nbytes,
            std::shared_ptr<Buffer> &out) const;

  /// Copy a section of the buffer into a new Buffer.
  void Copy(uint32_t start, uint32_t nbytes, Buffer &out) const;

  /// Copy a section of the buffer.
  void Copy(uint32_t start, uint32_t nbytes, uint8_t *out) const;

  /// Copy a section of the buffer.
  void Copy(uint32_t start, uint32_t nbytes, uint8_t *out,
            uint32_t offset) const;

  /// Copy data from `src` yo buffer
  void CopyFrom(uint32_t offset, const uint8_t *src, uint32_t src_offset,
                uint32_t nbytes);

  /// Zero all bytes in padding
  void ZeroPadding() {
    // A zero-size buffer_ can have a null data pointer
    if (size_ != 0) {
      memset(data_, 0, static_cast<size_t>(size_));
    }
  }

  /// \brief Copy buffer contents into a new std::string
  /// \return std::string
  /// \note Can throw std::bad_alloc if buffer is large
  std::string ToString() const;

  std::string Hex() const;

private:
  uint8_t *data_;
  uint32_t size_;
  bool own_data_;
  uint32_t writer_index_;
  uint32_t reader_index_;
};

/// \brief Allocate a fixed-size mutable buffer from the default memory pool
///
/// \param[in] size size of buffer to allocate
/// \param[out] out the allocated buffer (contains padding)
///
/// \return success or not
bool AllocateBuffer(uint32_t size, std::shared_ptr<Buffer> *out);

bool AllocateBuffer(uint32_t size, Buffer **out);

Buffer *AllocateBuffer(uint32_t size);

} // namespace fury
