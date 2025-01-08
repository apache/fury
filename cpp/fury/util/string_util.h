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
#include <string>
// AVX not included here since some older intel cpu doesn't support avx2
// but the built wheel for avx2 is same as sse2.
#if defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#define USE_NEON_SIMD
#elif defined(__SSE2__)
#include <emmintrin.h>
#define USE_SSE2_SIMD
#endif

namespace fury {

static inline bool isAsciiFallback(const char *data, size_t size) {
  size_t i = 0;
  // Loop through 8-byte chunks
  for (; i + 7 < size; i += 8) {
    // Load 8 bytes from the string
    uint64_t chunk = *reinterpret_cast<const uint64_t *>(data + i);
    // Check if any byte in the 64-bit chunk is >= 128
    // This checks if any of the top bits of each byte are set
    if (chunk & 0x8080808080808080ULL) {
      return false;
    }
  }
  for (; i < size; ++i) {
    if (static_cast<unsigned char>(data[i]) >= 128) {
      return false;
    }
  }
  return true;
}

static inline bool hasSurrogatePairFallback(const uint16_t *data, size_t size) {
  for (size_t i = 0; i < size; ++i) {
    auto c = data[i];
    if (c >= 0xD800 && c <= 0xDFFF) {
      return true;
    }
  }
  return false;
}

#if defined(USE_NEON_SIMD)
inline bool isAscii(const char *data, size_t length) {
  size_t i = 0;
  uint8x16_t mostSignificantBit = vdupq_n_u8(0x80);
  for (; i + 15 < length; i += 16) {
    uint8x16_t chunk = vld1q_u8(reinterpret_cast<const uint8_t *>(&data[i]));
    uint8x16_t result = vandq_u8(chunk, mostSignificantBit);
    if (vmaxvq_u8(result) != 0) {
      return false;
    }
  }
  // Check the remaining characters
  return isAsciiFallback(data + i, length - i);
}

inline bool utf16HasSurrogatePairs(const uint16_t *data, size_t length) {
  size_t i = 0;
  uint16x8_t lower_bound = vdupq_n_u16(0xD800);
  uint16x8_t higher_bound = vdupq_n_u16(0xDFFF);
  for (; i + 7 < length; i += 8) {
    uint16x8_t chunk = vld1q_u16(data + i);
    uint16x8_t mask1 = vcgeq_u16(chunk, lower_bound);
    uint16x8_t mask2 = vcleq_u16(chunk, higher_bound);
    if (vmaxvq_u16(mask1 & mask2)) {
      return true; // Detected a high surrogate
    }
  }
  return hasSurrogatePairFallback(data + i, length - i);
}
#elif defined(USE_SSE2_SIMD)
inline bool isAscii(const char *data, size_t length) {
  const __m128i mostSignificantBit = _mm_set1_epi8(static_cast<char>(0x80));
  size_t i = 0;
  for (; i + 15 < length; i += 16) {
    __m128i chunk =
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(&data[i]));
    __m128i result = _mm_and_si128(chunk, mostSignificantBit);
    if (_mm_movemask_epi8(result) != 0) {
      return false;
    }
  }
  // Check the remaining characters
  return isAsciiFallback(data + i, length - i);
}
inline bool utf16HasSurrogatePairs(const uint16_t *data, size_t length) {
  size_t i = 0;
  __m128i lower_bound = _mm_set1_epi16(0xd7ff);
  __m128i higher_bound = _mm_set1_epi16(0xe000);
  for (; i + 7 < length; i += 8) {
    __m128i chunk =
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(data + i));
    __m128i cmp1 = _mm_cmpgt_epi16(chunk, lower_bound);
    __m128i cmp2 = _mm_cmpgt_epi16(higher_bound, chunk);
    if (_mm_movemask_epi8(_mm_and_si128(cmp1, cmp2)) != 0) {
      return true; // Detected a surrogate
    }
  }
  return hasSurrogatePairFallback(data + i, length - i);
}
#else
inline bool isAscii(const char *data, size_t length) {
  return isAsciiFallback(data, length);
}
inline bool utf16HasSurrogatePairs(const uint16_t *data, size_t length) {
  return hasSurrogatePairFallback(data, length);
}
#endif

inline bool isAscii(const std::string &str) {
  return isAscii(str.data(), str.size());
}

inline bool utf16HasSurrogatePairs(const std::u16string &str) {
  // Get the data pointer
  const std::uint16_t *data =
      reinterpret_cast<const std::uint16_t *>(str.data());
  return utf16HasSurrogatePairs(data, str.size());
}

std::string utf16ToUtf8(const std::u16string &utf16, bool is_little_endian);

std::u16string utf8ToUtf16(const std::string &utf8, bool is_little_endian);

} // namespace fury
