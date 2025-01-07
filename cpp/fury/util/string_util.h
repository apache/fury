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

bool isLatin(const std::string &str);

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
inline bool utf16HasSurrogatePairs(const uint16_t *data, size_t length) {
  return hasSurrogatePairFallback(data, length);
}
#endif

inline bool utf16HasSurrogatePairs(const std::u16string &str) {
  // Get the data pointer
  const std::uint16_t *data =
      reinterpret_cast<const std::uint16_t *>(str.data());
  return utf16HasSurrogatePairs(data, str.size());
}

std::string utf16ToUtf8(const std::u16string &utf16, bool is_little_endian);

std::u16string utf8ToUtf16(const std::string &utf8, bool is_little_endian);

} // namespace fury
