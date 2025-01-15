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
#include "fury/util/platform.h"
#include <cstdint>

namespace fury {
#if defined(FURY_HAS_NEON)
uint16_t getMaxValue(const uint16_t *arr, size_t length) {
  if (length == 0) {
    return 0; // Return 0 for empty arrays
  }
  uint16x8_t max_val = vdupq_n_u16(0); // Initialize max vector to zero

  size_t i = 0;
  for (; i + 8 <= length; i += 8) {
    uint16x8_t current_val = vld1q_u16(&arr[i]);
    max_val = vmaxq_u16(max_val, current_val); // Max operation
  }

  // Find the max value in the resulting vector
  uint16_t temp[8];
  vst1q_u16(temp, max_val);
  uint16_t max_neon = temp[0];
  for (int j = 1; j < 8; j++) {
    if (temp[j] > max_neon) {
      max_neon = temp[j];
    }
  }

  // Handle remaining elements
  for (; i < length; i++) {
    if (arr[i] > max_neon) {
      max_neon = arr[i];
    }
  }
  return max_neon;
}

void copyArray(const uint16_t *from, uint8_t *to, size_t length) {
  size_t i = 0;
  for (; i + 7 < length; i += 8) {
    uint16x8_t src = vld1q_u16(&from[i]);
    uint8x8_t result = vmovn_u16(src);
    vst1_u8(&to[i], result);
  }

  // Fallback for the remainder
  for (; i < length; ++i) {
    to[i] = static_cast<uint8_t>(from[i]);
  }
}
#elif defined(FURY_HAS_SSE2)
uint16_t getMaxValue(const uint16_t *arr, size_t length) {
  if (length == 0) {
    return 0; // Return 0 for empty arrays
  }

  __m128i max_val = _mm_setzero_si128(); // Initialize max vector with zeros

  size_t i = 0;
  for (; i + 8 <= length; i += 8) {
    __m128i current_val = _mm_loadu_si128((__m128i *)&arr[i]);
    max_val = _mm_max_epu16(max_val, current_val); // Max operation
  }

  // Find the max value in the resulting vector
  uint16_t temp[8];
  _mm_storeu_si128((__m128i *)temp, max_val);
  uint16_t max_sse = temp[0];
  for (int j = 1; j < 8; j++) {
    if (temp[j] > max_sse) {
      max_sse = temp[j];
    }
  }

  // Handle remaining elements
  for (; i < length; i++) {
    if (arr[i] > max_sse) {
      max_sse = arr[i];
    }
  }
  return max_sse;
}

void copyArray(const uint16_t *from, uint8_t *to, size_t length) {
  size_t i = 0;
  __m128i mask = _mm_set1_epi16(0xFF); // Mask to zero out the high byte
  for (; i + 7 < length; i += 8) {
    __m128i src = _mm_loadu_si128(reinterpret_cast<const __m128i *>(&from[i]));
    __m128i result = _mm_and_si128(src, mask);
    _mm_storel_epi64(reinterpret_cast<__m128i *>(&to[i]),
                     _mm_packus_epi16(result, result));
  }

  // Fallback for the remainder
  for (; i < length; ++i) {
    to[i] = static_cast<uint8_t>(from[i]);
  }
}
#else
uint16_t getMaxValue(const uint16_t *arr, size_t length) {
  if (length == 0) {
    return 0; // Return 0 for empty arrays
  }
  uint16_t max_val = arr[0];
  for (size_t i = 1; i < length; i++) {
    if (arr[i] > max_val) {
      max_val = arr[i];
    }
  }
  return max_val;
}

void copyArray(const uint16_t *from, uint8_t *to, size_t length) {
  // Fallback for systems without SSE2/NEON
  for (size_t i = 0; i < length; ++i) {
    to[i] = static_cast<uint8_t>(from[i]);
  }
}
#endif
} // namespace fury
