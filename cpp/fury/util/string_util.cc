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

#include "string_util.h"

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#elif defined(__riscv) && __riscv_vector
#include <riscv_vector.h>
#endif

namespace fury {

#if defined(__x86_64__) || defined(_M_X64)

bool isLatin(const std::string &str) {
  const char *data = str.data();
  size_t len = str.size();

  size_t i = 0;
  __m256i latin_mask = _mm256_set1_epi8(0x80);
  for (; i + 32 <= len; i += 32) {
    __m256i chars =
        _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data + i));
    __m256i result = _mm256_and_si256(chars, latin_mask);
    if (!_mm256_testz_si256(result, result)) {
      return false;
    }
  }

  for (; i < len; ++i) {
    if (static_cast<unsigned char>(data[i]) >= 128) {
      return false;
    }
  }

  return true;
}

#elif defined(__ARM_NEON) || defined(__ARM_NEON__)

bool isLatin(const std::string &str) {
  const char *data = str.data();
  size_t len = str.size();

  size_t i = 0;
  uint8x16_t latin_mask = vdupq_n_u8(0x80);
  for (; i + 16 <= len; i += 16) {
    uint8x16_t chars = vld1q_u8(reinterpret_cast<const uint8_t *>(data + i));
    uint8x16_t result = vandq_u8(chars, latin_mask);
    if (vmaxvq_u8(result) != 0) {
      return false;
    }
  }

  for (; i < len; ++i) {
    if (static_cast<unsigned char>(data[i]) >= 128) {
      return false;
    }
  }

  return true;
}

#elif defined(__riscv) && __riscv_vector

bool isLatin(const std::string &str) {
  const char *data = str.data();
  size_t len = str.size();

  size_t i = 0;
  for (; i + 16 <= len; i += 16) {
    auto chars = vle8_v_u8m1(reinterpret_cast<const uint8_t *>(data + i), 16);
    auto mask = vmv_v_x_u8m1(0x80, 16);
    auto result = vand_vv_u8m1(chars, mask, 16);
    if (vmax_v_u8m1(result, 16) != 0) {
      return false;
    }
  }

  for (; i < len; ++i) {
    if (static_cast<unsigned char>(data[i]) >= 128) {
      return false;
    }
  }

  return true;
}

#else

bool isLatin(const std::string &str) {
  for (char c : str) {
    if (static_cast<unsigned char>(c) >= 128) {
      return false;
    }
  }
  return true;
}

#endif

} // namespace fury
