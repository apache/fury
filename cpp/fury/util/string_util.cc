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
#include <iostream>
#include <random>

#if defined(__x86_64__) || defined(_M_X64)
#include <emmintrin.h>
#include <immintrin.h>
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
#include <arm_neon.h>
#elif defined(__riscv) && __riscv_vector
#include <riscv_vector.h>
#endif

namespace fury {

bool isLatin_Baseline(const std::string &str) {
  for (char c : str) {
    if (static_cast<unsigned char>(c) >= 128) {
      return false;
    }
  }
  return true;
}

#if defined(__x86_64__) || defined(_M_X64)
bool isLatin_AVX2(const std::string &str) {
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

bool isLatin_SSE2(const std::string &str) {
  const char *data = str.data();
  size_t len = str.size();

  size_t i = 0;
  __m128i latin_mask = _mm_set1_epi8(0x80);
  for (; i + 16 <= len; i += 16) {
    __m128i chars =
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(data + i));
    __m128i result = _mm_and_si128(chars, latin_mask);
    if (!_mm_testz_si128(result, result)) {
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
bool isLatin_AVX2(const std::string &str) { return isLatin_Baseline(str); }

bool isLatin_SSE2(const std::string &str) { return isLatin_Baseline(str); }
#endif

#if defined(__ARM_NEON) || defined(__ARM_NEON__)
bool isLatin_NEON(const std::string &str) {
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
#else
bool isLatin_NEON(const std::string &str) { return isLatin_Baseline(str); }
#endif

#if defined(__riscv) && __riscv_vector
bool isLatin_RISCV(const std::string &str) {
  const char *data = str.data();
  size_t len = str.size();

  size_t i = 0;
  size_t vl;
  while ((vl = vsetvl_e8m1(len - i)) > 0) {
    vuint8m1_t chars =
        vle8_v_u8m1(reinterpret_cast<const uint8_t *>(data + i), vl);
    vuint8m1_t latin_mask = vmv_v_x_u8m1(0x80, vl);
    vbool8_t result =
        vmseq_vv_u8m1_b8(vand_vv_u8m1(chars, latin_mask, vl), latin_mask, vl);
    if (vfirst_m_b8(result) != -1) {
      return false;
    }
    i += vl;
  }

  for (; i < len; ++i) {
    if (static_cast<unsigned char>(data[i]) >= 128) {
      return false;
    }
  }

  return true;
}
#else
bool isLatin_RISCV(const std::string &str) { return isLatin_Baseline(str); }
#endif

std::string generateRandomString(size_t length) {
  const char charset[] =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
  std::default_random_engine rng(std::random_device{}());
  std::uniform_int_distribution<> dist(0, sizeof(charset) - 2);

  std::string result;
  result.reserve(length);
  for (size_t i = 0; i < length; ++i) {
    result += charset[dist(rng)];
  }

  return result;
}

} // namespace fury
