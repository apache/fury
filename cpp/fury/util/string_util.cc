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

#include <chrono>
#include <string>

namespace fury {

// Swap bytes to convert from big endian to little endian
    inline uint16_t swapBytes(uint16_t value) {
        return (value >> 8) | (value << 8);
    }

    inline void utf16ToUtf8(uint16_t code_unit, char*& output) {
        if (code_unit < 0x80) {
            *output++ = static_cast<char>(code_unit);
        } else if (code_unit < 0x800) {
            *output++ = static_cast<char>(0xC0 | (code_unit >> 6));
            *output++ = static_cast<char>(0x80 | (code_unit & 0x3F));
        } else {
            *output++ = static_cast<char>(0xE0 | (code_unit >> 12));
            *output++ = static_cast<char>(0x80 | ((code_unit >> 6) & 0x3F));
            *output++ = static_cast<char>(0x80 | (code_unit & 0x3F));
        }
    }

    inline void utf16SurrogatePairToUtf8(uint16_t high, uint16_t low, char *&utf8) {
        uint32_t code_point = 0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00);
        *utf8++ = static_cast<char>((code_point >> 18) | 0xF0);
        *utf8++ = static_cast<char>(((code_point >> 12) & 0x3F) | 0x80);
        *utf8++ = static_cast<char>(((code_point >> 6) & 0x3F) | 0x80);
        *utf8++ = static_cast<char>((code_point & 0x3F) | 0x80);
    }

#if defined(__x86_64__) || defined(_M_X64)

    bool isLatin(const std::string &str) {
        const char *data = str.data();
        size_t len = str.size();

        size_t i = 0;
        __m256i latin_mask = _mm256_set1_epi8(0x80);
        for (; i + 32 <= len; i += 32) {
            __m256i chars = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(data + i));
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

    std::string utf16ToUtf8(const std::u16string &utf16, bool is_little_endian) {
        std::string utf8;
        utf8.reserve(utf16.size() * 3); // Reserve enough space to avoid frequent reallocations

        const __m256i limit1 = _mm256_set1_epi16(0x80);
        const __m256i limit2 = _mm256_set1_epi16(0x800);
        const __m256i surrogate_high_start = _mm256_set1_epi16(0xD800);
        const __m256i surrogate_high_end = _mm256_set1_epi16(0xDBFF);
        const __m256i surrogate_low_start = _mm256_set1_epi16(0xDC00);
        const __m256i surrogate_low_end = _mm256_set1_epi16(0xDFFF);

        char buffer[64]; // Buffer to hold temporary UTF-8 bytes
        char *output = buffer;

        size_t i = 0;
        size_t n = utf16.size();

        while (i + 16 <= n) {
            __m256i in = _mm256_loadu_si256(reinterpret_cast<const __m256i *>(utf16.data() + i));

            if (!is_little_endian) {
                in = _mm256_or_si256(_mm256_slli_epi16(in, 8), _mm256_srli_epi16(in, 8)); // Swap bytes for big-endian
            }

            __m256i mask1 = _mm256_cmpgt_epi16(in, limit1);
            __m256i mask2 = _mm256_cmpgt_epi16(in, limit2);
            __m256i high_surrogate_mask = _mm256_and_si256(_mm256_cmpgt_epi16(in, surrogate_high_start), _mm256_cmpgt_epi16(in, surrogate_high_end));
            __m256i low_surrogate_mask = _mm256_and_si256(_mm256_cmpgt_epi16(in, surrogate_low_start), _mm256_cmpgt_epi16(in, surrogate_low_end));

            if (_mm256_testz_si256(mask1, mask1)) {
                // All values < 0x80, 1 byte per character
                for (int j = 0; j < 16; ++j) {
                    *output++ = static_cast<char>(utf16[i + j]);
                }
            } else if (_mm256_testz_si256(mask2, mask2)) {
                // All values < 0x800, 2 bytes per character
                for (int j = 0; j < 16; ++j) {
                    utf16ToUtf8(utf16[i + j], output);
                }
            } else {
                // Mix of 1, 2, and 3 byte characters
                for (int j = 0; j < 16; ++j) {
                    if (_mm256_testz_si256(high_surrogate_mask, high_surrogate_mask) && j + 1 < 16 && !_mm256_testz_si256(low_surrogate_mask, low_surrogate_mask)) {
                        // Surrogate pair
                        utf16SurrogatePairToUtf8(utf16[i + j], utf16[i + j + 1], output);
                        ++j;
                    } else {
                        utf16ToUtf8(utf16[i + j], output);
                    }
                }
            }

            utf8.append(buffer, output - buffer);
            output = buffer; // Reset output buffer pointer
            i += 16;
        }

        // Handle remaining characters
        while (i < n) {
            if (i + 1 < n && utf16[i] >= 0xD800 && utf16[i] <= 0xDBFF && utf16[i + 1] >= 0xDC00 && utf16[i + 1] <= 0xDFFF) {
                // Surrogate pair
                utf16SurrogatePairToUtf8(utf16[i], utf16[i + 1], output);
                ++i;
            } else {
                utf16ToUtf8(utf16[i], output);
            }
            ++i;
        }
        utf8.append(buffer, output - buffer);

        return utf8;
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

std::string utf16ToUtf8(const std::u16string &utf16, bool is_little_endian) {
  std::string utf8;
  utf8.reserve(utf16.size() * 3);

  uint16x8_t limit1 = vdupq_n_u16(0x80);
  uint16x8_t limit2 = vdupq_n_u16(0x800);
  uint16x8_t surrogate_high_start = vdupq_n_u16(0xD800);
  uint16x8_t surrogate_high_end = vdupq_n_u16(0xDBFF);
  uint16x8_t surrogate_low_start = vdupq_n_u16(0xDC00);
  uint16x8_t surrogate_low_end = vdupq_n_u16(0xDFFF);

  char buffer[64];
  char *output = buffer;
  size_t i = 0;
  size_t n = utf16.size();

  while (i + 8 <= n) {
    uint16x8_t in = vld1q_u16(reinterpret_cast<const uint16_t *>(utf16.data() + i));
    if (!is_little_endian) {
      in = vorrq_u16(vshlq_n_u16(in, 8), vshrq_n_u16(in, 8)); // Swap bytes for big-endian
    }

    uint16x8_t mask1 = vcgtq_u16(in, limit1);
    uint16x8_t mask2 = vcgtq_u16(in, limit2);
    uint16x8_t high_surrogate_mask = vandq_u16(vcgtq_u16(in, surrogate_high_start), vcltq_u16(in, surrogate_high_end));
    uint16x8_t low_surrogate_mask = vandq_u16(vcgtq_u16(in, surrogate_low_start), vcltq_u16(in, surrogate_low_end));

    if (vmaxvq_u16(mask1) == 0) {
      for (int j = 0; j < 8; ++j) {
        *output++ = static_cast<char>(utf16[i + j]);
      }
    } else if (vmaxvq_u16(mask2) == 0) {
      for (int j = 0; j < 8; ++j) {
        utf16ToUtf8(utf16[i + j], output);
      }
    } else {
      for (int j = 0; j < 8; ++j) {
        if (vmaxvq_u16(high_surrogate_mask) == 0 && j + 1 < 8 && vmaxvq_u16(low_surrogate_mask) != 0) {
          utf16SurrogatePairToUtf8(utf16[i + j], utf16[i + j + 1], output);
          ++j;
        } else {
          utf16ToUtf8(utf16[i + j], output);
        }
      }
    }

    utf8.append(buffer, output - buffer);
    output = buffer;
    i += 8;
  }

  while (i < n) {
    if (i + 1 < n && utf16[i] >= 0xD800 && utf16[i] <= 0xDBFF && utf16[i + 1] >= 0xDC00 && utf16[i + 1] <= 0xDFFF) {
      utf16SurrogatePairToUtf8(utf16[i], utf16[i + 1], output);
      ++i;
    } else {
      utf16ToUtf8(utf16[i], output);
    }
    ++i;
  }
  utf8.append(buffer, output - buffer);

  return utf8;
}

#elif defined(__riscv) && __riscv_vector

bool isLatin(const std::string &str) {
  const char *data = str.data();
  size_t len = str.size();

  size_t i = 0;
  auto latin_mask = vmv_v_x_u8m1(0x80, 16);
  for (; i + 16 <= len; i += 16) {
    auto chars = vle8_v_u8m1(reinterpret_cast<const uint8_t *>(data + i), 16);
    auto result = vand_vv_u8m1(chars, latin_mask, 16);
    if (vfirst_m_b8(vmsne_vx_u8m1_b8(result, 0, 16))) {
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

std::string utf16ToUtf8(const std::u16string &utf16, bool is_little_endian) {
  std::string utf8;
  utf8.reserve(utf16.size() * 3);

  auto limit1 = vmv_v_x_u16m1(0x80, 8);
  auto limit2 = vmv_v_x_u16m1(0x800, 8);
  auto surrogate_high_start = vmv_v_x_u16m1(0xD800, 8);
  auto surrogate_high_end = vmv_v_x_u16m1(0xDBFF, 8);
  auto surrogate_low_start = vmv_v_x_u16m1(0xDC00, 8);
  auto surrogate_low_end = vmv_v_x_u16m1(0xDFFF, 8);

  char buffer[48];
  char *output = buffer;
  size_t i = 0;
  size_t n = utf16.size();

  while (i + 8 <= n) {
    auto in = vle16_v_u16m1(reinterpret_cast<const uint16_t *>(utf16.data() + i), 8);
    if (!is_little_endian) {
      in = vor_vv_u16m1(vsrl_vx_u16m1(in, 8, 8), vsll_vx_u16m1(in, 8, 8), 8);
    }

    auto mask1 = vmsgt_vx_u16m1(in, 0x80, 8);
    auto mask2 = vmsgt_vx_u16m1(in, 0x800, 8);
    auto high_surrogate_mask = vmand_vv_u16m1(vmsgt_vx_u16m1(in, 0xD800, 8), vmslt_vx_u16m1(in, 0xDBFF, 8), 8);
    auto low_surrogate_mask = vmand_vv_u16m1(vmsgt_vx_u16m1(in, 0xDC00, 8), vmslt_vx_u16m1(in, 0xDFFF, 8), 8);

    if (vmslt_vx_u16m1(mask1, 0, 8)) {
      for (int j = 0; j < 8; ++j) {
        *output++ = static_cast<char>(vget_vx_u16m1(in, j));
      }
    } else if (vmslt_vx_u16m1(mask2, 0, 8)) {
      for (int j = 0; j < 8; ++j) {
        utf16ToUtf8(vget_vx_u16m1(in, j), output);
      }
    } else {
      for (int j = 0; j < 8; ++j) {
        if (vfirst_m_b8(vmand_vv_b8(high_surrogate_mask, vmsne_vx_u8m1_b8(vmv_v_x_u8m1(0, 8), 0, 8))) && j + 1 < 8 && vfirst_m_b8(vmand_vv_b8(low_surrogate_mask, vmsne_vx_u8m1_b8(vmv_v_x_u8m1(0, 8), 0, 8)))) {
          utf16SurrogatePairToUtf8(vget_vx_u16m1(in, j), vget_vx_u16m1(in, j + 1), output);
          ++j;
        } else {
          utf16ToUtf8(vget_vx_u16m1(in, j), output);
        }
      }
    }

    utf8.append(buffer, output - buffer);
    output = buffer;
    i += 8;
  }

  while (i < n) {
    if (i + 1 < n && utf16[i] >= 0xD800 && utf16[i] <= 0xDBFF && utf16[i + 1] >= 0xDC00 && utf16[i + 1] <= 0xDFFF) {
      utf16SurrogatePairToUtf8(utf16[i], utf16[i + 1], output);
      ++i;
    } else {
      utf16ToUtf8(utf16[i], output);
    }
    ++i;
  }
  utf8.append(buffer, output - buffer);

  return utf8;
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

// Fallback implementation without SIMD acceleration
std::string utf16ToUtf8(const std::u16string &utf16, bool is_little_endian) {
    std::string utf8;
    utf8.reserve(utf16.size() * 3); // Reserve enough space to avoid frequent reallocations

    size_t i = 0;
    size_t n = utf16.size();
    char buffer[4]; // Buffer to hold temporary UTF-8 bytes
    char *output = buffer;

    while (i < n) {
        uint16_t code_unit = utf16[i];
        if (!is_little_endian) {
            code_unit = swapBytes(code_unit);
        }
        if (i + 1 < n && code_unit >= 0xD800 && code_unit <= 0xDBFF && utf16[i + 1] >= 0xDC00 && utf16[i + 1] <= 0xDFFF) {
            // Surrogate pair
            uint16_t high = code_unit;
            uint16_t low = utf16[i + 1];
            if (!is_little_endian) {
                low = swapBytes(low);
            }
            utf16SurrogatePairToUtf8(high, low, output);
            utf8.append(buffer, output - buffer);
            output = buffer;
            ++i;
        } else {
            utf16ToUtf8(code_unit, output);
            utf8.append(buffer, output - buffer);
            output = buffer;
        }
        ++i;
    }
    return utf8;
}



#endif

} // namespace fury

