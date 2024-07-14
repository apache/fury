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

inline void utf16ToUtf8(uint16_t utf16, char *&utf8) {
  if (utf16 < 0x80) {
    *utf8++ = static_cast<char>(utf16);
  } else if (utf16 < 0x800) {
    *utf8++ = static_cast<char>((utf16 >> 6) | 0xC0);
    *utf8++ = static_cast<char>((utf16 & 0x3F) | 0x80);
  } else {
    *utf8++ = static_cast<char>((utf16 >> 12) | 0xE0);
    *utf8++ = static_cast<char>(((utf16 >> 6) & 0x3F) | 0x80);
    *utf8++ = static_cast<char>((utf16 & 0x3F) | 0x80);
  }
}

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

std::string utf16ToUtf8(const std::u16string &utf16, bool is_little_endian) {
  std::string utf8;
  utf8.reserve(utf16.size() *
               3); // Reserve enough space to avoid frequent reallocations

  const __m256i limit1 = _mm256_set1_epi16(0x80);
  const __m256i limit2 = _mm256_set1_epi16(0x800);

  char buffer[48]; // Buffer to hold temporary UTF-8 bytes
  char *output = buffer;

  size_t i = 0;
  size_t n = utf16.size();

  while (i + 16 <= n) {
    __m256i in =
        _mm256_loadu_si256(reinterpret_cast<const __m256i *>(utf16.data() + i));

    if (!is_little_endian) {
      in = _mm256_or_si256(
          _mm256_slli_epi16(in, 8),
          _mm256_srli_epi16(in, 8)); // Swap bytes for big-endian
    }

    __m256i mask1 = _mm256_cmpgt_epi16(in, limit1);
    __m256i mask2 = _mm256_cmpgt_epi16(in, limit2);

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
        utf16ToUtf8(utf16[i + j], output);
      }
    }

    utf8.append(buffer, output - buffer);
    output = buffer; // Reset output buffer pointer
    i += 16;
  }

  // Handle remaining characters
  while (i < n) {
    utf16ToUtf8(utf16[i++], output);
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

  char buffer[48];
  char *output = buffer;
  size_t i = 0;
  size_t n = utf16.size();

  while (i + 8 <= n) {
    uint16x8_t in =
        vld1q_u16(reinterpret_cast<const uint16_t *>(utf16.data() + i));
    if (!is_little_endian) {
      in = vorrq_u16(vshrq_n_u16(in, 8),
                     vshlq_n_u16(in, 8)); // Swap bytes for big-endian
    }

    uint16x8_t mask1 = vcgtq_u16(in, limit1);
    uint16x8_t mask2 = vcgtq_u16(in, limit2);

    if (vminvq_u16(mask1) == 0) {
      // All values < 0x80, 1 byte per character
      for (int j = 0; j < 8; ++j) {
        *output++ = static_cast<char>(vgetq_lane_u16(in, j));
      }
    } else if (vminvq_u16(mask2) == 0) {
      // All values < 0x800, 2 bytes per character
      for (int j = 0; j < 8; ++j) {
        utf16ToUtf8(vgetq_lane_u16(in, j), output);
      }
    } else {
      // Mix of 1, 2, and 3 byte characters
      for (int j = 0; j < 8; ++j) {
        utf16ToUtf8(vgetq_lane_u16(in, j), output);
      }
    }

    utf8.append(buffer, output - buffer);
    output = buffer; // Reset output buffer pointer
    i += 8;
  }

  // Handle remaining characters
  while (i < n) {
    uint16_t code_unit = utf16[i++];
    if (!is_little_endian) {
      code_unit =
          (code_unit >> 8) | (code_unit << 8); // Swap bytes for big-endian
    }
    utf16ToUtf8(code_unit, output);
  }
  utf8.append(buffer, output - buffer);

  return utf8;
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

std::string utf16ToUtf8(const std::u16string &utf16, bool is_little_endian) {
  std::string utf8;
  utf8.reserve(utf16.size() * 3);

  size_t vlmax = vsetvlmax_e16m1();
  vuint16m1_t limit1 = vmv_v_x_u16m1(0x80, vlmax);
  vuint16m1_t limit2 = vmv_v_x_u16m1(0x800, vlmax);

  char buffer[48];
  char *output = buffer;

  size_t i = 0;
  size_t n = utf16.size();

  while (i + vlmax <= n) {
    size_t vl = vsetvl_e16m1(n - i);
    vuint16m1_t in =
        vle16_v_u16m1(reinterpret_cast<const uint16_t *>(utf16.data() + i), vl);
    if (!is_little_endian) {
      in = vor_vv_u16m1(vsrl_vx_u16m1(in, 8, vl), vsll_vx_u16m1(in, 8, vl),
                        vl); // Swap bytes for big-endian
    }

    vbool16_t mask1 = vmseq_vv_u16m1_b16(in, limit1, vl);
    vbool16_t mask2 = vmseq_vv_u16m1_b16(in, limit2, vl);

    if (vfirst_m_b16(mask1) == 0) {
      // All values < 0x80, 1 byte per character
      for (size_t j = 0; j < vl; ++j) {
        *output++ = static_cast<char>(vle16_v_u16m1(
            reinterpret_cast<const uint16_t *>(utf16.data() + i + j), vl)[0]);
      }
    } else if (vfirst_m_b16(mask2) == 0) {
      // All values < 0x800, 2 bytes per character
      for (size_t j = 0; j < vl; ++j) {
        utf16ToUtf8(vle16_v_u16m1(reinterpret_cast<const uint16_t *>(
                                      utf16.data() + i + j),
                                  vl)[0],
                    output);
      }
    } else {
      // Mix of 1, 2, and 3 byte characters
      for (size_t j = 0; j < vl; ++j) {
        utf16ToUtf8(vle16_v_u16m1(reinterpret_cast<const uint16_t *>(
                                      utf16.data() + i + j),
                                  vl)[0],
                    output);
      }
    }

    utf8.append(buffer, output - buffer);
    output = buffer; // Reset output buffer pointer
    i += vl;
  }

  // Handle remaining characters
  while (i < n) {
    uint16_t code_unit = utf16[i++];
    if (!is_little_endian) {
      code_unit =
          (code_unit >> 8) | (code_unit << 8); // Swap bytes for big-endian
    }
    utf16ToUtf8(code_unit, output);
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

  for (size_t i = 0; i < utf16.size(); ++i) {
    uint16_t w1 = utf16[i];
    if (!is_little_endian) {
      w1 = swapBytes(w1);
    }

    if (w1 >= 0xD800 && w1 <= 0xDBFF) {
      if (i + 1 >= utf16.size()) {
        throw std::runtime_error("Invalid UTF-16 sequence");
      }

      uint16_t w2 = utf16[++i];
      if (!is_little_endian) {
        w2 = swapBytes(w2);
      }

      if (w2 < 0xDC00 || w2 > 0xDFFF) {
        throw std::runtime_error("Invalid UTF-16 sequence");
      }

      uint32_t code_point = ((w1 - 0xD800) << 10) + (w2 - 0xDC00) + 0x10000;

      utf8.push_back(0xF0 | (code_point >> 18));
      utf8.push_back(0x80 | ((code_point >> 12) & 0x3F));
      utf8.push_back(0x80 | ((code_point >> 6) & 0x3F));
      utf8.push_back(0x80 | (code_point & 0x3F));
    } else if (w1 >= 0xDC00 && w1 <= 0xDFFF) {
      throw std::runtime_error("Invalid UTF-16 sequence");
    } else {
      if (w1 < 0x80) {
        utf8.push_back(static_cast<char>(w1));
      } else if (w1 < 0x800) {
        utf8.push_back(0xC0 | (w1 >> 6));
        utf8.push_back(0x80 | (w1 & 0x3F));
      } else {
        utf8.push_back(0xE0 | (w1 >> 12));
        utf8.push_back(0x80 | ((w1 >> 6) & 0x3F));
        utf8.push_back(0x80 | (w1 & 0x3F));
      }
    }
  }

  return utf8;
}

#endif

} // namespace fury
