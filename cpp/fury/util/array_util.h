#include "fury/util/platform.h"

namespace fury {
#if defined(FURY_HAS_IMMINTRIN)
inline uint16_t getMaxValue(const uint16_t* arr, size_t length) {
  if (length == 0) {
    return 0;  // Return 0 for empty arrays
  }

  __m256i max_val = _mm256_setzero_si256();  // Initialize max vector with zeros

  size_t i = 0;
  for (; i + 16 <= length; i += 16) {
    __m256i current_val = _mm256_loadu_si256((__m256i*)&arr[i]);
    max_val = _mm256_max_epu16(max_val, current_val);  // Max operation
  }

  // Find the max value in the resulting vector
  uint16_t temp[16];
  _mm256_storeu_si256((__m256i*)temp, max_val);
  uint16_t max_avx = temp[0];
  for (int j = 1; j < 16; j++) {
    if (temp[j] > max_avx) {
      max_avx = temp[j];
    }
  }

  // Handle remaining elements
  for (; i < length; i++) {
    if (arr[i] > max_avx) {
      max_avx = arr[i];
    }
  }
  return max_avx;
}

inline void copyValue(const uint16_t* from, uint8_t* to, size_t length) {
  size_t i = 0;
  // Process chunks of 32 bytes (16 uint16_t elements at a time)
  for (; i + 31 < length; i += 32) {
    // Load two 256-bit blocks (32 uint16_t elements total)
    __m256i src1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&from[i]));
    __m256i src2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&from[i + 16]));

    // Narrow the 16-bit integers to 8-bit integers
    __m256i packed = _mm256_packus_epi16(src1, src2);

    // Shuffle the packed result to interleave lower and upper parts
    packed = _mm256_permute4x64_epi64(packed, _MM_SHUFFLE(3, 1, 2, 0));

    // Store the result
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(&to[i]), packed);
  }
  // Check if at least 16 elements are left to process
  if (i + 15 < length) {
    // Process the next 16 elements
    __m256i src1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(&from[i]));
    // Narrow the 16-bit integers to 8-bit integers by zeroing the upper halves
    __m128i packed1 = _mm256_castsi256_si128(src1);       // Lower 128 bits
    __m128i packed2 = _mm256_extracti128_si256(src1, 1);  // Upper 128 bits
    // Pack two 128-wide vectors into 8-bit integers, ignore saturating with itself.
    __m128i packed = _mm_packus_epi16(packed1, packed2);

    // Store the result; using only the first 128 bits
    _mm_storeu_si128(reinterpret_cast<__m128i*>(&to[i]), packed);

    i += 16;
  }
  // Process remaining elements one at a time
  for (; i < length; ++i) {
    to[i] = static_cast<uint8_t>(from[i]);
  }
}
#elif defined(FURY_HAS_NEON)
inline uint16_t getMaxValue(const uint16_t* arr, size_t length) {
  if (length == 0) {
    return 0;  // Return 0 for empty arrays
  }
  uint16x8_t max_val = vdupq_n_u16(0);  // Initialize max vector to zero

  size_t i = 0;
  for (; i + 8 <= length; i += 8) {
    uint16x8_t current_val = vld1q_u16(&arr[i]);
    max_val = vmaxq_u16(max_val, current_val);  // Max operation
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

inline void copyValue(const uint16_t* from, uint8_t* to, size_t length) {
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
inline uint16_t getMaxValue(const uint16_t* arr, size_t length) {
  if (length == 0) {
    return 0;  // Return 0 for empty arrays
  }

  __m128i max_val = _mm_setzero_si128();  // Initialize max vector with zeros

  size_t i = 0;
  for (; i + 8 <= length; i += 8) {
    __m128i current_val = _mm_loadu_si128((__m128i*)&arr[i]);
    max_val = _mm_max_epu16(max_val, current_val);  // Max operation
  }

  // Find the max value in the resulting vector
  uint16_t temp[8];
  _mm_storeu_si128((__m128i*)temp, max_val);
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

inline void copyValue(const uint16_t* from, uint8_t* to, size_t length) {
  size_t i = 0;
  __m128i mask = _mm_set1_epi16(0xFF);  // Mask to zero out the high byte
  for (; i + 7 < length; i += 8) {
    __m128i src = _mm_loadu_si128(reinterpret_cast<const __m128i*>(&from[i]));
    __m128i result = _mm_and_si128(src, mask);
    _mm_storel_epi64(reinterpret_cast<__m128i*>(&to[i]),
                     _mm_packus_epi16(result, result));
  }

  // Fallback for the remainder
  for (; i < length; ++i) {
    to[i] = static_cast<uint8_t>(from[i]);
  }
}
#else
inline uint16_t getMaxValue(const uint16_t* arr, size_t length) {
  if (length == 0) {
    return 0;  // Return 0 for empty arrays
  }
  uint16_t max_val = arr[0];
  for (size_t i = 1; i < length; i++) {
    if (arr[i] > max_val) {
      max_val = arr[i];
    }
  }
  return max_val;
}

inline void copyValue(const uint16_t* from, const uint8_t* to, size_t length) {
  // Fallback for systems without SSE2/NEON
  for (size_t i = 0; i < length; ++i) {
    to[i] = static_cast<uint8_t>(from[i]);
  }
}
#endif
}  // namespace fury
