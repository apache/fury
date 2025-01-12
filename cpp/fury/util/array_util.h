#include "fury/util/platform.h"

namespace fury {
#if defined(FURY_HAS_NEON)
inline uint16_t getMaxValue(uint16_t* arr, size_t length) {
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

#elif defined(FURY_HAS_SSE2)

inline uint16_t getMaxValue(uint16_t* arr, size_t length) {
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
#else
inline uint16_t getMaxValue(uint16_t* arr, size_t length) {
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
#endif
}  // namespace fury
