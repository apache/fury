#pragma once
#include <chrono>
#include <cstdint>
#include <ctime>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

#ifdef _WIN32
#define ROW_LITTLE_ENDIAN 1
#else
#ifdef __APPLE__

#include <machine/endian.h>

#else
#include <endian.h>
#endif
#

#ifndef __BYTE_ORDER__
#error "__BYTE_ORDER__ not defined"
#endif
#

#ifndef __ORDER_LITTLE_ENDIAN__
#error "__ORDER_LITTLE_ENDIAN__ not defined"
#endif
#

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define ROW_LITTLE_ENDIAN 1
#else
#define ROW_LITTLE_ENDIAN 0
#endif
#endif
#if defined(_MSC_VER)
#include <intrin.h>
#pragma intrinsic(_BitScanReverse)
#pragma intrinsic(_BitScanForward)
#define ROW_BYTE_SWAP64 _byteswap_uint64
#define ROW_BYTE_SWAP32 _byteswap_ulong
#else
#define ROW_BYTE_SWAP64 __builtin_bswap64
#define ROW_BYTE_SWAP32 __builtin_bswap32
#endif

namespace fury {

/// \brief Metafunction to allow checking if a type matches any of another set
/// of types
template <typename...>
struct IsOneOf : std::false_type {}; /// Base case: nothing has matched

template <typename T, typename U, typename... Args>
struct IsOneOf<T, U, Args...> {
  /// Recursive case: T == U or T matches any other types provided (not
  /// including U).
  static constexpr bool value =
      std::is_same<T, U>::value || IsOneOf<T, Args...>::value;
};

/// \brief Shorthand for using IsOneOf + std::enable_if
template <typename T, typename... Args>
using EnableIfIsOneOf =
    typename std::enable_if<IsOneOf<T, Args...>::value, T>::type;

namespace BitUtil {

//
// Byte-swap 16-bit, 32-bit and 64-bit values. based on arrow/util/bit-util.h
//

// Swap the byte order (i.e. endianess)
static inline int64_t ByteSwap(int64_t value) { return ROW_BYTE_SWAP64(value); }

static inline uint64_t ByteSwap(uint64_t value) {
  return static_cast<uint64_t>(ROW_BYTE_SWAP64(value));
}

static inline int32_t ByteSwap(int32_t value) { return ROW_BYTE_SWAP32(value); }

static inline uint32_t ByteSwap(uint32_t value) {
  return static_cast<uint32_t>(ROW_BYTE_SWAP32(value));
}

static inline int16_t ByteSwap(int16_t value) {
  constexpr auto m = static_cast<int16_t>(0xff);
  return static_cast<int16_t>(((value >> 8) & m) | ((value & m) << 8));
}

static inline uint16_t ByteSwap(uint16_t value) {
  return static_cast<uint16_t>(ByteSwap(static_cast<int16_t>(value)));
}

static inline float ByteSwap(float value) {
  auto *ptr = reinterpret_cast<uint32_t *>(&value);
  uint32_t i = ROW_BYTE_SWAP32(*ptr);
  auto *f = reinterpret_cast<float *>(&i);
  return *(f);
}

static inline double ByteSwap(double value) {
  auto *ptr = reinterpret_cast<uint64_t *>(&value);
  uint64_t i = ROW_BYTE_SWAP64(*ptr);
  auto *d = reinterpret_cast<double *>(&i);
  return *d;
}

// WriteString the swapped bytes into dst. Src and dst cannot overlap.
static inline void ByteSwap(void *dst, const void *src, int len) {
  switch (len) {
  case 1:
    *reinterpret_cast<int8_t *>(dst) = *reinterpret_cast<const int8_t *>(src);
    return;
  case 2:
    *reinterpret_cast<int16_t *>(dst) =
        ByteSwap(*reinterpret_cast<const int16_t *>(src));
    return;
  case 4:
    *reinterpret_cast<int32_t *>(dst) =
        ByteSwap(*reinterpret_cast<const int32_t *>(src));
    return;
  case 8:
    *reinterpret_cast<int64_t *>(dst) =
        ByteSwap(*reinterpret_cast<const int64_t *>(src));
    return;
  default:
    break;
  }

  auto d = reinterpret_cast<uint8_t *>(dst);
  auto s = reinterpret_cast<const uint8_t *>(src);
  for (int i = 0; i < len; ++i) {
    d[i] = s[len - i - 1];
  }
}

// Convert to little/big endian format from the machine's native endian format.
#if ROW_LITTLE_ENDIAN

template <typename T,
          typename = EnableIfIsOneOf<T, int64_t, uint64_t, int32_t, uint32_t,
                                     int16_t, uint16_t, float, double>>
static inline T ToBigEndian(T value) {
  return ByteSwap(value);
}

template <typename T,
          typename = EnableIfIsOneOf<T, int64_t, uint64_t, int32_t, uint32_t,
                                     int16_t, uint16_t, float, double>>
static inline T ToLittleEndian(T value) {
  return value;
}

#else
template <typename T,
          typename = EnableIfIsOneOf<T, int64_t, uint64_t, int32_t, uint32_t,
                                     int16_t, uint16_t, float, double>>
static inline T ToBigEndian(T value) {
  return value;
}

template <typename T,
          typename = EnableIfIsOneOf<T, int64_t, uint64_t, int32_t, uint32_t,
                                     int16_t, uint16_t, float, double>>
static inline T ToLittleEndian(T value) {
  return ByteSwap(value);
}
#endif

// Convert from big/little endian format to the machine's native endian format.
#if ROW_LITTLE_ENDIAN

template <typename T,
          typename = EnableIfIsOneOf<T, int64_t, uint64_t, int32_t, uint32_t,
                                     int16_t, uint16_t, float, double>>
static inline T FromBigEndian(T value) {
  return ByteSwap(value);
}

template <typename T,
          typename = EnableIfIsOneOf<T, int64_t, uint64_t, int32_t, uint32_t,
                                     int16_t, uint16_t, float, double>>
static inline T FromLittleEndian(T value) {
  return value;
}

#else
template <typename T,
          typename = EnableIfIsOneOf<T, int64_t, uint64_t, int32_t, uint32_t,
                                     int16_t, uint16_t, float, double>>
static inline T FromBigEndian(T value) {
  return value;
}

template <typename T,
          typename = EnableIfIsOneOf<T, int64_t, uint64_t, int32_t, uint32_t,
                                     int16_t, uint16_t, float, double>>
static inline T FromLittleEndian(T value) {
  return ByteSwap(value);
}
#endif

// Bitmask selecting the k-th bit in a byte
static constexpr uint8_t kBitmask[] = {1, 2, 4, 8, 16, 32, 64, 128};

// the bitwise complement version of kBitmask
static constexpr uint8_t kFlippedBitmask[] = {254, 253, 251, 247,
                                              239, 223, 191, 127};

constexpr bool IsMultipleOf64(int64_t n) { return (n & 63) == 0; }

constexpr bool IsMultipleOf8(int64_t n) { return (n & 7) == 0; }

static inline bool GetBit(const uint8_t *bits, uint32_t i) {
  return static_cast<bool>((bits[i >> 3] >> (i & 0x07)) & 1);
}

static inline void ClearBit(uint8_t *bits, int64_t i) {
  bits[i / 8] &= kFlippedBitmask[i % 8];
}

static inline void SetBit(uint8_t *bits, int64_t i) {
  bits[i / 8] |= kBitmask[i % 8];
}

static inline void SetBitTo(uint8_t *bits, int64_t i, bool bit_is_set) {
  // https://graphics.stanford.edu/~seander/bithacks.html
  // "Conditionally set or clear bits without branching"
  // NOTE: this seems to confuse Valgrind as it reads from potentially
  // uninitialized memory
  bits[i / 8] ^=
      static_cast<uint8_t>(-static_cast<uint8_t>(bit_is_set) ^ bits[i / 8]) &
      kBitmask[i % 8];
}

static inline int RoundNumberOfBytesToNearestWord(int num_bytes) {
  int remainder = num_bytes & 0x07;
  if (remainder == 0) {
    return num_bytes;
  } else {
    return num_bytes + (8 - remainder);
  }
}

static inline std::string hex(uint8_t *data, int32_t length) {
  constexpr char hex[] = "0123456789abcdef";
  std::string result;
  for (int i = 0; i < length; i++) {
    uint8_t val = data[i];
    result.push_back(hex[val >> 4]);
    result.push_back(hex[val & 0xf]);
  }
  return result;
}
} // namespace BitUtil

std::string FormatTimePoint(std::chrono::system_clock::time_point tp);

} // namespace fury
