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

#include <chrono>
#include <codecvt>
#include <iostream>
#include <locale>
#include <random>

#include "fury/util/logging.h"
#include "string_util.h"
#include "gtest/gtest.h"

namespace fury {

// Function to generate a random string
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

bool isLatin_BaseLine(const std::string &str) {
  for (char c : str) {
    if (static_cast<unsigned char>(c) >= 128) {
      return false;
    }
  }
  return true;
}

TEST(StringUtilTest, TestIsLatinFunctions) {
  std::string testStr = generateRandomString(100000);
  auto start_time = std::chrono::high_resolution_clock::now();
  bool result = isLatin_BaseLine(testStr);
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                      end_time - start_time)
                      .count();
  FURY_LOG(INFO) << "BaseLine Running Time: " << duration << " ns.";

  start_time = std::chrono::high_resolution_clock::now();
  result = isLatin(testStr);
  end_time = std::chrono::high_resolution_clock::now();
  duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time -
                                                                  start_time)
                 .count();
  FURY_LOG(INFO) << "Optimized Running Time: " << duration << " ns.";

  EXPECT_TRUE(result);
}

TEST(StringUtilTest, TestIsLatinLogic) {
  // Test strings with only Latin characters
  EXPECT_TRUE(isLatin("Fury"));
  EXPECT_TRUE(isLatin(generateRandomString(80)));

  // Test unaligned strings with only Latin characters
  EXPECT_TRUE(isLatin(generateRandomString(80) + "1"));
  EXPECT_TRUE(isLatin(generateRandomString(80) + "12"));
  EXPECT_TRUE(isLatin(generateRandomString(80) + "123"));

  // Test strings with non-Latin characters
  EXPECT_FALSE(isLatin("你好, Fury"));
  EXPECT_FALSE(isLatin(generateRandomString(80) + "你好"));
  EXPECT_FALSE(isLatin(generateRandomString(80) + "1你好"));
  EXPECT_FALSE(isLatin(generateRandomString(11) + "你"));
  EXPECT_FALSE(isLatin(generateRandomString(10) + "你好"));
  EXPECT_FALSE(isLatin(generateRandomString(9) + "性能好"));
  EXPECT_FALSE(isLatin("\u1234"));
  EXPECT_FALSE(isLatin("a\u1234"));
  EXPECT_FALSE(isLatin("ab\u1234"));
  EXPECT_FALSE(isLatin("abc\u1234"));
  EXPECT_FALSE(isLatin("abcd\u1234"));
  EXPECT_FALSE(isLatin("Javaone Keynote\u1234"));
}

// Generate random UTF-16 string ensuring valid surrogate pairs
std::u16string generateRandomUTF16String(size_t length) {
  std::u16string str;
  std::mt19937 generator(std::random_device{}());
  std::uniform_int_distribution<uint32_t> distribution(0, 0x10FFFF);

  while (str.size() < length) {
    uint32_t code_point = distribution(generator);

    if (code_point <= 0xD7FF ||
        (code_point >= 0xE000 && code_point <= 0xFFFF)) {
      str.push_back(static_cast<char16_t>(code_point));
    } else if (code_point >= 0x10000 && code_point <= 0x10FFFF) {
      code_point -= 0x10000;
      str.push_back(static_cast<char16_t>((code_point >> 10) + 0xD800));
      str.push_back(static_cast<char16_t>((code_point & 0x3FF) + 0xDC00));
    }
  }

  return str;
}

// Basic implementation

// Swap bytes to convert from big endian to little endian
inline uint16_t swapBytes(uint16_t value) {
  return (value >> 8) | (value << 8);
}

inline void utf16ToUtf8(uint16_t code_unit, char *&output) {
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

std::string utf16ToUtf8BaseLine(const std::u16string &utf16,
                                bool is_little_endian) {
  std::string utf8;
  utf8.reserve(utf16.size() *
               3); // Reserve enough space to avoid frequent reallocations

  size_t i = 0;
  size_t n = utf16.size();
  char buffer[4]; // Buffer to hold temporary UTF-8 bytes
  char *output = buffer;

  while (i < n) {
    uint16_t code_unit = utf16[i];
    if (!is_little_endian) {
      code_unit = swapBytes(code_unit);
    }
    if (i + 1 < n && code_unit >= 0xD800 && code_unit <= 0xDBFF &&
        utf16[i + 1] >= 0xDC00 && utf16[i + 1] <= 0xDFFF) {
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

// Testing Basic Logic
TEST(UTF16ToUTF8Test, BasicConversion) {
  std::u16string utf16 = u"Hello, 世界!";
  std::string utf8 = fury::utf16ToUtf8(utf16, true);
  ASSERT_EQ(utf8, u8"Hello, 世界!");
}

// Testing Empty String
TEST(UTF16ToUTF8Test, EmptyString) {
  std::u16string utf16 = u"";
  std::string utf8 = fury::utf16ToUtf8(utf16, true);
  ASSERT_EQ(utf8, "");
}

// Testing emoji
TEST(UTF16ToUTF8Test, SurrogatePairs) {
  std::u16string utf16 = {0xD83D, 0xDE00}; // 😀 emoji
  std::string utf8 = fury::utf16ToUtf8(utf16, true);
  ASSERT_EQ(utf8, "\xF0\x9F\x98\x80");
}

// Testing Boundary
TEST(UTF16ToUTF8Test, BoundaryValues) {
  std::u16string utf16 = {0x0000, 0xFFFF};
  std::string utf8 = fury::utf16ToUtf8(utf16, true);
  std::string expected_utf8 = std::string("\x00", 1) + "\xEF\xBF\xBF";
  ASSERT_EQ(utf8, expected_utf8);
}

// Testing Special Characters
TEST(UTF16ToUTF8Test, SpecialCharacters) {
  std::u16string utf16 = u" \n\t";
  std::string utf8 = fury::utf16ToUtf8(utf16, true);
  ASSERT_EQ(utf8, " \n\t");
}

// Testing LittleEndian
TEST(UTF16ToUTF8Test, LittleEndian) {
  std::u16string utf16 = {0x61, 0x62}; // "ab"
  std::string utf8 = fury::utf16ToUtf8(utf16, true);
  ASSERT_EQ(utf8, "ab");
}

// Testing BigEndian
TEST(UTF16ToUTF8Test, BigEndian) {
  std::u16string utf16 = {0xFFFE, 0xFFFE};
  std::string utf8 = fury::utf16ToUtf8(utf16, false);
  ASSERT_EQ(utf8, "\xEF\xBF\xBE\xEF\xBF\xBE");
}

// Testing Performance
TEST(UTF16ToUTF8Test, PerformanceTest) {
  const size_t num_tests = 1000;
  const size_t string_length = 1000;
  // Default little_endian
  bool is_little_endian = true;

  // Random UTF-16
  std::vector<std::u16string> test_strings;
  for (size_t i = 0; i < num_tests; ++i) {
    test_strings.push_back(generateRandomUTF16String(string_length));
  }

  // Lib
  try {
    auto start_time = std::chrono::high_resolution_clock::now();
    for (const auto &str : test_strings) {
      std::wstring_convert<std::codecvt_utf8_utf16<char16_t>, char16_t> convert;
      std::string utf8 = convert.to_bytes(str);
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        end_time - start_time)
                        .count();
    FURY_LOG(INFO) << "Standard library Running Time: " << duration << " ns";
  } catch (const std::exception &e) {
    FURY_LOG(FATAL) << "Caught exception: " << e.what();
  }

  // BaseLine
  try {
    auto start_time = std::chrono::high_resolution_clock::now();
    for (const auto &str : test_strings) {
      std::string utf8 = utf16ToUtf8BaseLine(str, is_little_endian);
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        end_time - start_time)
                        .count();
    FURY_LOG(INFO) << "Baseline Running Time: " << duration << " ns";
  } catch (const std::exception &e) {
    FURY_LOG(FATAL) << "Caught exception: " << e.what();
  }

  // SIMD
  try {
    auto start_time = std::chrono::high_resolution_clock::now();
    for (const auto &str : test_strings) {
      std::string utf8 = fury::utf16ToUtf8(str, is_little_endian);
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        end_time - start_time)
                        .count();
    FURY_LOG(INFO) << "SIMD Running Time: " << duration << " ns";
  } catch (const std::exception &e) {
    FURY_LOG(FATAL) << "Caught exception: " << e.what();
  }
}

// Generate random UTF-8 string
std::string generateRandomUTF8String(size_t length) {
  std::string str;
  std::mt19937 generator(std::random_device{}());
  std::uniform_int_distribution<uint32_t> distribution(0, 0x10FFFF);

  while (str.size() < length) {
    uint32_t code_point = distribution(generator);

    // Skip surrogate pairs (0xD800 to 0xDFFF) and other invalid Unicode code
    // points
    if ((code_point >= 0xD800 && code_point <= 0xDFFF) ||
        code_point > 0x10FFFF) {
      continue;
    }

    if (code_point <= 0x7F) {
      str.push_back(static_cast<char>(code_point));
    } else if (code_point <= 0x7FF) {
      str.push_back(0xC0 | (code_point >> 6));
      str.push_back(0x80 | (code_point & 0x3F));
    } else if (code_point <= 0xFFFF) {
      str.push_back(0xE0 | (code_point >> 12));
      str.push_back(0x80 | ((code_point >> 6) & 0x3F));
      str.push_back(0x80 | (code_point & 0x3F));
    } else if (code_point <= 0x10FFFF) {
      str.push_back(0xF0 | (code_point >> 18));
      str.push_back(0x80 | ((code_point >> 12) & 0x3F));
      str.push_back(0x80 | ((code_point >> 6) & 0x3F));
      str.push_back(0x80 | (code_point & 0x3F));
    }
  }

  return str;
}

std::u16string utf8ToUtf16BaseLine(const std::string &utf8,
                                   bool is_little_endian) {
  std::u16string utf16;   // Resulting UTF-16 string
  size_t i = 0;           // Index for traversing the UTF-8 string
  size_t n = utf8.size(); // Total length of the UTF-8 string

  // Loop through each byte of the UTF-8 string
  while (i < n) {
    uint32_t code_point = 0;   // The Unicode code point
    unsigned char c = utf8[i]; // Current byte of the UTF-8 string

    // Determine the number of bytes for this character based on its first byte
    if ((c & 0x80) == 0) {
      // 1-byte character (ASCII)
      code_point = c;
      ++i;
    } else if ((c & 0xE0) == 0xC0) {
      // 2-byte character
      code_point = c & 0x1F;
      code_point = (code_point << 6) | (utf8[i + 1] & 0x3F);
      i += 2;
    } else if ((c & 0xF0) == 0xE0) {
      // 3-byte character
      code_point = c & 0x0F;
      code_point = (code_point << 6) | (utf8[i + 1] & 0x3F);
      code_point = (code_point << 6) | (utf8[i + 2] & 0x3F);
      i += 3;
    } else if ((c & 0xF8) == 0xF0) {
      // 4-byte character
      code_point = c & 0x07;
      code_point = (code_point << 6) | (utf8[i + 1] & 0x3F);
      code_point = (code_point << 6) | (utf8[i + 2] & 0x3F);
      code_point = (code_point << 6) | (utf8[i + 3] & 0x3F);
      i += 4;
    } else {
      // Invalid UTF-8 byte sequence
      throw std::invalid_argument("Invalid UTF-8 encoding.");
    }

    // If the code point is beyond the BMP range, use surrogate pairs
    if (code_point >= 0x10000) {
      code_point -= 0x10000; // Subtract 0x10000 to get the surrogate pair
      uint16_t high_surrogate = 0xD800 + (code_point >> 10);  // High surrogate
      uint16_t low_surrogate = 0xDC00 + (code_point & 0x3FF); // Low surrogate

      // If not little-endian, swap bytes of the surrogates
      if (!is_little_endian) {
        high_surrogate = (high_surrogate >> 8) | (high_surrogate << 8);
        low_surrogate = (low_surrogate >> 8) | (low_surrogate << 8);
      }

      // Add both high and low surrogates to the UTF-16 string
      utf16.push_back(high_surrogate);
      utf16.push_back(low_surrogate);
    } else {
      // For code points within the BMP range, directly store as a 16-bit value
      uint16_t utf16_char = static_cast<uint16_t>(code_point);

      // If not little-endian, swap the bytes of the 16-bit character
      if (!is_little_endian) {
        utf16_char = (utf16_char >> 8) | (utf16_char << 8);
      }

      // Add the UTF-16 character to the string
      utf16.push_back(utf16_char);
    }
  }

  // Return the resulting UTF-16 string
  return utf16;
}

// Testing Basic Logic
TEST(UTF8ToUTF16Test, BasicConversion) {
  std::string utf8 = u8"Hello, 世界!";
  std::u16string utf16 = fury::utf8ToUtf16(utf8, true);
  ASSERT_EQ(utf16, u"Hello, 世界!");
}

// Testing Empty String
TEST(UTF8ToUTF16Test, EmptyString) {
  std::string utf8 = "";
  std::u16string utf16 = fury::utf8ToUtf16(utf8, true);
  ASSERT_EQ(utf16, u"");
}

// Testing emoji
TEST(UTF8ToUTF16Test, SurrogatePairs) {
  std::string utf8 = "\xF0\x9F\x98\x80"; // 😀 emoji
  std::u16string utf16 = fury::utf8ToUtf16(utf8, true);
  std::u16string expected_utf16 = {0xD83D, 0xDE00}; // Surrogate pair for emoji
  ASSERT_EQ(utf16, expected_utf16);
}

// Correct Boundary testing for U+FFFD (replacement character)
TEST(UTF8ToUTF16Test, BoundaryValues) {
  // "\xEF\xBF\xBD" is the UTF-8 encoding for U+FFFD (replacement character)
  std::string utf8 = "\xEF\xBF\xBD"; // U+FFFD in UTF-8
  std::u16string utf16 = fury::utf8ToUtf16(utf8, true);
  std::u16string expected_utf16 = {
      0xFFFD}; // Expected UTF-16 representation of U+FFFD
  ASSERT_EQ(utf16, expected_utf16);
}

// Testing Special Characters
TEST(UTF8ToUTF16Test, SpecialCharacters) {
  std::string utf8 = " \n\t";
  std::u16string utf16 = fury::utf8ToUtf16(utf8, true);
  ASSERT_EQ(utf16, u" \n\t");
}

// Testing LittleEndian
TEST(UTF8ToUTF16Test, LittleEndian) {
  std::string utf8 = "ab";
  std::u16string utf16 = fury::utf8ToUtf16(utf8, true);
  std::u16string expected_utf16 = {
      0x61, 0x62}; // Little-endian UTF-16 representation of "ab"
  ASSERT_EQ(utf16, expected_utf16);
}

// Correct BigEndian testing for BOM (Byte Order Mark)
TEST(UTF8ToUTF16Test, BigEndian) {
  std::string utf8 = "\xEF\xBB\xBF"; // BOM in UTF-8 (0xFEFF)
  std::u16string utf16 = fury::utf8ToUtf16(utf8, false); // Big-endian
  std::u16string expected_utf16 = {0xFFFE}; // Expected BOM in UTF-16
  ASSERT_EQ(utf16, expected_utf16);
}

// Testing round-trip conversion (UTF-8 -> UTF-16 -> UTF-8)
TEST(UTF8ToUTF16Test, RoundTripConversion) {
  std::string original_utf8 = u8"Hello, 世界!";
  std::u16string utf16 = fury::utf8ToUtf16(original_utf8, true);
  std::string utf8_converted_back = fury::utf16ToUtf8(utf16, true);
  ASSERT_EQ(original_utf8, utf8_converted_back);
}

// Testing Performance
TEST(UTF8ToUTF16Test, PerformanceTest) {
  const size_t num_tests = 1000;
  const size_t string_length = 1000;
  // Default little_endian
  bool is_little_endian = true;

  // Random UTF-8
  std::vector<std::string> test_strings;
  for (size_t i = 0; i < num_tests; ++i) {
    test_strings.push_back(generateRandomUTF8String(string_length));
  }

  // Standard Library
  try {
    auto start_time = std::chrono::high_resolution_clock::now();
    std::wstring_convert<std::codecvt_utf8<wchar_t>, wchar_t> convert;
    // Loop through test strings and convert each UTF-8 string to UTF-16
    for (const auto &str : test_strings) {
      std::wstring wide_str = convert.from_bytes(str);
      std::u16string utf16;
      for (wchar_t wc : wide_str) {
        utf16.push_back(static_cast<char16_t>(wc));
      }
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        end_time - start_time)
                        .count();
    FURY_LOG(INFO) << "Standard Library Running Time: " << duration << " ns";
  } catch (const std::exception &e) {
    FURY_LOG(FATAL) << "Caught exception in standard library conversion: "
                    << e.what();
  }

  // BaseLine
  try {
    auto start_time = std::chrono::high_resolution_clock::now();
    for (const auto &str : test_strings) {
      std::u16string utf16 = utf8ToUtf16BaseLine(str, is_little_endian);
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        end_time - start_time)
                        .count();
    FURY_LOG(INFO) << "BaseLine Running Time: " << duration << " ns";
  } catch (const std::exception &e) {
    FURY_LOG(FATAL) << "Caught exception in baseline conversion: " << e.what();
  }

  // Optimized (SIMD)
  try {
    auto start_time = std::chrono::high_resolution_clock::now();
    for (const auto &str : test_strings) {
      std::u16string utf16 = fury::utf8ToUtf16(str, is_little_endian);
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                        end_time - start_time)
                        .count();
    FURY_LOG(INFO) << "SIMD Optimized Running Time: " << duration << " ns";
  } catch (const std::exception &e) {
    FURY_LOG(FATAL) << "Caught exception in SIMD optimized conversion: "
                    << e.what();
  }
}

} // namespace fury

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}