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
#include <chrono>
#include <gtest/gtest.h>
#include <iostream>

namespace fury {

#if defined(__x86_64__) || defined(_M_X64)
bool isLatin(const std::string &str) { return isLatin_AVX2(str); }

bool isLatin2(const std::string &str) { return isLatin_SSE2(str); }
#elif defined(__ARM_NEON) || defined(__ARM_NEON__)
bool isLatin(const std::string &str) { return isLatin_NEON(str); }
#elif defined(__riscv) && __riscv_vector
bool isLatin(const std::string &str) { return isLatin_RISCV(str); }
#else
bool isLatin(const std::string &str) { return isLatin_Baseline(str); }
#endif

TEST(StringUtilTest, TestIsLatinFunctions) {
  std::string testStr = generateRandomString(100000);

  auto start_time = std::chrono::high_resolution_clock::now();
  bool result = isLatin(testStr);
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(
                      end_time - start_time)
                      .count();
  std::cout << "Running Time: " << duration << " ns" << std::endl;
  EXPECT_TRUE(result);
}

TEST(StringUtilTest, TestIsLatinLogic) {
  // Test with Latin strings
  EXPECT_TRUE(isLatin("Fury"));
  EXPECT_TRUE(isLatin(generateRandomString(80)));

  // Test unaligned strings
  std::string randomStr = generateRandomString(80);
  EXPECT_TRUE(isLatin(randomStr + "1"));
  EXPECT_TRUE(isLatin(randomStr + "12"));
  EXPECT_TRUE(isLatin(randomStr + "123"));

  // Test with non-Latin strings
  EXPECT_FALSE(isLatin("你好, Fury"));
  EXPECT_FALSE(isLatin(randomStr + "你好"));
  EXPECT_FALSE(isLatin(randomStr + "1你好"));
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

} // namespace fury

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
