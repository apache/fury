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

namespace fury {
// TEST efficiency
TEST(StringUtilTest, TestIsLatinFunctions) {
  // Generate a unique test string once
  std::string testStr = generateRandomString(100000);
  std::cout << "testStr : " << std::endl;

  //  Baseline
  auto start_time = std::chrono::high_resolution_clock::now();
  bool result_baseline = isLatin_Baseline(testStr);
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration_baseline = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               end_time - start_time)
                               .count();
  std::cout << "Baseline Running Time: " << duration_baseline << " ns"
            << std::endl;
  EXPECT_TRUE(result_baseline);

  //  AVX2
  start_time = std::chrono::high_resolution_clock::now();
  bool result_avx2 = isLatin_AVX2(testStr);
  end_time = std::chrono::high_resolution_clock::now();
  auto duration_avx2 = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           end_time - start_time)
                           .count();
  std::cout << "AVX2 Running Time: " << duration_avx2 << " ns" << std::endl;
  EXPECT_TRUE(result_avx2);

  //  SSE2
  start_time = std::chrono::high_resolution_clock::now();
  bool result_sse2 = isLatin_SSE2(testStr);
  end_time = std::chrono::high_resolution_clock::now();
  auto duration_sse2 = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           end_time - start_time)
                           .count();
  std::cout << "SSE2 Running Time: " << duration_sse2 << " ns" << std::endl;
  EXPECT_TRUE(result_sse2);
}

// TEST Logic
TEST(StringUtilTest, TestIsLatinLogic) {
  // 使用AVX2指令集进行逻辑测试
  EXPECT_TRUE(isLatin_AVX2("Fury"));

  std::string randomStr = generateRandomString(80);
  EXPECT_TRUE(isLatin_AVX2(randomStr));

  // Test unaligned strings
  EXPECT_TRUE(isLatin_AVX2(randomStr + "1"));
  EXPECT_TRUE(isLatin_AVX2(randomStr + "12"));
  EXPECT_TRUE(isLatin_AVX2(randomStr + "123"));

  EXPECT_FALSE(isLatin_AVX2("你好, Fury"));
  EXPECT_FALSE(isLatin_AVX2(randomStr + "你好"));
  EXPECT_FALSE(isLatin_AVX2(randomStr + "1你好"));
  EXPECT_FALSE(isLatin_AVX2(generateRandomString(11) + "你"));
  EXPECT_FALSE(isLatin_AVX2(generateRandomString(10) + "你好"));
  EXPECT_FALSE(isLatin_AVX2(generateRandomString(9) + "性能好"));
  EXPECT_FALSE(isLatin_AVX2("\u1234"));
  EXPECT_FALSE(isLatin_AVX2("a\u1234"));
  EXPECT_FALSE(isLatin_AVX2("ab\u1234"));
  EXPECT_FALSE(isLatin_AVX2("abc\u1234"));
  EXPECT_FALSE(isLatin_AVX2("abcd\u1234"));
  EXPECT_FALSE(isLatin_AVX2("Javaone Keynote\u1234"));
}

} // namespace fury

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}