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
#include <gtest/gtest.h>
#include <chrono>

namespace fury {

    TEST(StringUtilTest, TestIsLatinAVX2) {
    std::string testStr = generateRandomString(100000);
    auto start_time = std::chrono::high_resolution_clock::now();
    bool result = isLatin_AVX2(testStr);
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
    std::cout << "AVX2 Running Time: " << duration << " ns" << std::endl;
    EXPECT_TRUE(result);
}

TEST(StringUtilTest, TestIsLatinSSE2) {
std::string testStr = generateRandomString(100000);
auto start_time = std::chrono::high_resolution_clock::now();
bool result = isLatin_SSE2(testStr);
auto end_time = std::chrono::high_resolution_clock::now();
auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
std::cout << "SSE2 Running Time: " << duration << " ns" << std::endl;
EXPECT_TRUE(result);
}

TEST(StringUtilTest, TestIsLatinBaseline) {
std::string testStr = generateRandomString(100000);
auto start_time = std::chrono::high_resolution_clock::now();
bool result = isLatin_Baseline(testStr);
auto end_time = std::chrono::high_resolution_clock::now();
auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
std::cout << "Baseline Running Time: " << duration << " ns" << std::endl;
EXPECT_TRUE(result);
}

TEST(StringUtilTest, TestIsLatinAVX2_vpshufb) {
std::string testStr = generateRandomString(100000);
auto start_time = std::chrono::high_resolution_clock::now();
bool result = isLatin_AVX2_vpshufb(testStr);
auto end_time = std::chrono::high_resolution_clock::now();
auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count();
std::cout << "AVX2_vpshufb Running Time: " << duration << " ns" << std::endl;
EXPECT_TRUE(result);
}

} // namespace fury

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
