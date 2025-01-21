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

#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "fury/util/logging.h"
#include "gtest/gtest.h"

namespace fury {

TEST(PrintLogTest, BasicLog) {
  FURY_LOG(FURY_INFO) << "test info";
  ASSERT_DEATH(FURY_LOG(FURY_FATAL) << "test fatal",
               "\\[.*\\] FATAL cpp/fury/util/logging_test.cc:.*: test fatal");
}

TEST(PrintLogTest, TestCheckOp) {
  int i = 1;
  FURY_CHECK_EQ(i, 1);
  ASSERT_DEATH(FURY_CHECK_EQ(i, 2), "1 vs 2");

  FURY_CHECK_NE(i, 0);
  ASSERT_DEATH(FURY_CHECK_NE(i, 1), "1 vs 1");

  FURY_CHECK_LE(i, 1);
  ASSERT_DEATH(FURY_CHECK_LE(i, 0), "1 vs 0");

  FURY_CHECK_LT(i, 2);
  ASSERT_DEATH(FURY_CHECK_LT(i, 1), "1 vs 1");

  FURY_CHECK_GE(i, 1);
  ASSERT_DEATH(FURY_CHECK_GE(i, 2), "1 vs 2");

  FURY_CHECK_GT(i, 0);
  ASSERT_DEATH(FURY_CHECK_GT(i, 1), "1 vs 1");

  int j = 0;
  FURY_CHECK_NE(i, j);
  ASSERT_DEATH(FURY_CHECK_EQ(i, j), "1 vs 0");
}

std::string TestFunctionLevel0() {
  std::string call_trace = GetCallTrace();
  return call_trace;
}

std::string TestFunctionLevel1() { return TestFunctionLevel0(); }

std::string TestFunctionLevel2() { return TestFunctionLevel1(); }

#ifndef _WIN32
TEST(PrintLogTest, CallstackTraceTest) {
  auto ret = TestFunctionLevel2();
  FURY_LOG(FURY_INFO) << "stack trace:\n" << ret;
  // work for linux
  // EXPECT_TRUE(ret.find("TestFunctionLevel0") != std::string::npos);
  // work for mac
  // EXPECT_TRUE(ret.find("GetCallTrace") != std::string::npos);
  EXPECT_TRUE(ret.find("fury") != std::string::npos);
  EXPECT_TRUE(ret.find("PrintLogTest") != std::string::npos);
}
#endif

} // namespace fury

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
