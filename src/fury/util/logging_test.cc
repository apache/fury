#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "fury/util/logging.h"
#include "gtest/gtest.h"

namespace fury {

TEST(PrintLogTest, BasicLog) {
  FURY_LOG(INFO) << "test info";
  ASSERT_DEATH(FURY_LOG(FATAL) << "test fatal",
               "\\[.*\\] FATAL src/fury/util/logging_test.cc:.*: test fatal");
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
  FURY_LOG(INFO) << "stack trace:\n" << ret;
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
