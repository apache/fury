#include "fury/util/util.h"
#include "gtest/gtest.h"

namespace fury {

TEST(TimeTest, TestFormatTimePoint) {
  std::tm tm{};             // zero initialise
  tm.tm_year = 2022 - 1900; // 2020
  tm.tm_mon = 7 - 1;        // February
  tm.tm_mday = 2;           // 15th
  tm.tm_hour = 10;
  tm.tm_min = 20;
  tm.tm_sec = 20;
  tm.tm_isdst = 0; // Not daylight saving
  std::time_t t = std::mktime(&tm);
  auto tp = std::chrono::system_clock::from_time_t(t);
  EXPECT_EQ(FormatTimePoint(tp), "2022-07-02 10:20:20,000");
  FormatTimePoint(std::chrono::system_clock::now());
}

} // namespace fury

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
