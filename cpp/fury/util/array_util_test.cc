#include "fury/util/array_util.h"
#include "gtest/gtest.h"

namespace fury {
TEST(GetMaxValueTest, HandlesEmptyArray) {
  uint16_t arr[] = {};
  EXPECT_EQ(getMaxValue(arr, 0), 0);
}

TEST(GetMaxValueTest, HandlesSingleElementArray) {
  uint16_t arr[] = {42};
  EXPECT_EQ(getMaxValue(arr, 1), 42);
}

TEST(GetMaxValueTest, HandlesSmallArray) {
  uint16_t arr[] = {10, 20, 30, 40, 5};
  EXPECT_EQ(getMaxValue(arr, 5), 40);
}

TEST(GetMaxValueTest, HandlesLargeArray) {
  const size_t length = 1024;
  uint16_t arr[length];
  for (size_t i = 0; i < length; ++i) {
    arr[i] = static_cast<uint16_t>(i);
  }
  EXPECT_EQ(getMaxValue(arr, length), 1023);
}
} // namespace fury

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
