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
