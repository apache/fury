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

#include "gtest/gtest.h"

#include "fury/util/buffer.h"

namespace fury {

TEST(Buffer, ToString) {
  std::shared_ptr<Buffer> buffer;
  AllocateBuffer(16, &buffer);
  for (int i = 0; i < 16; ++i) {
    buffer->UnsafePutByte<int8_t>(i, static_cast<int8_t>('a' + i));
  }
  EXPECT_EQ(buffer->ToString(), "abcdefghijklmnop");

  float f = 1.11;
  buffer->UnsafePut<float>(0, f);
  EXPECT_EQ(buffer->Get<float>(0), f);
}

void checkVarUint32(int32_t startOffset, std::shared_ptr<Buffer> buffer,
                    int32_t value, uint32_t bytesWritten) {
  uint32_t actualBytesWritten = buffer->PutVarUint32(startOffset, value);
  EXPECT_EQ(actualBytesWritten, bytesWritten);
  uint32_t readBytesLength;
  int32_t varInt = buffer->GetVarUint32(startOffset, &readBytesLength);
  EXPECT_EQ(value, varInt);
  EXPECT_EQ(readBytesLength, bytesWritten);
}

TEST(Buffer, TestVarUint) {
  std::shared_ptr<Buffer> buffer;
  AllocateBuffer(64, &buffer);
  for (int i = 0; i < 32; ++i) {
    checkVarUint32(i, buffer, 1, 1);
    checkVarUint32(i, buffer, 1 << 6, 1);
    checkVarUint32(i, buffer, 1 << 7, 2);
    checkVarUint32(i, buffer, 1 << 13, 2);
    checkVarUint32(i, buffer, 1 << 14, 3);
    checkVarUint32(i, buffer, 1 << 20, 3);
    checkVarUint32(i, buffer, 1 << 21, 4);
    checkVarUint32(i, buffer, 1 << 27, 4);
    checkVarUint32(i, buffer, 1 << 28, 5);
    checkVarUint32(i, buffer, 1 << 30, 5);
  }
}

} // namespace fury

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
