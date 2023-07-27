/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.io;

import static org.testng.Assert.assertEquals;

import java.nio.ByteBuffer;
import org.testng.annotations.Test;

public class MockWritableByteChannelTest {

  @Test
  public void testTotalBytes() {
    try (MockWritableByteChannel channel = new MockWritableByteChannel()) {
      channel.write(ByteBuffer.allocate(100));
      channel.write(ByteBuffer.allocateDirect(100));
      ByteBuffer buffer = ByteBuffer.allocate(100);
      buffer.position(50);
      channel.write(buffer);
      assertEquals(channel.totalBytes(), 250);
    }
  }
}
