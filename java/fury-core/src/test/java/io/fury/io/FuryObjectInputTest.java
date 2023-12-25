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

package io.fury.io;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import io.fury.Fury;
import io.fury.memory.MemoryBuffer;
import io.fury.memory.MemoryUtils;
import java.io.IOException;
import org.testng.annotations.Test;

public class FuryObjectInputTest {

  @Test
  public void testFuryObjectInput() throws IOException {
    Fury fury = Fury.builder().build();
    MemoryBuffer buffer = MemoryUtils.buffer(32);
    buffer.writeByte(1);
    buffer.writeInt(2);
    buffer.writeLong(3);
    buffer.writeBoolean(true);
    buffer.writeFloat(4.1f);
    buffer.writeDouble(4.2);
    fury.writeJavaString(buffer, "abc");
    try (FuryObjectInput input = new FuryObjectInput(fury, buffer)) {
      assertEquals(input.readByte(), 1);
      assertEquals(input.readInt(), 2);
      assertEquals(input.readLong(), 3);
      assertTrue(input.readBoolean());
      assertEquals(input.readFloat(), 4.1f);
      assertEquals(input.readDouble(), 4.2);
      assertEquals(input.readUTF(), "abc");
    }
  }
}
