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

package org.apache.fury.serializer;

import static org.testng.Assert.*;

import org.apache.fury.Fury;
import org.apache.fury.config.Language;
import org.apache.fury.memory.MemoryBuffer;
import org.testng.annotations.Test;

public class PrimitiveSerializersTest {
  @Test
  public void testUint8Serializer() {
    Fury fury = Fury.builder().withLanguage(Language.XLANG).requireClassRegistration(false).build();
    PrimitiveSerializers.Uint8Serializer serializer =
        new PrimitiveSerializers.Uint8Serializer(fury);
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(8);
    serializer.xwrite(buffer, 0);
    assertEquals(serializer.xread(buffer), Integer.valueOf(0));
    serializer.xwrite(buffer, 255);
    assertEquals(serializer.xread(buffer), Integer.valueOf(255));
    assertThrows(IllegalArgumentException.class, () -> serializer.xwrite(buffer, -1));
    assertThrows(IllegalArgumentException.class, () -> serializer.xwrite(buffer, 256));
  }

  @Test
  public void testUint16Serializer() {
    Fury fury = Fury.builder().withLanguage(Language.XLANG).requireClassRegistration(false).build();
    PrimitiveSerializers.Uint16Serializer serializer =
        new PrimitiveSerializers.Uint16Serializer(fury);
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(16);
    serializer.xwrite(buffer, 0);
    assertEquals(serializer.xread(buffer), Integer.valueOf(0));
    serializer.xwrite(buffer, 65535);
    assertEquals(serializer.xread(buffer), Integer.valueOf(65535));
    assertThrows(IllegalArgumentException.class, () -> serializer.xwrite(buffer, -1));
    assertThrows(IllegalArgumentException.class, () -> serializer.xwrite(buffer, 65536));
  }
}
