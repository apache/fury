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

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.FuryBuilder;
import org.apache.fury.config.Language;
import org.apache.fury.config.LongEncoding;
import org.apache.fury.memory.MemoryBuffer;
import org.testng.annotations.Test;

public class PrimitiveSerializersTest extends FuryTestBase {
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

  @Data
  @AllArgsConstructor
  public static class PrimitiveStruct {
    byte byte1;
    byte byte2;
    char char1;
    char char2;
    short short1;
    short short2;
    int int1;
    int int2;
    long long1;
    long long2;
    long long3;
    float float1;
    float float2;
    double double1;
    double double2;
  }

  @Test(dataProvider = "compressNumberAndCodeGen")
  public void testPrimitiveStruct(boolean compressNumber, boolean codegen) {
    PrimitiveStruct struct =
        new PrimitiveStruct(
            Byte.MIN_VALUE,
            Byte.MIN_VALUE,
            Character.MIN_VALUE,
            Character.MIN_VALUE,
            Short.MIN_VALUE,
            Short.MIN_VALUE,
            Integer.MIN_VALUE,
            Integer.MIN_VALUE,
            Long.MIN_VALUE,
            Long.MIN_VALUE,
            -3763915443215605988L, // test Long.reverseBytes in _readVarInt64OnBE
            Float.MIN_VALUE,
            Float.MIN_VALUE,
            Double.MIN_VALUE,
            Double.MIN_VALUE);
    if (compressNumber) {
      FuryBuilder builder =
          Fury.builder()
              .withLanguage(Language.JAVA)
              .withCodegen(codegen)
              .requireClassRegistration(false);
      serDeCheck(
          builder.withNumberCompressed(true).withLongCompressed(LongEncoding.PVL).build(), struct);
      serDeCheck(
          builder.withNumberCompressed(true).withLongCompressed(LongEncoding.SLI).build(), struct);
    } else {
      Fury fury =
          Fury.builder()
              .withLanguage(Language.JAVA)
              .withCodegen(codegen)
              .requireClassRegistration(false)
              .build();
      serDeCheck(fury, struct);
    }
  }
}
