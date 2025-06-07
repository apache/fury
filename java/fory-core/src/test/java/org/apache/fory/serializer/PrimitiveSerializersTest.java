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

package org.apache.fory.serializer;

import static org.testng.Assert.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.fory.Fory;
import org.apache.fory.ForyTestBase;
import org.apache.fory.config.ForyBuilder;
import org.apache.fory.config.Language;
import org.apache.fory.config.LongEncoding;
import org.apache.fory.memory.MemoryBuffer;
import org.testng.annotations.Test;

public class PrimitiveSerializersTest extends ForyTestBase {
  @Test
  public void testUint8Serializer() {
    Fory fory = Fory.builder().withLanguage(Language.XLANG).requireClassRegistration(false).build();
    PrimitiveSerializers.Uint8Serializer serializer =
        new PrimitiveSerializers.Uint8Serializer(fory);
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
    Fory fory = Fory.builder().withLanguage(Language.XLANG).requireClassRegistration(false).build();
    PrimitiveSerializers.Uint16Serializer serializer =
        new PrimitiveSerializers.Uint16Serializer(fory);
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
      ForyBuilder builder =
          Fory.builder()
              .withLanguage(Language.JAVA)
              .withCodegen(codegen)
              .requireClassRegistration(false);
      serDeCheck(
          builder.withNumberCompressed(true).withLongCompressed(LongEncoding.PVL).build(), struct);
      serDeCheck(
          builder.withNumberCompressed(true).withLongCompressed(LongEncoding.SLI).build(), struct);
    } else {
      Fory fory =
          Fory.builder()
              .withLanguage(Language.JAVA)
              .withCodegen(codegen)
              .requireClassRegistration(false)
              .build();
      serDeCheck(fory, struct);
    }
  }

  @Test(dataProvider = "foryCopyConfig")
  public void testPrimitiveStruct(Fory fory) {
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
    copyCheck(fory, struct);
  }
}
