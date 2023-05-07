/*
 * Copyright 2023 The Fury authors
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.serializer;

import static org.testng.Assert.*;

import io.fury.Fury;
import io.fury.FuryTestBase;
import io.fury.Language;
import io.fury.memory.MemoryBuffer;
import org.testng.annotations.Test;

public class SerializersTest extends FuryTestBase {

  @Test
  public void testUint8Serializer() {
    Fury fury = Fury.builder().withLanguage(Language.XLANG).disableSecureMode().build();
    Serializers.Uint8Serializer serializer = new Serializers.Uint8Serializer(fury);
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(8);
    serializer.crossLanguageWrite(buffer, 0);
    assertEquals(serializer.crossLanguageRead(buffer), Integer.valueOf(0));
    serializer.crossLanguageWrite(buffer, 255);
    assertEquals(serializer.crossLanguageRead(buffer), Integer.valueOf(255));
    assertThrows(IllegalArgumentException.class, () -> serializer.crossLanguageWrite(buffer, -1));
    assertThrows(IllegalArgumentException.class, () -> serializer.crossLanguageWrite(buffer, 256));
  }

  @Test
  public void testUint16Serializer() {
    Fury fury = Fury.builder().withLanguage(Language.XLANG).disableSecureMode().build();
    Serializers.Uint16Serializer serializer = new Serializers.Uint16Serializer(fury);
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(16);
    serializer.crossLanguageWrite(buffer, 0);
    assertEquals(serializer.crossLanguageRead(buffer), Integer.valueOf(0));
    serializer.crossLanguageWrite(buffer, 65535);
    assertEquals(serializer.crossLanguageRead(buffer), Integer.valueOf(65535));
    assertThrows(IllegalArgumentException.class, () -> serializer.crossLanguageWrite(buffer, -1));
    assertThrows(
      IllegalArgumentException.class, () -> serializer.crossLanguageWrite(buffer, 65536));
  }


  @Test(dataProvider = "crossLanguageReferenceTrackingConfig")
  public void testStringBuilder(boolean referenceTracking, Language language) {
    Fury fury1 =
        Fury.builder()
            .withLanguage(language)
            .withReferenceTracking(referenceTracking)
            .disableSecureMode()
            .build();
    Fury fury2 =
        Fury.builder()
            .withLanguage(language)
            .withReferenceTracking(referenceTracking)
            .disableSecureMode()
            .build();
    assertEquals("str", serDe(fury1, fury2, "str"));
    assertEquals("str", serDe(fury1, fury2, new StringBuilder("str")).toString());
    assertEquals("str", serDe(fury1, fury2, new StringBuffer("str")).toString());
  }
}
