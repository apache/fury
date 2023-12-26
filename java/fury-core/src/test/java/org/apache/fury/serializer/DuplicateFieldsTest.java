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

import static org.testng.Assert.assertEquals;

import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.builder.CodecUtils;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.Language;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.testng.annotations.Test;

public class DuplicateFieldsTest extends FuryTestBase {

  @ToString
  @EqualsAndHashCode
  public static class B {
    int f1;
    int f2;
  }

  @ToString(callSuper = true)
  @EqualsAndHashCode(callSuper = true)
  public static class C extends B {
    int f1;
  }

  @Test()
  public void testDuplicateFieldsNoCompatible() {
    C c = new C();
    ((B) c).f1 = 100;
    c.f1 = -100;
    assertEquals(((B) c).f1, 100);
    assertEquals(c.f1, -100);
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(false)
            .withCodegen(true)
            .requireClassRegistration(false)
            .build();
    {
      ObjectSerializer<C> serializer = new ObjectSerializer<>(fury, C.class);
      MemoryBuffer buffer = MemoryUtils.buffer(32);
      serializer.write(buffer, c);
      C newC = serializer.read(buffer);
      assertEquals(newC.f1, c.f1);
      assertEquals(((B) newC).f1, ((B) c).f1);
      assertEquals(newC, c);
    }
    {
      Serializer<C> serializer =
          Serializers.newSerializer(
              fury, C.class, CodecUtils.loadOrGenObjectCodecClass(C.class, fury));
      MemoryBuffer buffer = MemoryUtils.buffer(32);
      serializer.write(buffer, c);
      C newC = serializer.read(buffer);
      assertEquals(newC.f1, c.f1);
      assertEquals(((B) newC).f1, ((B) c).f1);
      assertEquals(newC, c);
    }
    {
      // FallbackSerializer/CodegenSerializer will set itself to ClassResolver.
      Fury fury1 =
          Fury.builder()
              .withLanguage(Language.JAVA)
              .withRefTracking(false)
              .withCodegen(true)
              .requireClassRegistration(false)
              .build();
      C newC = (C) serDeCheckSerializer(fury1, c, "Codec");
      assertEquals(newC.f1, c.f1);
      assertEquals(((B) newC).f1, ((B) c).f1);
      assertEquals(newC, c);
    }
  }

  @Test()
  public void testDuplicateFieldsCompatible() {
    C c = new C();
    ((B) c).f1 = 100;
    c.f1 = -100;
    assertEquals(((B) c).f1, 100);
    assertEquals(c.f1, -100);
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(false)
            .withCodegen(true)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .requireClassRegistration(false)
            .build();
    {
      CompatibleSerializer<C> serializer = new CompatibleSerializer<>(fury, C.class);
      MemoryBuffer buffer = MemoryUtils.buffer(32);
      serializer.write(buffer, c);
      C newC = serializer.read(buffer);
      assertEquals(newC.f1, c.f1);
      assertEquals(((B) newC).f1, ((B) c).f1);
      assertEquals(newC, c);
    }
    {
      Serializer<C> serializer =
          Serializers.newSerializer(
              fury, C.class, CodecUtils.loadOrGenCompatibleCodecClass(C.class, fury));
      MemoryBuffer buffer = MemoryUtils.buffer(32);
      serializer.write(buffer, c);
      C newC = serializer.read(buffer);
      assertEquals(newC.f1, c.f1);
      assertEquals(((B) newC).f1, ((B) c).f1);
      assertEquals(newC, c);
    }
    {
      // FallbackSerializer/CodegenSerializer will set itself to ClassResolver.
      Fury fury1 =
          Fury.builder()
              .withLanguage(Language.JAVA)
              .withRefTracking(false)
              .withCodegen(true)
              .withCompatibleMode(CompatibleMode.COMPATIBLE)
              .requireClassRegistration(false)
              .build();
      C newC = (C) serDeCheckSerializer(fury1, c, "Compatible.*Codec");
      assertEquals(newC.f1, c.f1);
      assertEquals(((B) newC).f1, ((B) c).f1);
      assertEquals(newC, c);
    }
  }
}
