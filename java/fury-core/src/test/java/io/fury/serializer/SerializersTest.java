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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Currency;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.Assert;
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
    Fury.FuryBuilder builder =
        Fury.builder()
            .withLanguage(language)
            .withReferenceTracking(referenceTracking)
            .disableSecureMode();
    Fury fury1 = builder.build();
    Fury fury2 = builder.build();
    assertEquals("str", serDe(fury1, fury2, "str"));
    assertEquals("str", serDe(fury1, fury2, new StringBuilder("str")).toString());
    assertEquals("str", serDe(fury1, fury2, new StringBuffer("str")).toString());
  }

  public enum EnumFoo {
    A,
    B
  }

  public enum EnumSubClass {
    A {
      @Override
      void f() {}
    },
    B {
      @Override
      void f() {}
    };

    abstract void f();
  }

  @Test(dataProvider = "crossLanguageReferenceTrackingConfig")
  public void testEnumSerialization(boolean referenceTracking, Language language) {
    Fury.FuryBuilder builder =
        Fury.builder()
            .withLanguage(language)
            .withReferenceTracking(referenceTracking)
            .disableSecureMode();
    Fury fury1 = builder.build();
    Fury fury2 = builder.build();
    assertEquals(SerializersTest.EnumFoo.A, serDe(fury1, fury2, SerializersTest.EnumFoo.A));
    assertEquals(SerializersTest.EnumFoo.B, serDe(fury1, fury2, SerializersTest.EnumFoo.B));
    assertEquals(
        SerializersTest.EnumSubClass.A, serDe(fury1, fury2, SerializersTest.EnumSubClass.A));
    assertEquals(
        SerializersTest.EnumSubClass.B, serDe(fury1, fury2, SerializersTest.EnumSubClass.B));
  }

  @Test(dataProvider = "crossLanguageReferenceTrackingConfig")
  public void testBigInt(boolean referenceTracking, Language language) {
    Fury.FuryBuilder builder =
        Fury.builder()
            .withLanguage(language)
            .withReferenceTracking(referenceTracking)
            .disableSecureMode();
    Fury fury1 = builder.build();
    Fury fury2 = builder.build();
    assertEquals(BigInteger.valueOf(100), serDe(fury1, fury2, BigInteger.valueOf(100)));
    assertEquals(BigDecimal.valueOf(100, 2), serDe(fury1, fury2, BigDecimal.valueOf(100, 2)));
  }

  @Test(dataProvider = "javaFury")
  public void testAtomic(Fury fury) {
    assertTrue(
        ((AtomicBoolean) serDeCheckSerializer(fury, new AtomicBoolean(true), "AtomicBoolean"))
            .get());

    Assert.assertEquals(
        ((AtomicInteger) serDeCheckSerializer(fury, new AtomicInteger(100), "AtomicInteger")).get(),
        100);
    Assert.assertEquals(
        ((AtomicLong) serDeCheckSerializer(fury, new AtomicLong(200), "AtomicLong")).get(), 200);
    Assert.assertEquals(
        ((AtomicReference)
                serDeCheckSerializer(fury, new AtomicReference<>(200), "AtomicReference"))
            .get(),
        200);
  }

  @Test
  public void testCurrency() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).build();
    Assert.assertEquals(
        serDeCheckSerializer(fury, Currency.getInstance("EUR"), "Currency"),
        Currency.getInstance("EUR"));
  }

  @Test
  public void testCharset() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
    Assert.assertEquals(
        serDeCheckSerializer(fury, Charset.defaultCharset(), "Charset"), Charset.defaultCharset());
  }

  @Test
  public void testURI() throws URISyntaxException {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).build();
    Assert.assertEquals(serDeCheckSerializer(fury, new URI(""), "URI"), new URI(""));
    Assert.assertEquals(serDeCheckSerializer(fury, new URI("abc"), "URI"), new URI("abc"));
  }
}
