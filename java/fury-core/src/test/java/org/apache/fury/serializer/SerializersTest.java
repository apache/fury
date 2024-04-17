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
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Currency;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.FuryBuilder;
import org.apache.fury.config.Language;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SerializersTest extends FuryTestBase {

  @Test(dataProvider = "crossLanguageReferenceTrackingConfig")
  public void testStringBuilder(boolean referenceTracking, Language language) {
    FuryBuilder builder =
        Fury.builder()
            .withLanguage(language)
            .withRefTracking(referenceTracking)
            .requireClassRegistration(false);
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
    FuryBuilder builder =
        Fury.builder()
            .withLanguage(language)
            .withRefTracking(referenceTracking)
            .requireClassRegistration(false);
    Fury fury1 = builder.build();
    Fury fury2 = builder.build();
    assertEquals(EnumFoo.A, serDe(fury1, fury2, EnumFoo.A));
    assertEquals(EnumFoo.B, serDe(fury1, fury2, EnumFoo.B));
    assertEquals(EnumSubClass.A, serDe(fury1, fury2, EnumSubClass.A));
    assertEquals(EnumSubClass.B, serDe(fury1, fury2, EnumSubClass.B));
  }

  @Test(dataProvider = "crossLanguageReferenceTrackingConfig")
  public void testBigInt(boolean referenceTracking, Language language) {
    FuryBuilder builder =
        Fury.builder()
            .withLanguage(language)
            .withRefTracking(referenceTracking)
            .requireClassRegistration(false);
    Fury fury1 = builder.build();
    Fury fury2 = builder.build();
    assertEquals(BigInteger.valueOf(100), serDe(fury1, fury2, BigInteger.valueOf(100)));
    assertEquals(BigDecimal.valueOf(100, 2), serDe(fury1, fury2, BigDecimal.valueOf(100, 2)));
    BigInteger bigInteger = new BigInteger("999999999999999999999999999999999999999999999999");
    BigDecimal bigDecimal = new BigDecimal(bigInteger, 200, MathContext.DECIMAL128);
    serDeCheck(fury1, bigDecimal);
    serDeCheck(
        fury1, new BigInteger("11111111110101010000283895380202208220050200000000111111111"));
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
    Assert.assertEquals(
        serDeCheckSerializer(getJavaFury(), Currency.getInstance("EUR"), "Currency"),
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
    Assert.assertEquals(serDeCheckSerializer(getJavaFury(), new URI(""), "URI"), new URI(""));
    Assert.assertEquals(serDeCheckSerializer(getJavaFury(), new URI("abc"), "URI"), new URI("abc"));
  }

  @Test
  public void testRegex() {
    Assert.assertEquals(
        serDeCheckSerializer(getJavaFury(), Pattern.compile("abc"), "Regex").toString(),
        Pattern.compile("abc").toString());
  }

  @Test
  public void testUUID() {
    UUID uuid = UUID.randomUUID();
    Assert.assertEquals(serDeCheckSerializer(getJavaFury(), uuid, "UUID"), uuid);
  }

  private static class TestClassSerialization {}

  private static class TestReplaceClassSerialization {
    private Object writeReplace() {
      return 1;
    }
  }

  @Test
  public void testSerializeClass() {
    Fury fury = Fury.builder().requireClassRegistration(false).build();
    // serialize both TestReplaceClassSerialization object and class.
    // Scala `object` native serialization will return ModuleSerializationProxy will write original
    // class.
    List<Object> list =
        serDe(
            fury,
            Arrays.asList(
                new TestReplaceClassSerialization(), TestReplaceClassSerialization.class));
    assertEquals(list.get(1), TestReplaceClassSerialization.class);
    serDeCheckSerializer(fury, TestClassSerialization.class, "ClassSerializer");
    serDeCheckSerializer(fury, TestReplaceClassSerialization.class, "ClassSerializer");
    serDe(fury, new TestReplaceClassSerialization());
  }

  @Test
  public void testEmptyObject() {
    Fury fury = Fury.builder().requireClassRegistration(true).build();
    assertSame(serDe(fury, new Object()).getClass(), Object.class);
  }
}
