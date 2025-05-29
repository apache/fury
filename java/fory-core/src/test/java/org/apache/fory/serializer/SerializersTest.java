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
import org.apache.fory.Fory;
import org.apache.fory.ForyTestBase;
import org.apache.fory.config.ForyBuilder;
import org.apache.fory.config.Language;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SerializersTest extends ForyTestBase {

  @Test(dataProvider = "crossLanguageReferenceTrackingConfig")
  public void testStringBuilder(boolean referenceTracking, Language language) {
    ForyBuilder builder =
        Fory.builder()
            .withLanguage(language)
            .withRefTracking(referenceTracking)
            .requireClassRegistration(false);
    Fory fory1 = builder.build();
    Fory fory2 = builder.build();
    assertEquals("str", serDe(fory1, fory2, "str"));
    assertEquals("str", serDeObject(fory1, fory2, new StringBuilder("str")).toString());
    assertEquals("str", serDeObject(fory1, fory2, new StringBuffer("str")).toString());
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testBigInt(boolean referenceTracking) {
    ForyBuilder builder =
        Fory.builder().withRefTracking(referenceTracking).requireClassRegistration(false);
    Fory fory1 = builder.build();
    Fory fory2 = builder.build();
    assertEquals(BigInteger.valueOf(100), serDe(fory1, fory2, BigInteger.valueOf(100)));
    assertEquals(BigDecimal.valueOf(100, 2), serDe(fory1, fory2, BigDecimal.valueOf(100, 2)));
    BigInteger bigInteger = new BigInteger("999999999999999999999999999999999999999999999999");
    BigDecimal bigDecimal = new BigDecimal(bigInteger, 200, MathContext.DECIMAL128);
    serDeCheck(fory1, bigDecimal);
    serDeCheck(
        fory1, new BigInteger("11111111110101010000283895380202208220050200000000111111111"));
  }

  @Test(dataProvider = "javaFury")
  public void testAtomic(Fory fory) {
    assertTrue(
        ((AtomicBoolean) serDeCheckSerializer(fory, new AtomicBoolean(true), "AtomicBoolean"))
            .get());

    Assert.assertEquals(
        ((AtomicInteger) serDeCheckSerializer(fory, new AtomicInteger(100), "AtomicInteger")).get(),
        100);
    Assert.assertEquals(
        ((AtomicLong) serDeCheckSerializer(fory, new AtomicLong(200), "AtomicLong")).get(), 200);
    Assert.assertEquals(
        ((AtomicReference)
                serDeCheckSerializer(fory, new AtomicReference<>(200), "AtomicReference"))
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
    Fory fory = Fory.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
    Assert.assertEquals(
        serDeCheckSerializer(fory, Charset.defaultCharset(), "Charset"), Charset.defaultCharset());
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
    Fory fory = Fory.builder().requireClassRegistration(false).build();
    // serialize both TestReplaceClassSerialization object and class.
    // Scala `object` native serialization will return ModuleSerializationProxy will write original
    // class.
    List<Object> list =
        serDe(
            fory,
            Arrays.asList(
                new TestReplaceClassSerialization(), TestReplaceClassSerialization.class));
    assertEquals(list.get(1), TestReplaceClassSerialization.class);
    serDeCheckSerializer(fory, TestClassSerialization.class, "ClassSerializer");
    serDeCheckSerializer(fory, TestReplaceClassSerialization.class, "ClassSerializer");
    serDe(fory, new TestReplaceClassSerialization());
  }

  @Test
  public void testEmptyObject() {
    Fory fory = Fory.builder().requireClassRegistration(true).build();
    assertSame(serDe(fory, new Object()).getClass(), Object.class);
  }
}
