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

package io.fury;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import io.fury.memory.MemoryBuffer;
import io.fury.memory.MemoryUtils;
import io.fury.serializer.SerializersTest;
import io.fury.test.bean.BeanA;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class FuryTest extends FuryTestBase {
  @DataProvider(name = "languageConfig")
  public static Object[] languageConfig() {
    return new Object[] {Language.JAVA, Language.PYTHON};
  }

  @Test(dataProvider = "crossLanguageReferenceTrackingConfig")
  public void primitivesTest(boolean referenceTracking, Language language) {
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
    assertEquals(true, serDe(fury1, fury2, true));
    assertEquals(Byte.MAX_VALUE, serDe(fury1, fury2, Byte.MAX_VALUE));
    assertEquals(Short.MAX_VALUE, serDe(fury1, fury2, Short.MAX_VALUE));
    assertEquals(Integer.MAX_VALUE, serDe(fury1, fury2, Integer.MAX_VALUE));
    assertEquals(Long.MAX_VALUE, serDe(fury1, fury2, Long.MAX_VALUE));
    assertEquals(Float.MAX_VALUE, serDe(fury1, fury2, Float.MAX_VALUE));
    assertEquals(Double.MAX_VALUE, serDe(fury1, fury2, Double.MAX_VALUE));
  }

  @Test(dataProvider = "languageConfig")
  public void testSerializationToBuffer(Language language) {
    Fury fury1 = Fury.builder().withLanguage(language).disableSecureMode().build();
    Fury fury2 = Fury.builder().withLanguage(language).disableSecureMode().build();
    MemoryBuffer buffer = MemoryUtils.buffer(64);
    assertSerializationToBuffer(fury1, fury2, buffer);
  }

  @Test(dataProvider = "languageConfig")
  public void testSerializationSlicedBuffer(Language language) {
    Fury fury1 = Fury.builder().withLanguage(language).disableSecureMode().build();
    Fury fury2 = Fury.builder().withLanguage(language).disableSecureMode().build();
    MemoryBuffer buffer0 = MemoryUtils.buffer(64);
    buffer0.writeLong(-1);
    buffer0.writeLong(-1);
    buffer0.readLong();
    buffer0.readLong();
    MemoryBuffer buffer = buffer0.slice(8);
    assertSerializationToBuffer(fury1, fury2, buffer);
  }

  public void assertSerializationToBuffer(Fury fury1, Fury fury2, MemoryBuffer buffer) {
    assertEquals(true, serDeCheckIndex(fury1, fury2, buffer, true));
    assertEquals(Byte.MAX_VALUE, serDeCheckIndex(fury1, fury2, buffer, Byte.MAX_VALUE));
    assertEquals(Short.MAX_VALUE, serDeCheckIndex(fury1, fury2, buffer, Short.MAX_VALUE));
    assertEquals("str", serDeCheckIndex(fury1, fury2, buffer, "str"));
    assertEquals("str", serDeCheckIndex(fury1, fury2, buffer, new StringBuilder("str")).toString());
    assertEquals(
        SerializersTest.EnumFoo.A,
        serDeCheckIndex(fury1, fury2, buffer, SerializersTest.EnumFoo.A));
    assertEquals(
        SerializersTest.EnumSubClass.A,
        serDeCheckIndex(fury1, fury2, buffer, SerializersTest.EnumSubClass.A));
    assertTrue(
        Arrays.equals(
            new boolean[] {false, true},
            (boolean[]) serDeCheckIndex(fury1, fury2, buffer, new boolean[] {false, true})));
    assertEquals(
        new byte[] {1, 1}, (byte[]) serDeCheckIndex(fury1, fury2, buffer, new byte[] {1, 1}));
    assertEquals(Arrays.asList(1, 2), serDe(fury1, fury2, buffer, Arrays.asList(1, 2)));
  }

  @Test(dataProvider = "enableCodegen")
  public void testSerializeJDKObject(boolean enableCodegen) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withJdkClassSerializableCheck(false)
            .disableSecureMode()
            .withCodegen(enableCodegen)
            .build();
    StringTokenizer tokenizer = new StringTokenizer("abc,1,23", ",");
    assertEquals(serDe(fury, tokenizer).countTokens(), tokenizer.countTokens());
  }

  @Test
  public void testSerializeJavaObject() {
    Fury fury =
        Fury.builder().withClassRegistrationRequired(false).withLanguage(Language.JAVA).build();
    BeanA beanA = BeanA.createBeanA(2);
    assertEquals(fury.deserializeJavaObject(fury.serializeJavaObject(beanA), BeanA.class), beanA);
    assertThrows(
        Exception.class,
        () -> fury.deserializeJavaObject(fury.serializeJavaObjectAndClass(beanA), BeanA.class));
    assertEquals(
        fury.deserializeJavaObjectAndClass(fury.serializeJavaObjectAndClass(beanA)), beanA);
    assertEquals(
        fury.deserializeJavaObjectAndClass(
            MemoryBuffer.fromByteArray(fury.serializeJavaObjectAndClass(beanA))),
        beanA);
  }

  @Test
  public void testOutputStream() throws IOException {
    Fury fury = Fury.builder().requireClassRegistration(false).build();
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    BeanA beanA = BeanA.createBeanA(2);
    fury.serialize(bas, beanA);
    fury.serialize(bas, beanA);
    bas.flush();
    ByteArrayInputStream bis = new ByteArrayInputStream(bas.toByteArray());
    Object newObj = fury.deserialize(bis);
    assertEquals(newObj, beanA);
    newObj = fury.deserialize(bis);
    assertEquals(newObj, beanA);
  }

  @Test
  public void testJavaOutputStream() throws IOException {
    Fury fury = Fury.builder().requireClassRegistration(false).build();
    BeanA beanA = BeanA.createBeanA(2);
    {
      ByteArrayOutputStream bas = new ByteArrayOutputStream();
      fury.serializeJavaObject(bas, beanA);
      fury.serializeJavaObject(bas, beanA);
      bas.flush();
      ByteArrayInputStream bis = new ByteArrayInputStream(bas.toByteArray());
      Object newObj = fury.deserializeJavaObject(bis, BeanA.class);
      assertEquals(newObj, beanA);
      newObj = fury.deserializeJavaObject(bis, BeanA.class);
      assertEquals(newObj, beanA);
    }
    {
      ByteArrayOutputStream bas = new ByteArrayOutputStream();
      fury.serializeJavaObjectAndClass(bas, beanA);
      fury.serializeJavaObjectAndClass(bas, beanA);
      bas.flush();
      ByteArrayInputStream bis = new ByteArrayInputStream(bas.toByteArray());
      Object newObj = fury.deserializeJavaObjectAndClass(bis);
      assertEquals(newObj, beanA);
      newObj = fury.deserializeJavaObjectAndClass(bis);
      assertEquals(newObj, beanA);
    }
  }
}
