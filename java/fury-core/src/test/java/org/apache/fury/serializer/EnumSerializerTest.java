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
import org.apache.fury.codegen.JaninoUtils;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.FuryBuilder;
import org.apache.fury.config.Language;
import org.apache.fury.exception.DeserializationException;
import org.testng.annotations.Test;

public class EnumSerializerTest extends FuryTestBase {

  @Test
  public void testWrite() {}

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
    if (fury1.getLanguage() != Language.JAVA) {
      fury1.register(EnumSerializerTest.EnumFoo.class);
      fury2.register(EnumSerializerTest.EnumFoo.class);
      fury1.register(EnumSerializerTest.EnumSubClass.class);
      fury2.register(EnumSerializerTest.EnumSubClass.class);
    }
    assertEquals(EnumFoo.A, serDe(fury1, fury2, EnumFoo.A));
    assertEquals(EnumFoo.B, serDe(fury1, fury2, EnumFoo.B));
    assertEquals(EnumSubClass.A, serDe(fury1, fury2, EnumSubClass.A));
    assertEquals(EnumSubClass.B, serDe(fury1, fury2, EnumSubClass.B));
  }

  @Test(dataProvider = "furyCopyConfig")
  public void testEnumSerializer(Fury fury) {
    copyCheckWithoutSame(fury, EnumFoo.A);
    copyCheckWithoutSame(fury, EnumFoo.B);
    copyCheckWithoutSame(fury, EnumSubClass.A);
    copyCheckWithoutSame(fury, EnumSubClass.B);
  }

  @Test
  public void testEnumSerializationUnexistentEnumValueAsNull() {
    String enumCode2 = "enum TestEnum2 {" + " A;" + "}";
    String enumCode1 = "enum TestEnum2 {" + " A, B" + "}";
    Class<?> cls1 =
        JaninoUtils.compileClass(getClass().getClassLoader(), "", "TestEnum2", enumCode1);
    Class<?> cls2 =
        JaninoUtils.compileClass(getClass().getClassLoader(), "", "TestEnum2", enumCode2);
    FuryBuilder builderSerialization =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .requireClassRegistration(false);
    FuryBuilder builderDeserialize =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .deserializeNonexistentEnumValueAsNull(true)
            .withClassLoader(cls2.getClassLoader());
    Fury furyDeserialize = builderDeserialize.build();
    Fury furySerialization = builderSerialization.build();
    byte[] bytes = furySerialization.serialize(cls1.getEnumConstants()[1]);
    Object data = furyDeserialize.deserialize(bytes);
    assertNull(data);
  }

  @Test
  public void testEnumSerializationAsString() {
    String enumCode1 = "enum TestEnum1 {" + " A, B;" + "}";
    String enumCode2 = "enum TestEnum1 {" + " B;" + "}";
    Class<?> cls1 =
        JaninoUtils.compileClass(getClass().getClassLoader(), "", "TestEnum1", enumCode1);
    Class<?> cls2 =
        JaninoUtils.compileClass(getClass().getClassLoader(), "", "TestEnum1", enumCode2);

    Fury furyDeserialize =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .serializeEnumByName(true)
            .withAsyncCompilation(false)
            .build();
    Fury furySerialization =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .serializeEnumByName(true)
            .withAsyncCompilation(false)
            .build();

    // serialize enum "B"
    furySerialization.register(cls1);
    byte[] bytes = furySerialization.serializeJavaObject(cls1.getEnumConstants()[1]);

    furyDeserialize.register(cls2);
    Object data = furyDeserialize.deserializeJavaObject(bytes, cls2);
    assertEquals(cls2.getEnumConstants()[0], data);
  }

  @Test
  public void testEnumSerializationAsString_differentClass() {
    String enumCode1 = "enum TestEnum1 {" + " A, B;" + "}";
    String enumCode2 = "enum TestEnum2 {" + " B;" + "}";
    Class<?> cls1 =
        JaninoUtils.compileClass(getClass().getClassLoader(), "", "TestEnum1", enumCode1);
    Class<?> cls2 =
        JaninoUtils.compileClass(getClass().getClassLoader(), "", "TestEnum2", enumCode2);

    Fury furyDeserialize =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .serializeEnumByName(true)
            .withAsyncCompilation(false)
            .build();
    Fury furySerialization =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .serializeEnumByName(true)
            .withAsyncCompilation(false)
            .build();

    // serialize enum "B"
    furySerialization.register(cls1);
    byte[] bytes = furySerialization.serializeJavaObject(cls1.getEnumConstants()[1]);

    furyDeserialize.register(cls2);
    Object data = furyDeserialize.deserializeJavaObject(bytes, cls2);
    assertEquals(cls2.getEnumConstants()[0], data);
  }

  @Test
  public void testEnumSerializationAsString_invalidEnum() {
    String enumCode1 = "enum TestEnum1 {" + " A;" + "}";
    String enumCode2 = "enum TestEnum2 {" + " B;" + "}";
    Class<?> cls1 =
        JaninoUtils.compileClass(getClass().getClassLoader(), "", "TestEnum1", enumCode1);
    Class<?> cls2 =
        JaninoUtils.compileClass(getClass().getClassLoader(), "", "TestEnum2", enumCode2);

    Fury furyDeserialize =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .serializeEnumByName(true)
            .withAsyncCompilation(false)
            .build();
    Fury furySerialization =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .serializeEnumByName(true)
            .withAsyncCompilation(false)
            .build();

    furySerialization.register(cls1);
    byte[] bytes = furySerialization.serializeJavaObject(cls1.getEnumConstants()[0]);

    try {
      furyDeserialize.register(cls2);
      furyDeserialize.deserializeJavaObject(bytes, cls2);
      fail("expected to throw exception");
    } catch (Exception e) {
      assertTrue(e instanceof DeserializationException);
      assertTrue(e.getCause() instanceof IllegalArgumentException);
    }
  }

  @Test
  public void testEnumSerializationAsString_nullValue() {
    String enumCode1 = "enum TestEnum1 {" + " A;" + "}";
    String enumCode2 = "enum TestEnum2 {" + " B;" + "}";
    Class<?> cls1 =
        JaninoUtils.compileClass(getClass().getClassLoader(), "", "TestEnum1", enumCode1);
    Class<?> cls2 =
        JaninoUtils.compileClass(getClass().getClassLoader(), "", "TestEnum2", enumCode2);

    Fury furyDeserialize =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .serializeEnumByName(true)
            .withAsyncCompilation(false)
            .build();
    Fury furySerialization =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .serializeEnumByName(true)
            .withAsyncCompilation(false)
            .build();

    byte[] bytes = furySerialization.serializeJavaObject(null);

    Object data = furyDeserialize.deserializeJavaObject(bytes, cls2);
    assertNull(data);
  }

  @Data
  @AllArgsConstructor
  static class EnumSubclassFieldTest {
    EnumSubClass subEnum;
  }

  @Test(dataProvider = "enableCodegen")
  public void testEnumSubclassField(boolean enableCodegen) {
    serDeCheck(
        builder().withCodegen(enableCodegen).build(), new EnumSubclassFieldTest(EnumSubClass.B));
  }

  @Test(dataProvider = "furyCopyConfig")
  public void testEnumSubclassField(Fury fury) {
    copyCheck(fury, new EnumSubclassFieldTest(EnumSubClass.B));
  }

  @Test(dataProvider = "scopedMetaShare")
  public void testEnumSubclassFieldCompatible(boolean scopedMetaShare) {
    serDeCheck(
        builder()
            .withScopedMetaShare(scopedMetaShare)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .build(),
        new EnumSubclassFieldTest(EnumSubClass.B));
  }
}
