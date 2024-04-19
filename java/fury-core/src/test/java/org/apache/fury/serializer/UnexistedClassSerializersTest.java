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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.List;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.FuryBuilder;
import org.apache.fury.config.Language;
import org.apache.fury.resolver.MetaContext;
import org.apache.fury.test.bean.Struct;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class UnexistedClassSerializersTest extends FuryTestBase {
  @DataProvider
  public static Object[][] config() {
    return Sets.cartesianProduct(
            ImmutableSet.of(true, false), // referenceTracking
            ImmutableSet.of(true, false), // fury1 enable codegen
            ImmutableSet.of(true, false) // fury2 enable codegen
            )
        .stream()
        .map(List::toArray)
        .toArray(Object[][]::new);
  }

  @DataProvider
  public static Object[][] metaShareConfig() {
    return Sets.cartesianProduct(
            ImmutableSet.of(true, false), // referenceTracking
            ImmutableSet.of(true, false), // fury1 enable codegen
            ImmutableSet.of(true, false), // fury2 enable codegen
            ImmutableSet.of(true, false)) // fury3 enable codegen
        .stream()
        .map(List::toArray)
        .toArray(Object[][]::new);
  }

  private FuryBuilder furyBuilder() {
    return Fury.builder()
        .withLanguage(Language.JAVA)
        .withCompatibleMode(CompatibleMode.COMPATIBLE)
        .requireClassRegistration(false)
        .withDeserializeUnexistedClass(true);
  }

  @Test(dataProvider = "config")
  public void testSkipUnexisted(
      boolean referenceTracking, boolean enableCodegen1, boolean enableCodegen2) {
    Fury fury =
        furyBuilder()
            .withRefTracking(referenceTracking)
            .withCodegen(enableCodegen1)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .build();
    ClassLoader classLoader = getClass().getClassLoader();
    for (Class<?> structClass :
        new Class<?>[] {
          Struct.createNumberStructClass("TestSkipUnexistedClass1", 2),
          Struct.createStructClass("TestSkipUnexistedClass1", 2)
        }) {
      Object pojo = Struct.createPOJO(structClass);
      byte[] bytes = fury.serialize(pojo);
      Fury fury2 =
          furyBuilder()
              .withRefTracking(referenceTracking)
              .withCodegen(enableCodegen2)
              .withClassLoader(classLoader)
              .build();
      Object o = fury2.deserialize(bytes);
      assertEquals(o.getClass(), UnexistedClassSerializers.UnexistedSkipClass.class);
    }
  }

  @Test(dataProvider = "metaShareConfig")
  public void testDeserializeUnexistedNewFury(
      boolean referenceTracking,
      boolean enableCodegen1,
      boolean enableCodegen2,
      boolean enableCodegen3) {
    Fury fury =
        furyBuilder()
            .withRefTracking(referenceTracking)
            .withCodegen(enableCodegen1)
            .withMetaContextShare(true)
            .build();
    ClassLoader classLoader = getClass().getClassLoader();
    for (Class<?> structClass :
        new Class<?>[] {
          Struct.createNumberStructClass("TestSkipUnexistedClass2", 2),
          Struct.createStructClass("TestSkipUnexistedClass2", 2)
        }) {
      Object pojo = Struct.createPOJO(structClass);
      MetaContext context1 = new MetaContext();
      fury.getSerializationContext().setMetaContext(context1);
      byte[] bytes = fury.serialize(pojo);
      Fury fury2 =
          furyBuilder()
              .withRefTracking(referenceTracking)
              .withCodegen(enableCodegen2)
              .withMetaContextShare(true)
              .withClassLoader(classLoader)
              .build();
      MetaContext context2 = new MetaContext();
      fury2.getSerializationContext().setMetaContext(context2);
      Object o2 = fury2.deserialize(bytes);
      assertEquals(o2.getClass(), UnexistedClassSerializers.UnexistedMetaSharedClass.class);
      fury2.getSerializationContext().setMetaContext(context2);
      byte[] bytes2 = fury2.serialize(o2);
      Fury fury3 =
          furyBuilder()
              .withRefTracking(referenceTracking)
              .withCodegen(enableCodegen3)
              .withMetaContextShare(true)
              .withClassLoader(pojo.getClass().getClassLoader())
              .build();
      MetaContext context3 = new MetaContext();
      fury3.getSerializationContext().setMetaContext(context3);
      Object o3 = fury3.deserialize(bytes2);
      assertEquals(o3.getClass(), structClass);
      assertEquals(o3, pojo);
    }
  }

  @Test(dataProvider = "metaShareConfig")
  public void testDeserializeUnexisted(
      boolean referenceTracking,
      boolean enableCodegen1,
      boolean enableCodegen2,
      boolean enableCodegen3) {
    Fury fury =
        furyBuilder()
            .withRefTracking(referenceTracking)
            .withCodegen(enableCodegen1)
            .withMetaContextShare(true)
            .build();
    MetaContext context1 = new MetaContext();
    MetaContext context2 = new MetaContext();
    MetaContext context3 = new MetaContext();
    ClassLoader classLoader = getClass().getClassLoader();
    for (Class<?> structClass :
        new Class<?>[] {
          Struct.createNumberStructClass("TestSkipUnexistedClass3", 2),
          Struct.createStructClass("TestSkipUnexistedClass3", 2)
        }) {
      Fury fury2 =
          furyBuilder()
              .withRefTracking(referenceTracking)
              .withCodegen(enableCodegen2)
              .withMetaContextShare(true)
              .withClassLoader(classLoader)
              .build();
      Fury fury3 =
          furyBuilder()
              .withRefTracking(referenceTracking)
              .withCodegen(enableCodegen3)
              .withMetaContextShare(true)
              .withClassLoader(structClass.getClassLoader())
              .build();
      for (int i = 0; i < 2; i++) {
        Object pojo = Struct.createPOJO(structClass);
        fury.getSerializationContext().setMetaContext(context1);
        byte[] bytes = fury.serialize(pojo);

        fury2.getSerializationContext().setMetaContext(context2);
        Object o2 = fury2.deserialize(bytes);
        assertEquals(o2.getClass(), UnexistedClassSerializers.UnexistedMetaSharedClass.class);
        fury2.getSerializationContext().setMetaContext(context2);
        byte[] bytes2 = fury2.serialize(o2);

        fury3.getSerializationContext().setMetaContext(context3);
        Object o3 = fury3.deserialize(bytes2);
        assertEquals(o3.getClass(), structClass);
        assertEquals(o3, pojo);
      }
    }
  }

  @Test
  public void testThrowExceptionIfClassNotExist() {
    Fury fury = furyBuilder().withDeserializeUnexistedClass(false).build();
    ClassLoader classLoader = getClass().getClassLoader();
    Class<?> structClass = Struct.createNumberStructClass("TestSkipUnexistedClass1", 2);
    Object pojo = Struct.createPOJO(structClass);
    Fury fury2 =
        furyBuilder().withDeserializeUnexistedClass(false).withClassLoader(classLoader).build();
    byte[] bytes = fury.serialize(pojo);
    Assert.assertThrows(RuntimeException.class, () -> fury2.deserialize(bytes));
  }
}
