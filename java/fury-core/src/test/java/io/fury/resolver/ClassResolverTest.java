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

package io.fury.resolver;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import io.fury.Fury;
import io.fury.FuryTestBase;
import io.fury.Language;
import io.fury.memory.MemoryBuffer;
import io.fury.memory.MemoryUtils;
import io.fury.resolver.longlongpkg.C1;
import io.fury.resolver.longlongpkg.C2;
import io.fury.resolver.longlongpkg.C3;
import io.fury.serializer.Serializer;
import io.fury.serializer.Serializers;
import io.fury.type.TypeUtils;
import io.fury.util.LoggerFactory;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.slf4j.Logger;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClassResolverTest extends FuryTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ClassResolverTest.class);

  public abstract static class A implements List {}

  public abstract static class B implements Map {}

  @Test
  public void testPrimitivesClassId() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).disableSecureMode().build();
    ClassResolver classResolver = fury.getClassResolver();
    for (List<Class<?>> classes :
        ImmutableList.of(
            TypeUtils.getSortedPrimitiveClasses(), TypeUtils.getSortedBoxedClasses())) {
      for (int i = 0; i < classes.size() - 1; i++) {
        assertEquals(
            classResolver.getRegisteredClassId(classes.get(i)) + 1,
            classResolver.getRegisteredClassId(classes.get(i + 1)).shortValue());
        assertTrue(classResolver.getRegisteredClassId(classes.get(i)) > 0);
      }
      assertEquals(
          classResolver.getRegisteredClassId(classes.get(classes.size() - 2)) + 1,
          classResolver.getRegisteredClassId(classes.get(classes.size() - 1)).shortValue());
      assertTrue(classResolver.getRegisteredClassId(classes.get(classes.size() - 1)) > 0);
    }
  }

  @Test
  public void testRegisterClass() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).disableSecureMode().build();
    ClassResolver classResolver = fury.getClassResolver();
    classResolver.register(io.fury.test.bean.Foo.class);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> classResolver.register(io.fury.test.bean.Foo.createCompatibleClass1()));
  }

  @Test
  public void testWriteClassName() {
    {
      Fury fury =
          Fury.builder()
              .withLanguage(Language.JAVA)
              .withReferenceTracking(true)
              .disableSecureMode()
              .build();
      ClassResolver classResolver = fury.getClassResolver();
      MemoryBuffer buffer = MemoryUtils.buffer(32);
      classResolver.writeClassInternal(buffer, getClass());
      int writerIndex = buffer.writerIndex();
      classResolver.writeClassInternal(buffer, getClass());
      Assert.assertEquals(buffer.writerIndex(), writerIndex + 7);
      buffer.writerIndex(0);
    }
  }

  @Test
  public void testWriteClassNamesInSamePackage() {
    Fury fury = Fury.builder().withClassRegistrationRequired(false).build();
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
    fury.writeReferencableToJava(buffer, C1.class);
    fury.writeReferencableToJava(buffer, C2.class);
    fury.writeReferencableToJava(buffer, C3.class);
    int len1 = C1.class.getName().getBytes(StandardCharsets.UTF_8).length;
    LOG.info("SomeClass1 {}", len1);
    LOG.info("buffer.writerIndex {}", buffer.writerIndex());
    Assert.assertTrue(buffer.writerIndex() < (3 + 8 + 3 + len1) * 3);
  }

  @Data
  static class Foo {
    int f1;
  }

  @EqualsAndHashCode(callSuper = true)
  @ToString
  static class Bar extends Foo {
    long f2;
  }

  private enum TestNeedToWriteReferenceClass {
    A,
    B
  }

  @Test
  public void testNeedToWriteReference() {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withReferenceTracking(true)
            .disableSecureMode()
            .build();
    ClassResolver classResolver = fury.getClassResolver();
    Assert.assertFalse(classResolver.needToWriteReference(TestNeedToWriteReferenceClass.class));
    assertNull(classResolver.getClassInfo(TestNeedToWriteReferenceClass.class, false));
  }

  private static class ErrorSerializer extends Serializer<Foo> {
    public ErrorSerializer(Fury fury) {
      super(fury, Foo.class);
      fury.getClassResolver().setSerializer(Foo.class, this);
      throw new RuntimeException();
    }
  }

  @Test
  public void testResetSerializer() {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withReferenceTracking(true)
            .disableSecureMode()
            .build();
    ClassResolver classResolver = fury.getClassResolver();
    Assert.assertThrows(() -> Serializers.newSerializer(fury, Foo.class, ErrorSerializer.class));
    Assert.assertNull(classResolver.getSerializer(Foo.class, false));
    Assert.assertThrows(
        () -> classResolver.createSerializerSafe(Foo.class, () -> new ErrorSerializer(fury)));
    Assert.assertNull(classResolver.getSerializer(Foo.class, false));
  }

  @Test
  public void testPrimitive() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).build();
    ClassResolver classResolver = fury.getClassResolver();
    Assert.assertTrue(classResolver.isPrimitive(classResolver.getRegisteredClassId(void.class)));
    Assert.assertTrue(classResolver.isPrimitive(classResolver.getRegisteredClassId(boolean.class)));
    Assert.assertTrue(classResolver.isPrimitive(classResolver.getRegisteredClassId(byte.class)));
    Assert.assertTrue(classResolver.isPrimitive(classResolver.getRegisteredClassId(short.class)));
    Assert.assertTrue(classResolver.isPrimitive(classResolver.getRegisteredClassId(char.class)));
    Assert.assertTrue(classResolver.isPrimitive(classResolver.getRegisteredClassId(int.class)));
    Assert.assertTrue(classResolver.isPrimitive(classResolver.getRegisteredClassId(long.class)));
    Assert.assertTrue(classResolver.isPrimitive(classResolver.getRegisteredClassId(float.class)));
    Assert.assertTrue(classResolver.isPrimitive(classResolver.getRegisteredClassId(double.class)));
    Assert.assertFalse(classResolver.isPrimitive(classResolver.getRegisteredClassId(String.class)));
    // Assert.assertFalse(classResolver.isPrimitive(classResolver.getRegisteredClassId(Date.class)));
  }
}
