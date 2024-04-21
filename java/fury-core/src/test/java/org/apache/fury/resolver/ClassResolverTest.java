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

package org.apache.fury.resolver;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.builder.Generated;
import org.apache.fury.config.Language;
import org.apache.fury.logging.Logger;
import org.apache.fury.logging.LoggerFactory;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.resolver.longlongpkg.C1;
import org.apache.fury.resolver.longlongpkg.C2;
import org.apache.fury.resolver.longlongpkg.C3;
import org.apache.fury.serializer.CompatibleSerializer;
import org.apache.fury.serializer.ObjectSerializer;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.serializer.Serializers;
import org.apache.fury.serializer.collection.CollectionSerializer;
import org.apache.fury.serializer.collection.CollectionSerializers;
import org.apache.fury.serializer.collection.MapSerializers;
import org.apache.fury.test.bean.BeanB;
import org.apache.fury.type.TypeUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClassResolverTest extends FuryTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ClassResolverTest.class);

  @Test
  public void testPrimitivesClassId() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
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
    Fury fury = Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
    ClassResolver classResolver = fury.getClassResolver();
    classResolver.register(org.apache.fury.test.bean.Foo.class);
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> classResolver.register(org.apache.fury.test.bean.Foo.class, 100));
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> classResolver.register(org.apache.fury.test.bean.Foo.createCompatibleClass1()));
    classResolver.register(Interface1.class, 200);
    Assert.assertThrows(
        IllegalArgumentException.class, () -> classResolver.register(Interface2.class, 200));
  }

  @Test
  public void testGetSerializerClass() throws ClassNotFoundException {
    {
      Fury fury =
          Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
      // serialize class first will create a class info with serializer null.
      serDeCheck(fury, BeanB.class);
      Assert.assertTrue(
          Generated.GeneratedSerializer.class.isAssignableFrom(
              fury.getClassResolver().getSerializerClass(BeanB.class)));
      // ensure serialize class first won't make object fail to serialize.
      serDeCheck(fury, BeanB.createBeanB(2));
    }
    {
      Fury fury =
          Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
      serDeCheck(fury, new Object[] {BeanB.class, BeanB.createBeanB(2)});
    }
    Fury fury = Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
    ClassResolver classResolver = fury.getClassResolver();
    assertEquals(
        classResolver.getSerializerClass(ArrayList.class),
        CollectionSerializers.ArrayListSerializer.class);
    assertEquals(
        classResolver.getSerializerClass(Arrays.asList(1, 2).getClass()),
        CollectionSerializers.ArraysAsListSerializer.class);
    assertEquals(classResolver.getSerializerClass(LinkedList.class), CollectionSerializer.class);

    assertEquals(
        classResolver.getSerializerClass(HashSet.class),
        CollectionSerializers.HashSetSerializer.class);
    assertEquals(
        classResolver.getSerializerClass(LinkedHashSet.class),
        CollectionSerializers.LinkedHashSetSerializer.class);
    assertEquals(
        classResolver.getSerializerClass(TreeSet.class),
        CollectionSerializers.SortedSetSerializer.class);

    assertEquals(
        classResolver.getSerializerClass(HashMap.class), MapSerializers.HashMapSerializer.class);
    assertEquals(
        classResolver.getSerializerClass(LinkedHashMap.class),
        MapSerializers.LinkedHashMapSerializer.class);
    assertEquals(
        classResolver.getSerializerClass(TreeMap.class), MapSerializers.SortedMapSerializer.class);

    if (ClassResolver.requireJavaSerialization(ArrayBlockingQueue.class)) {
      assertEquals(
          classResolver.getSerializerClass(ArrayBlockingQueue.class),
          CollectionSerializers.JDKCompatibleCollectionSerializer.class);
    } else {
      assertEquals(
          classResolver.getSerializerClass(ArrayBlockingQueue.class),
          CollectionSerializers.DefaultJavaCollectionSerializer.class);
    }
    assertEquals(
        classResolver.getSerializerClass(ConcurrentHashMap.class),
        MapSerializers.ConcurrentHashMapSerializer.class);
    assertEquals(
        classResolver.getSerializerClass(
            Class.forName("org.apache.fury.serializer.collection.CollectionContainer")),
        CollectionSerializers.DefaultJavaCollectionSerializer.class);
    assertEquals(
        classResolver.getSerializerClass(
            Class.forName("org.apache.fury.serializer.collection.MapContainer")),
        MapSerializers.DefaultJavaMapSerializer.class);
  }

  interface Interface1 {}

  interface Interface2 {}

  @Test
  public void testSerializeClassesShared() {
    Fury fury = builder().build();
    serDeCheck(fury, Foo.class);
    serDeCheck(fury, Arrays.asList(Foo.class, Foo.class));
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testSerializeClasses(boolean referenceTracking) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTracking)
            .requireClassRegistration(false)
            .build();
    Primitives.allPrimitiveTypes()
        .forEach(cls -> assertSame(cls, fury.deserialize(fury.serialize(cls))));
    Primitives.allWrapperTypes()
        .forEach(cls -> assertSame(cls, fury.deserialize(fury.serialize(cls))));
    assertSame(Class.class, fury.deserialize(fury.serialize(Class.class)));
    assertSame(Fury.class, fury.deserialize(fury.serialize(Fury.class)));
    List<Class<?>> classes =
        Arrays.asList(getClass(), getClass(), Foo.class, Foo.class, Bar.class, Bar.class);
    serDeCheck(fury, classes);
    serDeCheck(
        fury,
        Arrays.asList(Interface1.class, Interface1.class, Interface2.class, Interface2.class));
  }

  @Test
  public void testWriteClassName() {
    {
      Fury fury =
          Fury.builder()
              .withLanguage(Language.JAVA)
              .withRefTracking(true)
              .requireClassRegistration(false)
              .build();
      ClassResolver classResolver = fury.getClassResolver();
      MemoryBuffer buffer = MemoryUtils.buffer(32);
      classResolver.writeClassInternal(buffer, getClass());
      int writerIndex = buffer.writerIndex();
      classResolver.writeClassInternal(buffer, getClass());
      Assert.assertEquals(buffer.writerIndex(), writerIndex + 2);
      buffer.writerIndex(0);
    }
    {
      Fury fury =
          Fury.builder()
              .withLanguage(Language.JAVA)
              .withRefTracking(true)
              .requireClassRegistration(false)
              .build();
      ClassResolver classResolver = fury.getClassResolver();
      MemoryBuffer buffer = MemoryUtils.buffer(32);
      classResolver.writeClassAndUpdateCache(buffer, getClass());
      classResolver.writeClassAndUpdateCache(buffer, getClass());
      Assert.assertSame(classResolver.readClassInfo(buffer).getCls(), getClass());
      Assert.assertSame(classResolver.readClassInfo(buffer).getCls(), getClass());
      classResolver.reset();
      buffer.writerIndex(0);
      buffer.readerIndex(0);
      List<org.apache.fury.test.bean.Foo> fooList =
          Arrays.asList(
              org.apache.fury.test.bean.Foo.create(), org.apache.fury.test.bean.Foo.create());
      Assert.assertEquals(fury.deserialize(fury.serialize(fooList)), fooList);
      Assert.assertEquals(fury.deserialize(fury.serialize(fooList)), fooList);
    }
  }

  @Test
  public void testWriteClassNamesInSamePackage() {
    Fury fury = Fury.builder().requireClassRegistration(false).build();
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
    fury.writeRef(buffer, C1.class);
    fury.writeRef(buffer, C2.class);
    fury.writeRef(buffer, C3.class);
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

  @Test
  public void testClassRegistrationInit() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).withCodegen(false).build();
    serDeCheck(fury, new HashMap<>(ImmutableMap.of("a", 1, "b", 2)));
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
            .withRefTracking(true)
            .requireClassRegistration(false)
            .build();
    ClassResolver classResolver = fury.getClassResolver();
    Assert.assertFalse(classResolver.needToWriteRef(TestNeedToWriteReferenceClass.class));
    assertNull(classResolver.getClassInfo(TestNeedToWriteReferenceClass.class, false));
  }

  @Test
  public void testSetSerializer() {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .build();
    ClassResolver classResolver = fury.getClassResolver();
    {
      classResolver.setSerializer(Foo.class, new ObjectSerializer<>(fury, Foo.class));
      ClassInfo classInfo = classResolver.getClassInfo(Foo.class);
      assertSame(classInfo.getSerializer().getClass(), ObjectSerializer.class);
      classResolver.setSerializer(Foo.class, new CompatibleSerializer<>(fury, Foo.class));
      Assert.assertSame(classResolver.getClassInfo(Foo.class), classInfo);
      assertSame(classInfo.getSerializer().getClass(), CompatibleSerializer.class);
    }
    {
      classResolver.register(Bar.class);
      ClassInfo classInfo = classResolver.getClassInfo(Bar.class);
      classResolver.setSerializer(Bar.class, new ObjectSerializer<>(fury, Bar.class));
      Assert.assertSame(classResolver.getClassInfo(Bar.class), classInfo);
      assertSame(classInfo.getSerializer().getClass(), ObjectSerializer.class);
      classResolver.setSerializer(Bar.class, new CompatibleSerializer<>(fury, Bar.class));
      Assert.assertSame(classResolver.getClassInfo(Bar.class), classInfo);
      assertSame(classInfo.getSerializer().getClass(), CompatibleSerializer.class);
    }
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
            .withRefTracking(true)
            .requireClassRegistration(false)
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
    Assert.assertFalse(classResolver.isPrimitive(classResolver.getRegisteredClassId(Date.class)));
  }
}
