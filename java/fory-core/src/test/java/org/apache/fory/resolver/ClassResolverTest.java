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

package org.apache.fory.resolver;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Primitives;
import java.nio.charset.StandardCharsets;
import java.util.AbstractList;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.fory.Fory;
import org.apache.fory.ForyTestBase;
import org.apache.fory.ThreadSafeFory;
import org.apache.fory.builder.Generated;
import org.apache.fory.config.Language;
import org.apache.fory.logging.Logger;
import org.apache.fory.logging.LoggerFactory;
import org.apache.fory.memory.MemoryBuffer;
import org.apache.fory.memory.MemoryUtils;
import org.apache.fory.reflect.TypeRef;
import org.apache.fory.resolver.longlongpkg.C1;
import org.apache.fory.resolver.longlongpkg.C2;
import org.apache.fory.resolver.longlongpkg.C3;
import org.apache.fory.serializer.CompatibleSerializer;
import org.apache.fory.serializer.ObjectSerializer;
import org.apache.fory.serializer.Serializer;
import org.apache.fory.serializer.Serializers;
import org.apache.fory.serializer.collection.AbstractCollectionSerializer;
import org.apache.fory.serializer.collection.AbstractMapSerializer;
import org.apache.fory.serializer.collection.CollectionSerializer;
import org.apache.fory.serializer.collection.CollectionSerializers;
import org.apache.fory.serializer.collection.MapSerializers;
import org.apache.fory.test.bean.BeanB;
import org.apache.fory.type.TypeUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClassResolverTest extends ForyTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ClassResolverTest.class);

  @Test
  public void testPrimitivesClassId() {
    Fory fory = Fory.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
    ClassResolver classResolver = fory.getClassResolver();
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
  public void testRegisterClassByName() {
    Fory fory = Fory.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build();
    ClassResolver classResolver = fory.getClassResolver();
    classResolver.register(C1.class, "ns", "C1");
    Assert.assertThrows(
        IllegalArgumentException.class, () -> classResolver.register(C1.class, "ns", "C1"));
    Assert.assertThrows(
        IllegalArgumentException.class, () -> classResolver.register(C1.class, 200));
    Assert.assertTrue(fory.serialize(C1.class).length < 12);
    serDeCheck(fory, C1.class);

    classResolver.register(C2.class, "", "C2");
    Assert.assertTrue(fory.serialize(C2.class).length < 12);
    serDeCheck(fory, C2.class);

    classResolver.register(Foo.class, "ns", "Foo");
    Foo foo = new Foo();
    foo.f1 = 10;
    serDeCheck(fory, foo);
  }

  @Test
  public void testRegisterClass() {
    Fory fory = Fory.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
  }

  @Test
  public void testGetSerializerClass() throws ClassNotFoundException {
    {
      Fory fory =
          Fory.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
      // serialize class first will create a class info with serializer null.
      serDeCheck(fory, BeanB.class);
      Assert.assertTrue(
          Generated.GeneratedSerializer.class.isAssignableFrom(
              fory.getClassResolver().getSerializerClass(BeanB.class)));
      // ensure serialize class first won't make object fail to serialize.
      serDeCheck(fory, BeanB.createBeanB(2));
    }
    {
      Fory fory =
          Fory.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
      serDeCheck(fory, new Object[] {BeanB.class, BeanB.createBeanB(2)});
    }
    Fory fory = Fory.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
    ClassResolver classResolver = fory.getClassResolver();
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
            Class.forName("org.apache.fory.serializer.collection.CollectionContainer")),
        CollectionSerializers.DefaultJavaCollectionSerializer.class);
    assertEquals(
        classResolver.getSerializerClass(
            Class.forName("org.apache.fory.serializer.collection.MapContainer")),
        MapSerializers.DefaultJavaMapSerializer.class);
  }

  interface Interface1 {}

  interface Interface2 {}

  @Test
  public void testSerializeClassesShared() {
    Fory fory = builder().build();
    serDeCheck(fory, Foo.class);
    serDeCheck(fory, Arrays.asList(Foo.class, Foo.class));
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testSerializeClasses(boolean referenceTracking) {
    Fory fory =
        Fory.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTracking)
            .requireClassRegistration(false)
            .build();
    Primitives.allPrimitiveTypes()
        .forEach(cls -> assertSame(cls, fory.deserialize(fory.serialize(cls))));
    Primitives.allWrapperTypes()
        .forEach(cls -> assertSame(cls, fory.deserialize(fory.serialize(cls))));
    assertSame(Class.class, fory.deserialize(fory.serialize(Class.class)));
    assertSame(Fory.class, fory.deserialize(fory.serialize(Fory.class)));
    List<Class<?>> classes =
        Arrays.asList(getClass(), getClass(), Foo.class, Foo.class, Bar.class, Bar.class);
    serDeCheck(fory, classes);
    serDeCheck(
        fory,
        Arrays.asList(Interface1.class, Interface1.class, Interface2.class, Interface2.class));
  }

  @Test
  public void testWriteClassName() {
    {
      Fory fory =
          Fory.builder()
              .withLanguage(Language.JAVA)
              .withRefTracking(true)
              .requireClassRegistration(false)
              .build();
      ClassResolver classResolver = fory.getClassResolver();
      MemoryBuffer buffer = MemoryUtils.buffer(32);
      classResolver.writeClassInternal(buffer, getClass());
      int writerIndex = buffer.writerIndex();
      classResolver.writeClassInternal(buffer, getClass());
      Assert.assertEquals(buffer.writerIndex(), writerIndex + 2);
      buffer.writerIndex(0);
    }
    {
      Fory fory =
          Fory.builder()
              .withLanguage(Language.JAVA)
              .withRefTracking(true)
              .requireClassRegistration(false)
              .build();
      ClassResolver classResolver = fory.getClassResolver();
      MemoryBuffer buffer = MemoryUtils.buffer(32);
      classResolver.writeClassAndUpdateCache(buffer, getClass());
      classResolver.writeClassAndUpdateCache(buffer, getClass());
      Assert.assertSame(classResolver.readClassInfo(buffer).getCls(), getClass());
      Assert.assertSame(classResolver.readClassInfo(buffer).getCls(), getClass());
      classResolver.reset();
      buffer.writerIndex(0);
      buffer.readerIndex(0);
      List<org.apache.fory.test.bean.Foo> fooList =
          Arrays.asList(
              org.apache.fory.test.bean.Foo.create(), org.apache.fory.test.bean.Foo.create());
      Assert.assertEquals(fory.deserialize(fory.serialize(fooList)), fooList);
      Assert.assertEquals(fory.deserialize(fory.serialize(fooList)), fooList);
    }
  }

  @Test
  public void testWriteClassNamesInSamePackage() {
    Fory fory = Fory.builder().requireClassRegistration(false).build();
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
    fory.writeRef(buffer, C1.class);
    fory.writeRef(buffer, C2.class);
    fory.writeRef(buffer, C3.class);
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
    Fory fory = Fory.builder().withLanguage(Language.JAVA).withCodegen(false).build();
    serDeCheck(fory, new HashMap<>(ImmutableMap.of("a", 1, "b", 2)));
  }

  private enum TestNeedToWriteReferenceClass {
    A,
    B
  }

  @Test
  public void testNeedToWriteReference() {
    Fory fory =
        Fory.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .build();
    ClassResolver classResolver = fory.getClassResolver();
    Assert.assertFalse(
        classResolver.needToWriteRef(TypeRef.of(TestNeedToWriteReferenceClass.class)));
    assertNull(classResolver.getClassInfo(TestNeedToWriteReferenceClass.class, false));
  }

  @Test
  public void testSetSerializer() {
    Fory fory =
        Fory.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .build();
    ClassResolver classResolver = fory.getClassResolver();
    {
      classResolver.setSerializer(Foo.class, new ObjectSerializer<>(fory, Foo.class));
      ClassInfo classInfo = classResolver.getClassInfo(Foo.class);
      assertSame(classInfo.getSerializer().getClass(), ObjectSerializer.class);
      classResolver.setSerializer(Foo.class, new CompatibleSerializer<>(fory, Foo.class));
      Assert.assertSame(classResolver.getClassInfo(Foo.class), classInfo);
      assertSame(classInfo.getSerializer().getClass(), CompatibleSerializer.class);
    }
    {
      classResolver.register(Bar.class);
      ClassInfo classInfo = classResolver.getClassInfo(Bar.class);
      classResolver.setSerializer(Bar.class, new ObjectSerializer<>(fory, Bar.class));
      Assert.assertSame(classResolver.getClassInfo(Bar.class), classInfo);
      assertSame(classInfo.getSerializer().getClass(), ObjectSerializer.class);
      classResolver.setSerializer(Bar.class, new CompatibleSerializer<>(fory, Bar.class));
      Assert.assertSame(classResolver.getClassInfo(Bar.class), classInfo);
      assertSame(classInfo.getSerializer().getClass(), CompatibleSerializer.class);
    }
  }

  private static class ErrorSerializer extends Serializer<Foo> {
    public ErrorSerializer(Fory fory) {
      super(fory, Foo.class);
      fory.getClassResolver().setSerializer(Foo.class, this);
      throw new RuntimeException();
    }
  }

  @Test
  public void testResetSerializer() {
    Fory fory =
        Fory.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .build();
    ClassResolver classResolver = fory.getClassResolver();
    Assert.assertThrows(() -> Serializers.newSerializer(fory, Foo.class, ErrorSerializer.class));
    Assert.assertNull(classResolver.getSerializer(Foo.class, false));
    Assert.assertThrows(
        () -> classResolver.createSerializerSafe(Foo.class, () -> new ErrorSerializer(fory)));
    Assert.assertNull(classResolver.getSerializer(Foo.class, false));
  }

  @Test
  public void testPrimitive() {
    Fory fory = Fory.builder().withLanguage(Language.JAVA).build();
    ClassResolver classResolver = fory.getClassResolver();
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

  // without static for test
  class FooCustomSerializer extends Serializer<Foo> {

    public FooCustomSerializer(Fory fory, Class<Foo> type) {
      super(fory, type);
    }

    @Override
    public void write(MemoryBuffer buffer, Foo value) {
      buffer.writeInt32(value.f1);
    }

    @Override
    public Foo read(MemoryBuffer buffer) {
      final Foo foo = new Foo();
      foo.f1 = buffer.readInt32();
      return foo;
    }
  }

  @Test
  public void testFooCustomSerializer() {
    ThreadSafeFory threadSafeFory =
        Fory.builder().withLanguage(Language.JAVA).buildThreadSafeFory();
    Assert.assertThrows(
        () -> threadSafeFory.registerSerializer(Foo.class, FooCustomSerializer.class));
    threadSafeFory.registerSerializer(Foo.class, f -> new FooCustomSerializer(f, Foo.class));
    final Foo foo = new Foo();
    foo.setF1(100);

    threadSafeFory.execute(
        fory -> {
          Assert.assertEquals(foo, serDe(fory, foo));
          return null;
        });
    threadSafeFory.execute(
        fory -> {
          Assert.assertEquals(
              fory.getClassResolver().getSerializer(foo.getClass()).getClass(),
              FooCustomSerializer.class);
          return null;
        });
  }

  static final class InvalidList extends AbstractList<Object> {
    @Override
    public Object get(int index) {
      throw new IndexOutOfBoundsException();
    }

    @Override
    public int size() {
      return 0;
    }

    public static final class InvalidListSerializer extends Serializer<InvalidList> {
      public InvalidListSerializer(Fory fory) {
        super(fory, InvalidList.class);
      }

      @Override
      public void write(MemoryBuffer buffer, InvalidList value) {
        // no-op
      }

      @Override
      public InvalidList read(MemoryBuffer buffer) {
        return new InvalidList();
      }
    }
  }

  static final class ValidList extends AbstractList<Object> {
    @Override
    public Object get(int index) {
      throw new IndexOutOfBoundsException();
    }

    @Override
    public int size() {
      return 0;
    }

    public static final class ValidListSerializer extends AbstractCollectionSerializer<ValidList> {
      public ValidListSerializer(Fory fory) {
        super(fory, ValidList.class);
      }

      @Override
      public Collection<?> onCollectionWrite(MemoryBuffer buffer, ValidList value) {
        return Collections.emptyList();
      }

      @Override
      public ValidList read(MemoryBuffer buffer) {
        return onCollectionRead(Collections.emptyList());
      }

      @Override
      public ValidList onCollectionRead(Collection collection) {
        return new ValidList();
      }
    }
  }

  static final class InvalidMap extends AbstractMap<Object, Object> {
    @Override
    public Set<Entry<Object, Object>> entrySet() {
      return Collections.emptySet();
    }

    public static final class InvalidMapSerializer extends Serializer<InvalidMap> {
      public InvalidMapSerializer(Fory fory) {
        super(fory, InvalidMap.class);
      }

      @Override
      public void write(MemoryBuffer buffer, InvalidMap value) {
        // no-op
      }

      @Override
      public InvalidMap read(MemoryBuffer buffer) {
        return new InvalidMap();
      }
    }
  }

  static final class ValidMap extends AbstractMap<Object, Object> {
    @Override
    public Set<Entry<Object, Object>> entrySet() {
      return Collections.emptySet();
    }

    public static final class ValidMapSerializer extends AbstractMapSerializer<ValidMap> {
      public ValidMapSerializer(Fory fory) {
        super(fory, ValidMap.class);
      }

      @Override
      public Map<?, ?> onMapWrite(MemoryBuffer buffer, ValidMap value) {
        return Collections.emptyMap();
      }

      @Override
      public ValidMap onMapCopy(Map map) {
        return new ValidMap();
      }

      @Override
      public ValidMap read(MemoryBuffer buffer) {
        return onMapRead(Collections.emptyMap());
      }

      @Override
      public ValidMap onMapRead(Map map) {
        return new ValidMap();
      }
    }
  }

  @Test
  public void testListAndMapSerializerRegistration() {
    Fory fory =
        Fory.builder()
            .withRefTracking(true)
            .requireClassRegistration(false)
            .validateSerializer(true)
            .build();
    // List invalid
    assertThrows(
        IllegalArgumentException.class,
        () -> fory.registerSerializer(InvalidList.class, InvalidList.InvalidListSerializer.class));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            fory.registerSerializer(
                InvalidList.class, new InvalidList.InvalidListSerializer(fory)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            fory.registerSerializer(
                InvalidList.class, f -> new InvalidList.InvalidListSerializer(f)));
    // List valid
    fory.register(ValidList.class);
    fory.registerSerializer(ValidList.class, new ValidList.ValidListSerializer(fory));
    Object listResult = fory.deserialize(fory.serialize(new ValidList()));
    assertTrue(listResult instanceof ValidList);
    // Map invalid
    assertThrows(
        IllegalArgumentException.class,
        () -> fory.registerSerializer(InvalidMap.class, InvalidMap.InvalidMapSerializer.class));
    assertThrows(
        IllegalArgumentException.class,
        () -> fory.registerSerializer(InvalidMap.class, new InvalidMap.InvalidMapSerializer(fory)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            fory.registerSerializer(InvalidMap.class, f -> new InvalidMap.InvalidMapSerializer(f)));
    // Map valid
    fory.register(ValidMap.class);
    fory.registerSerializer(ValidMap.class, new ValidMap.ValidMapSerializer(fory));
    Object mapResult = fory.deserialize(fory.serialize(new ValidMap()));
    assertTrue(mapResult instanceof ValidMap);
  }
}
