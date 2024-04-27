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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Data;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.Language;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.serializer.collection.UnmodifiableSerializersTest;
import org.apache.fury.test.bean.BeanA;
import org.apache.fury.test.bean.BeanB;
import org.apache.fury.test.bean.CollectionFields;
import org.apache.fury.test.bean.Foo;
import org.apache.fury.test.bean.MapFields;
import org.apache.fury.test.bean.Struct;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CompatibleSerializerTest extends FuryTestBase {
  @Test(dataProvider = "referenceTrackingConfig")
  public void testWrite(boolean referenceTracking) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTracking)
            .requireClassRegistration(false)
            .build();
    fury.registerSerializer(Foo.class, new CompatibleSerializer<>(fury, Foo.class));
    fury.registerSerializer(BeanA.class, new CompatibleSerializer<>(fury, BeanA.class));
    fury.registerSerializer(BeanB.class, new CompatibleSerializer<>(fury, BeanB.class));
    serDeCheck(fury, Foo.create());
    serDeCheck(fury, BeanB.createBeanB(2));
    serDeCheck(fury, BeanA.createBeanA(2));
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testWriteCompatibleBasic(boolean referenceTrackingConfig) throws Exception {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .build();
    fury.registerSerializer(Foo.class, new CompatibleSerializer<>(fury, Foo.class));
    Object foo = Foo.create();
    for (Class<?> fooClass :
        new Class<?>[] {
          Foo.createCompatibleClass1(), Foo.createCompatibleClass2(), Foo.createCompatibleClass3(),
        }) {
      Object newFoo = fooClass.newInstance();
      ReflectionUtils.unsafeCopy(foo, newFoo);
      Fury newFury =
          Fury.builder()
              .withLanguage(Language.JAVA)
              .withRefTracking(referenceTrackingConfig)
              .requireClassRegistration(false)
              .withClassLoader(fooClass.getClassLoader())
              .build();
      newFury.registerSerializer(fooClass, new CompatibleSerializer<>(newFury, fooClass));
      {
        byte[] foo1Bytes = newFury.serialize(newFoo);
        Object deserialized = fury.deserialize(foo1Bytes);
        Assert.assertEquals(deserialized.getClass(), Foo.class);
        Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(deserialized, newFoo));
        byte[] fooBytes = fury.serialize(deserialized);
        Assert.assertTrue(
            ReflectionUtils.objectFieldsEquals(newFury.deserialize(fooBytes), newFoo));
      }
      {
        byte[] bytes1 = fury.serialize(foo);
        Object o1 = newFury.deserialize(bytes1);
        Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(o1, foo));
        Object o2 = fury.deserialize(newFury.serialize(o1));
        List<String> fields =
            Arrays.stream(fooClass.getDeclaredFields())
                .map(f -> f.getDeclaringClass().getSimpleName() + f.getType() + f.getName())
                .collect(Collectors.toList());
        Assert.assertTrue(ReflectionUtils.objectFieldsEquals(new HashSet<>(fields), o2, foo));
      }
      {
        Fury fury2 =
            Fury.builder()
                .withLanguage(Language.JAVA)
                .withRefTracking(referenceTrackingConfig)
                .requireClassRegistration(false)
                .withClassLoader(fooClass.getClassLoader())
                .build();
        fury2.registerSerializer(Foo.class, new CompatibleSerializer<>(newFury, Foo.class));
        Object o3 = fury.deserialize(newFury.serialize(foo));
        Assert.assertTrue(ReflectionUtils.objectFieldsEquals(o3, foo));
      }
    }
  }

  @Data
  public static class CollectionOuter {
    public List<BeanB> beanBList;
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testWriteNestedCollection(boolean referenceTrackingConfig) throws Exception {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .build();
    fury.registerSerializer(
        CollectionOuter.class, new CompatibleSerializer<>(fury, CollectionOuter.class));
    fury.registerSerializer(BeanB.class, new ObjectSerializer<>(fury, BeanB.class));
    CollectionOuter collectionOuter = new CollectionOuter();
    collectionOuter.beanBList = new ArrayList<>(ImmutableList.of(BeanB.createBeanB(2)));
    byte[] newBeanABytes = fury.serialize(collectionOuter);
    Object deserialized = fury.deserialize(newBeanABytes);
    Assert.assertEquals(deserialized, collectionOuter);
  }

  @Data
  public static class MapOuter {
    public Map<String, BeanB> stringBeanBMap;
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testWriteNestedMap(boolean referenceTrackingConfig) throws Exception {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .build();
    fury.registerSerializer(MapOuter.class, new CompatibleSerializer<>(fury, MapOuter.class));
    fury.registerSerializer(BeanB.class, new ObjectSerializer<>(fury, BeanB.class));
    MapOuter outerCollection = new MapOuter();
    outerCollection.stringBeanBMap = new HashMap<>(ImmutableMap.of("k", BeanB.createBeanB(2)));
    byte[] newBeanABytes = fury.serialize(outerCollection);
    Object deserialized = fury.deserialize(newBeanABytes);
    Assert.assertEquals(deserialized, outerCollection);
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testWriteCompatibleContainer(boolean referenceTrackingConfig) throws Exception {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .build();
    fury.registerSerializer(BeanA.class, new CompatibleSerializer<>(fury, BeanA.class));
    fury.registerSerializer(BeanB.class, new ObjectSerializer<>(fury, BeanB.class));
    BeanA beanA = BeanA.createBeanA(2);
    Class<?> cls = createCompatibleClass1();
    Object newBeanA = cls.newInstance();
    ReflectionUtils.unsafeCopy(beanA, newBeanA);
    Fury newFury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .withClassLoader(cls.getClassLoader())
            .build();
    newFury.registerSerializer(cls, new CompatibleSerializer<>(newFury, cls));
    newFury.registerSerializer(BeanB.class, new ObjectSerializer<>(newFury, BeanB.class));
    byte[] newBeanABytes = newFury.serialize(newBeanA);
    Object deserialized = fury.deserialize(newBeanABytes);
    Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(deserialized, newBeanA));
    Assert.assertEquals(deserialized.getClass(), BeanA.class);
    byte[] beanABytes = fury.serialize(deserialized);
    Assert.assertTrue(
        ReflectionUtils.objectFieldsEquals(newFury.deserialize(beanABytes), newBeanA));

    byte[] objBytes = fury.serialize(beanA);
    Object obj2 = newFury.deserialize(objBytes);
    Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(obj2, newBeanA));
  }

  public static Class<?> createCompatibleClass1() {
    String pkg = BeanA.class.getPackage().getName();
    String code =
        ""
            + "package "
            + pkg
            + ";\n"
            + "import java.util.*;\n"
            + "import java.math.*;\n"
            + "public class BeanA {\n"
            + "  private Float f4;\n"
            + "  private double f5;\n"
            + "  private BeanB beanB;\n"
            + "  private BeanB beanB_added;\n"
            + "  private int[] intArray;\n"
            + "  private int[] intArray_added;\n"
            + "  private byte[] bytes;\n"
            + "  private transient BeanB f13;\n"
            + "  public BigDecimal f16;\n"
            + "  public String f17;\n"
            + "  public String longStringNameField_added;\n"
            + "  private List<Double> doubleList;\n"
            + "  private Iterable<BeanB> beanBIterable;\n"
            + "  private List<BeanB> beanBList;\n"
            + "  private List<BeanB> beanBList_added;\n"
            + "  private Map<String, BeanB> stringBeanBMap;\n"
            + "  private Map<String, String> stringStringMap_added;\n"
            + "  private int[][] int2DArray;\n"
            + "  private int[][] int2DArray_added;\n"
            + "}";
    return ClassUtils.loadClass(
        BeanA.class, code, CompatibleSerializerTest.class + "createCompatibleClass1");
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testWriteCompatibleCollection(boolean referenceTrackingConfig) throws Exception {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .build();
    fury.registerSerializer(
        CollectionFields.class, new CompatibleSerializer<>(fury, CollectionFields.class));
    CollectionFields collectionFields = UnmodifiableSerializersTest.createCollectionFields();
    {
      Object o = serDe(fury, collectionFields);
      Object o1 = CollectionFields.copyToCanEqual(o, o.getClass().newInstance());
      Object o2 =
          CollectionFields.copyToCanEqual(
              collectionFields, collectionFields.getClass().newInstance());
      Assert.assertEquals(o1, o2);
    }
    Class<?> cls = createCompatibleClass2();
    Object newObj = cls.newInstance();
    ReflectionUtils.unsafeCopy(collectionFields, newObj);
    Fury newFury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .build();
    newFury.registerSerializer(cls, new CompatibleSerializer<>(newFury, cls));
    byte[] bytes1 = newFury.serialize(newObj);
    Object deserialized = fury.deserialize(bytes1);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            CollectionFields.copyToCanEqual(deserialized, deserialized.getClass().newInstance()),
            CollectionFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));
    Assert.assertEquals(deserialized.getClass(), CollectionFields.class);
    byte[] bytes2 = fury.serialize(deserialized);
    Object obj2 = newFury.deserialize(bytes2);
    Assert.assertTrue(
        ReflectionUtils.objectFieldsEquals(
            CollectionFields.copyToCanEqual(obj2, obj2.getClass().newInstance()),
            CollectionFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));

    byte[] objBytes = fury.serialize(collectionFields);
    Object obj3 = newFury.deserialize(objBytes);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            CollectionFields.copyToCanEqual(obj3, obj3.getClass().newInstance()),
            CollectionFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));
  }

  public static Class<?> createCompatibleClass2() {
    String pkg = CollectionFields.class.getPackage().getName();
    String code =
        ""
            + "package "
            + pkg
            + ";\n"
            + "import java.util.*;\n"
            + "public class CollectionFields {\n"
            + "  public Collection<Integer> collection2;\n"
            + "  public List<Integer> collection3;\n"
            + "  public Collection<String> randomAccessList2;\n"
            + "  public List<String> randomAccessList3;\n"
            + "  public Collection list;\n"
            + "  public Collection<String> list2;\n"
            + "  public List<String> list3;\n"
            + "  public Collection<String> set2;\n"
            + "  public Set<String> set3;\n"
            + "  public Collection<String> sortedSet2;\n"
            + "  public SortedSet<String> sortedSet3;\n"
            + "  public Map map;\n"
            + "  public Map<String, String> map2;\n"
            + "  public SortedMap<Integer, Integer> sortedMap3;"
            + "}";
    return ClassUtils.loadClass(
        CollectionFields.class, code, CompatibleSerializerTest.class + "createCompatibleClass2");
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testWriteCompatibleMap(boolean referenceTrackingConfig) throws Exception {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .build();
    fury.registerSerializer(MapFields.class, new CompatibleSerializer<>(fury, MapFields.class));
    MapFields mapFields = UnmodifiableSerializersTest.createMapFields();
    {
      Object o = serDe(fury, mapFields);
      Object o1 = MapFields.copyToCanEqual(o, o.getClass().newInstance());
      Object o2 = MapFields.copyToCanEqual(mapFields, mapFields.getClass().newInstance());
      Assert.assertEquals(o1, o2);
    }
    Class<?> cls = createCompatibleClass3();
    Object newObj = cls.newInstance();
    ReflectionUtils.unsafeCopy(mapFields, newObj);
    Fury newFury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .build();
    newFury.registerSerializer(cls, new CompatibleSerializer<>(newFury, cls));
    byte[] bytes1 = newFury.serialize(newObj);
    Object deserialized = fury.deserialize(bytes1);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            MapFields.copyToCanEqual(deserialized, deserialized.getClass().newInstance()),
            MapFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));
    Assert.assertEquals(deserialized.getClass(), MapFields.class);
    byte[] bytes2 = fury.serialize(deserialized);
    Object obj2 = newFury.deserialize(bytes2);
    Assert.assertTrue(
        ReflectionUtils.objectFieldsEquals(
            MapFields.copyToCanEqual(obj2, obj2.getClass().newInstance()),
            MapFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));

    byte[] objBytes = fury.serialize(mapFields);
    Object obj3 = newFury.deserialize(objBytes);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            MapFields.copyToCanEqual(obj3, obj3.getClass().newInstance()),
            MapFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));
  }

  public static Class<?> createCompatibleClass3() {
    String pkg = MapFields.class.getPackage().getName();
    String code =
        ""
            + "package "
            + pkg
            + ";\n"
            + "import java.util.*;\n"
            + "import java.util.concurrent.*;\n"
            + "public class MapFields {\n"
            + " public Map map;\n"
            + "  public Map<String, Integer> map2;\n"
            + "  public Map linkedHashMap;\n"
            + "  public LinkedHashMap<String, Integer> linkedHashMap3;\n"
            + "  public SortedMap sortedMap;\n"
            + "  public SortedMap<String, Integer> sortedMap2;\n"
            + "  public Map concurrentHashMap;\n"
            + "  public ConcurrentHashMap<String, Integer> concurrentHashMap2;\n"
            + "  public ConcurrentSkipListMap skipListMap2;\n"
            + "  public ConcurrentSkipListMap<String, Integer> skipListMap3;\n"
            + "  public EnumMap enumMap2;\n"
            + "  public Map emptyMap;\n"
            + "  public Map singletonMap;\n"
            + "  public Map<String, Integer> singletonMap2;\n"
            + "}";
    return ClassUtils.loadClass(
        MapFields.class, code, CompatibleSerializerTest.class + "createCompatibleClass3");
  }

  @Test(dataProvider = "compressNumber")
  public void testCompressInt(boolean compressNumber) throws Exception {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withNumberCompressed(compressNumber)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .requireClassRegistration(false)
            .build();
    Class<?> structClass = Struct.createNumberStructClass("CompatibleCompressIntStruct", 2);
    serDeCheck(fury, Struct.createPOJO(structClass));
  }
}
