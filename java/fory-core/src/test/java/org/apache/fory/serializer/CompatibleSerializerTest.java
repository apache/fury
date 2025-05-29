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
import org.apache.fory.Fory;
import org.apache.fory.ForyTestBase;
import org.apache.fory.config.CompatibleMode;
import org.apache.fory.config.Language;
import org.apache.fory.reflect.ReflectionUtils;
import org.apache.fory.serializer.collection.UnmodifiableSerializersTest;
import org.apache.fory.test.bean.BeanA;
import org.apache.fory.test.bean.BeanB;
import org.apache.fory.test.bean.CollectionFields;
import org.apache.fory.test.bean.Foo;
import org.apache.fory.test.bean.MapFields;
import org.apache.fory.test.bean.Struct;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CompatibleSerializerTest extends ForyTestBase {
  @Test(dataProvider = "referenceTrackingConfig")
  public void testWrite(boolean referenceTracking) {
    Fory fory =
        Fory.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTracking)
            .requireClassRegistration(false)
            .build();
    fory.registerSerializer(Foo.class, new CompatibleSerializer<>(fory, Foo.class));
    fory.registerSerializer(BeanA.class, new CompatibleSerializer<>(fory, BeanA.class));
    fory.registerSerializer(BeanB.class, new CompatibleSerializer<>(fory, BeanB.class));
    serDeCheck(fory, Foo.create());
    serDeCheck(fory, BeanB.createBeanB(2));
    serDeCheck(fory, BeanA.createBeanA(2));
  }

  @Test(dataProvider = "foryCopyConfig")
  public void testCopy(Fory fory) {
    fory.registerSerializer(Foo.class, new CompatibleSerializer<>(fory, Foo.class));
    fory.registerSerializer(BeanA.class, new CompatibleSerializer<>(fory, BeanA.class));
    fory.registerSerializer(BeanB.class, new CompatibleSerializer<>(fory, BeanB.class));
    copyCheck(fory, Foo.create());
    copyCheck(fory, BeanB.createBeanB(2));
    copyCheck(fory, BeanA.createBeanA(2));
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testWriteCompatibleBasic(boolean referenceTrackingConfig) throws Exception {
    Fory fory =
        Fory.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .build();
    fory.registerSerializer(Foo.class, new CompatibleSerializer<>(fory, Foo.class));
    Object foo = Foo.create();
    for (Class<?> fooClass :
        new Class<?>[] {
          Foo.createCompatibleClass1(), Foo.createCompatibleClass2(), Foo.createCompatibleClass3(),
        }) {
      Object newFoo = fooClass.newInstance();
      ReflectionUtils.unsafeCopy(foo, newFoo);
      Fory newFory =
          Fory.builder()
              .withLanguage(Language.JAVA)
              .withRefTracking(referenceTrackingConfig)
              .requireClassRegistration(false)
              .withClassLoader(fooClass.getClassLoader())
              .build();
      newFory.registerSerializer(fooClass, new CompatibleSerializer<>(newFory, fooClass));
      {
        byte[] foo1Bytes = newFory.serialize(newFoo);
        Object deserialized = fory.deserialize(foo1Bytes);
        Assert.assertEquals(deserialized.getClass(), Foo.class);
        Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(deserialized, newFoo));
        byte[] fooBytes = fory.serialize(deserialized);
        Assert.assertTrue(
            ReflectionUtils.objectFieldsEquals(newFory.deserialize(fooBytes), newFoo));
      }
      {
        byte[] bytes1 = fory.serialize(foo);
        Object o1 = newFory.deserialize(bytes1);
        Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(o1, foo));
        Object o2 = fory.deserialize(newFory.serialize(o1));
        List<String> fields =
            Arrays.stream(fooClass.getDeclaredFields())
                .map(f -> f.getDeclaringClass().getSimpleName() + f.getName())
                .collect(Collectors.toList());
        Assert.assertTrue(ReflectionUtils.objectFieldsEquals(new HashSet<>(fields), o2, foo));
      }
      {
        Fory fory2 =
            Fory.builder()
                .withLanguage(Language.JAVA)
                .withRefTracking(referenceTrackingConfig)
                .requireClassRegistration(false)
                .withClassLoader(fooClass.getClassLoader())
                .build();
        fory2.registerSerializer(Foo.class, new CompatibleSerializer<>(newFory, Foo.class));
        Object o3 = fory.deserialize(newFory.serialize(foo));
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
    Fory fory =
        Fory.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .build();
    fory.registerSerializer(
        CollectionOuter.class, new CompatibleSerializer<>(fory, CollectionOuter.class));
    fory.registerSerializer(BeanB.class, new ObjectSerializer<>(fory, BeanB.class));
    CollectionOuter collectionOuter = new CollectionOuter();
    collectionOuter.beanBList = new ArrayList<>(ImmutableList.of(BeanB.createBeanB(2)));
    byte[] newBeanABytes = fory.serialize(collectionOuter);
    Object deserialized = fory.deserialize(newBeanABytes);
    Assert.assertEquals(deserialized, collectionOuter);
  }

  @Data
  public static class MapOuter {
    public Map<String, BeanB> stringBeanBMap;
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testWriteNestedMap(boolean referenceTrackingConfig) throws Exception {
    Fory fory =
        Fory.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .build();
    fory.registerSerializer(MapOuter.class, new CompatibleSerializer<>(fory, MapOuter.class));
    fory.registerSerializer(BeanB.class, new ObjectSerializer<>(fory, BeanB.class));
    MapOuter outerCollection = new MapOuter();
    outerCollection.stringBeanBMap = new HashMap<>(ImmutableMap.of("k", BeanB.createBeanB(2)));
    byte[] newBeanABytes = fory.serialize(outerCollection);
    Object deserialized = fory.deserialize(newBeanABytes);
    Assert.assertEquals(deserialized, outerCollection);
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testWriteCompatibleContainer(boolean referenceTrackingConfig) throws Exception {
    Fory fory =
        Fory.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .build();
    fory.registerSerializer(BeanA.class, new CompatibleSerializer<>(fory, BeanA.class));
    fory.registerSerializer(BeanB.class, new ObjectSerializer<>(fory, BeanB.class));
    BeanA beanA = BeanA.createBeanA(2);
    Class<?> cls = createCompatibleClass1();
    Object newBeanA = cls.newInstance();
    ReflectionUtils.unsafeCopy(beanA, newBeanA);
    Fory newFory =
        Fory.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .withClassLoader(cls.getClassLoader())
            .build();
    newFory.registerSerializer(cls, new CompatibleSerializer<>(newFory, cls));
    newFory.registerSerializer(BeanB.class, new ObjectSerializer<>(newFory, BeanB.class));
    byte[] newBeanABytes = newFory.serialize(newBeanA);
    Object deserialized = fory.deserialize(newBeanABytes);
    Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(deserialized, newBeanA));
    Assert.assertEquals(deserialized.getClass(), BeanA.class);
    byte[] beanABytes = fory.serialize(deserialized);
    Assert.assertTrue(
        ReflectionUtils.objectFieldsEquals(newFory.deserialize(beanABytes), newBeanA));

    byte[] objBytes = fory.serialize(beanA);
    Object obj2 = newFory.deserialize(objBytes);
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
    Fory fory =
        Fory.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .build();
    fory.registerSerializer(
        CollectionFields.class, new CompatibleSerializer<>(fory, CollectionFields.class));
    CollectionFields collectionFields = UnmodifiableSerializersTest.createCollectionFields();
    {
      Object o = serDe(fory, collectionFields);
      Object o1 = CollectionFields.copyToCanEqual(o, o.getClass().newInstance());
      Object o2 =
          CollectionFields.copyToCanEqual(
              collectionFields, collectionFields.getClass().newInstance());
      Assert.assertEquals(o1, o2);
    }
    Class<?> cls = createCompatibleClass2();
    Object newObj = cls.newInstance();
    ReflectionUtils.unsafeCopy(collectionFields, newObj);
    Fory newFory =
        Fory.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .build();
    newFory.registerSerializer(cls, new CompatibleSerializer<>(newFory, cls));
    byte[] bytes1 = newFory.serialize(newObj);
    Object deserialized = fory.deserialize(bytes1);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            CollectionFields.copyToCanEqual(deserialized, deserialized.getClass().newInstance()),
            CollectionFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));
    Assert.assertEquals(deserialized.getClass(), CollectionFields.class);
    byte[] bytes2 = fory.serialize(deserialized);
    Object obj2 = newFory.deserialize(bytes2);
    Assert.assertTrue(
        ReflectionUtils.objectFieldsEquals(
            CollectionFields.copyToCanEqual(obj2, obj2.getClass().newInstance()),
            CollectionFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));

    byte[] objBytes = fory.serialize(collectionFields);
    Object obj3 = newFory.deserialize(objBytes);
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
    Fory fory =
        Fory.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .build();
    fory.registerSerializer(MapFields.class, new CompatibleSerializer<>(fory, MapFields.class));
    MapFields mapFields = UnmodifiableSerializersTest.createMapFields();
    {
      Object o = serDe(fory, mapFields);
      Object o1 = MapFields.copyToCanEqual(o, o.getClass().newInstance());
      Object o2 = MapFields.copyToCanEqual(mapFields, mapFields.getClass().newInstance());
      Assert.assertEquals(o1, o2);
    }
    Class<?> cls = createCompatibleClass3();
    Object newObj = cls.newInstance();
    ReflectionUtils.unsafeCopy(mapFields, newObj);
    Fory newFory =
        Fory.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .build();
    newFory.registerSerializer(cls, new CompatibleSerializer<>(newFory, cls));
    byte[] bytes1 = newFory.serialize(newObj);
    Object deserialized = fory.deserialize(bytes1);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            MapFields.copyToCanEqual(deserialized, deserialized.getClass().newInstance()),
            MapFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));
    Assert.assertEquals(deserialized.getClass(), MapFields.class);
    byte[] bytes2 = fory.serialize(deserialized);
    Object obj2 = newFory.deserialize(bytes2);
    Assert.assertTrue(
        ReflectionUtils.objectFieldsEquals(
            MapFields.copyToCanEqual(obj2, obj2.getClass().newInstance()),
            MapFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));

    byte[] objBytes = fory.serialize(mapFields);
    Object obj3 = newFory.deserialize(objBytes);
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

  @Test(dataProvider = "compressNumberScopedMetaShare")
  public void testCompressInt(boolean compressNumber, boolean scopedMetaShare) throws Exception {
    Class<?> structClass = Struct.createNumberStructClass("CompatibleCompressIntStruct", 2);
    Fory fory =
        builder()
            .withNumberCompressed(compressNumber)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withClassLoader(structClass.getClassLoader())
            .withScopedMetaShare(scopedMetaShare)
            .build();
    serDeCheck(fory, Struct.createPOJO(structClass));
  }
}
