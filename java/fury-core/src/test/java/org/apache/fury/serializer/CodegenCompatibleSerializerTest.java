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

import static org.apache.fury.serializer.ClassUtils.loadClass;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.FuryBuilder;
import org.apache.fury.config.Language;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.serializer.collection.UnmodifiableSerializersTest;
import org.apache.fury.test.bean.BeanA;
import org.apache.fury.test.bean.BeanB;
import org.apache.fury.test.bean.CollectionFields;
import org.apache.fury.test.bean.Foo;
import org.apache.fury.test.bean.MapFields;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class CodegenCompatibleSerializerTest extends FuryTestBase {

  @DataProvider(name = "config")
  public static Object[][] config() {
    return Sets.cartesianProduct(
            ImmutableSet.of(true, false), // referenceTracking
            ImmutableSet.of(true, false), // scopedMetaShare
            ImmutableSet.of(true, false)) // enable codegen
        .stream()
        .map(List::toArray)
        .toArray(Object[][]::new);
  }

  private FuryBuilder furyBuilder() {
    return Fury.builder()
        .withLanguage(Language.JAVA)
        .requireClassRegistration(false)
        .suppressClassRegistrationWarnings(true);
  }

  @Test(dataProvider = "config")
  public void testWrite(boolean referenceTracking, boolean scopedMetaShare, boolean enableCodegen) {
    Fury fury =
        furyBuilder()
            .withRefTracking(referenceTracking)
            .withCodegen(enableCodegen)
            .withScopedMetaShare(scopedMetaShare)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .build();
    serDeCheck(fury, Foo.create());
    serDeCheck(fury, BeanB.createBeanB(2));
    serDeCheck(fury, BeanA.createBeanA(2));
  }

  @Test(dataProvider = "config")
  public void testCopy(boolean referenceTracking, boolean scopedMetaShare, boolean enableCodegen) {
    Fury fury =
        furyBuilder()
            .withRefCopy(referenceTracking)
            .withCodegen(enableCodegen)
            .withScopedMetaShare(scopedMetaShare)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .build();
    copyCheck(fury, Foo.create());
    copyCheck(fury, BeanB.createBeanB(2));
    copyCheck(fury, BeanA.createBeanA(2));
  }

  @Test(dataProvider = "config")
  public void testWriteCompatibleBasic(
      boolean referenceTracking, boolean scopedMetaShare, boolean enableCodegen) throws Exception {
    Supplier<FuryBuilder> builder =
        () ->
            Fury.builder()
                .withLanguage(Language.JAVA)
                .withRefTracking(referenceTracking)
                .withCodegen(enableCodegen)
                .withCompatibleMode(CompatibleMode.COMPATIBLE)
                .withScopedMetaShare(scopedMetaShare)
                .requireClassRegistration(false);
    Fury fury = builder.get().build();
    Object foo = Foo.create();
    for (Class<?> fooClass :
        new Class<?>[] {
          Foo.createCompatibleClass1(), Foo.createCompatibleClass2(), Foo.createCompatibleClass3(),
        }) {
      Object newFoo = fooClass.newInstance();
      ReflectionUtils.unsafeCopy(foo, newFoo);
      Fury newFury = builder.get().withClassLoader(fooClass.getClassLoader()).build();

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
                .map(f -> f.getDeclaringClass().getSimpleName() + f.getName())
                .collect(Collectors.toList());
        Assert.assertTrue(ReflectionUtils.objectFieldsEquals(new HashSet<>(fields), o2, foo));
      }
      {
        Object o3 = fury.deserialize(newFury.serialize(foo));
        Assert.assertTrue(ReflectionUtils.objectFieldsEquals(o3, foo));
      }
    }
  }

  @Test(dataProvider = "config")
  public void testWriteCompatibleBasicCopy(
      boolean referenceTracking, boolean scopedMetaShare, boolean enableCodegen) throws Exception {
    Supplier<FuryBuilder> builder =
        () ->
            Fury.builder()
                .withLanguage(Language.JAVA)
                .withRefCopy(referenceTracking)
                .withCodegen(enableCodegen)
                .withCompatibleMode(CompatibleMode.COMPATIBLE)
                .withScopedMetaShare(scopedMetaShare)
                .requireClassRegistration(false);
    Fury fury = builder.get().build();
    Object foo = Foo.create();
    for (Class<?> fooClass :
        new Class<?>[] {
          Foo.createCompatibleClass1(), Foo.createCompatibleClass2(), Foo.createCompatibleClass3(),
        }) {
      Object newFoo = fooClass.newInstance();
      ReflectionUtils.unsafeCopy(foo, newFoo);
      Fury newFury = builder.get().withClassLoader(fooClass.getClassLoader()).build();
      {
        Object copy = fury.copy(newFoo);
        Assert.assertEquals(copy.getClass().getName(), Foo.class.getName());
        Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(copy, newFoo));
      }
      {
        Object o3 = newFury.copy(foo);
        Assert.assertTrue(ReflectionUtils.objectFieldsEquals(o3, foo));
      }
    }
  }

  @Test(dataProvider = "config")
  public void testWriteCompatibleCollectionBasic(
      boolean referenceTracking, boolean scopedMetaShare, boolean enableCodegen) throws Exception {
    BeanA beanA = BeanA.createBeanA(2);
    Supplier<FuryBuilder> builder =
        () ->
            Fury.builder()
                .withLanguage(Language.JAVA)
                .withRefTracking(referenceTracking)
                .withCodegen(enableCodegen)
                .withCompatibleMode(CompatibleMode.COMPATIBLE)
                .withScopedMetaShare(scopedMetaShare)
                .requireClassRegistration(false);
    Fury fury = builder.get().build();
    String pkg = BeanA.class.getPackage().getName();
    String code =
        ""
            + "package "
            + pkg
            + ";\n"
            + "import java.util.*;\n"
            + "import java.math.*;\n"
            + "public class BeanA {\n"
            + "  private List<Double> doubleList;\n"
            + "  private Iterable<BeanB> beanBIterable;\n"
            + "  private List<BeanB> beanBList;\n"
            + "}";
    Class<?> cls1 =
        loadClass(
            BeanA.class,
            code,
            CodegenCompatibleSerializerTest.class + "testWriteCompatibleCollectionBasic_1");
    Fury fury1 = builder.get().withClassLoader(cls1.getClassLoader()).build();
    code =
        ""
            + "package "
            + pkg
            + ";\n"
            + "import java.util.*;\n"
            + "import java.math.*;\n"
            + "public class BeanA {\n"
            + "  private List<Double> doubleList;\n"
            + "  private Iterable<BeanB> beanBIterable;\n"
            + "}";
    Class<?> cls2 =
        loadClass(
            BeanA.class,
            code,
            CodegenCompatibleSerializerTest.class + "testWriteCompatibleCollectionBasic_2");
    Object newBeanA = cls2.newInstance();
    ReflectionUtils.unsafeCopy(beanA, newBeanA);
    Fury fury2 = builder.get().withClassLoader(cls2.getClassLoader()).build();
    byte[] newBeanABytes = fury2.serialize(newBeanA);
    Object deserialized = fury1.deserialize(newBeanABytes);
    Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(deserialized, newBeanA));
    Assert.assertEquals(deserialized.getClass(), cls1);
    byte[] beanABytes = fury1.serialize(deserialized);
    Assert.assertTrue(ReflectionUtils.objectFieldsEquals(fury2.deserialize(beanABytes), newBeanA));

    byte[] objBytes = fury1.serialize(beanA);
    Object obj2 = fury2.deserialize(objBytes);
    Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(obj2, newBeanA));

    Assert.assertEquals(fury.deserialize(fury2.serialize(beanA)), beanA);
  }

  @Test(dataProvider = "config")
  public void testWriteCompatibleContainer(
      boolean referenceTracking, boolean scopedMetaShare, boolean enableCodegen) throws Exception {
    Supplier<FuryBuilder> builder =
        () ->
            Fury.builder()
                .withLanguage(Language.JAVA)
                .withRefTracking(referenceTracking)
                .withCodegen(enableCodegen)
                .withCompatibleMode(CompatibleMode.COMPATIBLE)
                .withScopedMetaShare(scopedMetaShare)
                .requireClassRegistration(false);
    Fury fury = builder.get().build();
    BeanA beanA = BeanA.createBeanA(2);
    serDe(fury, beanA);
    Class<?> cls = ClassUtils.createCompatibleClass1();
    Object newBeanA = cls.newInstance();
    ReflectionUtils.unsafeCopy(beanA, newBeanA);
    Fury newFury = builder.get().withClassLoader(cls.getClassLoader()).build();
    byte[] newBeanABytes = newFury.serialize(newBeanA);
    BeanA deserialized = (BeanA) fury.deserialize(newBeanABytes);
    Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(deserialized, newBeanA));
    Assert.assertEquals(deserialized.getClass(), BeanA.class);
    byte[] beanABytes = fury.serialize(deserialized);
    Assert.assertTrue(
        ReflectionUtils.objectFieldsEquals(newFury.deserialize(beanABytes), newBeanA));

    byte[] objBytes = fury.serialize(beanA);
    Object obj2 = newFury.deserialize(objBytes);
    Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(obj2, newBeanA));
    Assert.assertEquals(fury.deserialize(newFury.serialize(beanA)), beanA);
  }

  @Test(dataProvider = "config")
  public void testWriteCompatibleCollection(
      boolean referenceTracking, boolean scopedMetaShare, boolean enableCodegen) throws Exception {
    Supplier<FuryBuilder> builder =
        () ->
            Fury.builder()
                .withLanguage(Language.JAVA)
                .withRefTracking(referenceTracking)
                .withCodegen(enableCodegen)
                .withCompatibleMode(CompatibleMode.COMPATIBLE)
                .withScopedMetaShare(scopedMetaShare)
                .requireClassRegistration(false);
    Fury fury = builder.get().build();
    CollectionFields collectionFields = UnmodifiableSerializersTest.createCollectionFields();
    {
      Object o = serDe(fury, collectionFields);
      Object o1 = CollectionFields.copyToCanEqual(o, o.getClass().newInstance());
      Object o2 =
          CollectionFields.copyToCanEqual(
              collectionFields, collectionFields.getClass().newInstance());
      Assert.assertEquals(o1, o2);
    }
    Class<?> cls2 = ClassUtils.createCompatibleClass2();
    Object newObj = cls2.newInstance();
    ReflectionUtils.unsafeCopy(collectionFields, newObj);
    Fury fury2 = builder.get().withClassLoader(cls2.getClassLoader()).build();
    byte[] bytes1 = fury2.serialize(newObj);
    Object deserialized = fury.deserialize(bytes1);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            CollectionFields.copyToCanEqual(deserialized, deserialized.getClass().newInstance()),
            CollectionFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));
    Assert.assertEquals(deserialized.getClass(), CollectionFields.class);

    byte[] bytes2 = fury.serialize(deserialized);
    Object obj2 = fury2.deserialize(bytes2);
    Assert.assertTrue(
        ReflectionUtils.objectFieldsEquals(
            CollectionFields.copyToCanEqual(obj2, obj2.getClass().newInstance()),
            CollectionFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));

    byte[] objBytes = fury.serialize(collectionFields);
    Object obj3 = fury2.deserialize(objBytes);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            CollectionFields.copyToCanEqual(obj3, obj3.getClass().newInstance()),
            CollectionFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));

    Assert.assertEquals(
        ((CollectionFields) (fury.deserialize(fury2.serialize(collectionFields)))).toCanEqual(),
        collectionFields.toCanEqual());
  }

  @Test(dataProvider = "config")
  public void testWriteCompatibleMap(
      boolean referenceTracking, boolean scopedMetaShare, boolean enableCodegen) throws Exception {
    Supplier<FuryBuilder> builder =
        () ->
            Fury.builder()
                .withLanguage(Language.JAVA)
                .withRefTracking(referenceTracking)
                .withCodegen(enableCodegen)
                .withCompatibleMode(CompatibleMode.COMPATIBLE)
                .withScopedMetaShare(scopedMetaShare)
                .requireClassRegistration(false);
    Fury fury = builder.get().build();
    MapFields mapFields = UnmodifiableSerializersTest.createMapFields();
    {
      Object o = serDe(fury, mapFields);
      Object o1 = MapFields.copyToCanEqual(o, o.getClass().newInstance());
      Object o2 = MapFields.copyToCanEqual(mapFields, mapFields.getClass().newInstance());
      Assert.assertEquals(o1, o2);
    }
    Class<?> cls = ClassUtils.createCompatibleClass3();
    Object newObj = cls.newInstance();
    ReflectionUtils.unsafeCopy(mapFields, newObj);
    Fury fury2 = builder.get().withClassLoader(cls.getClassLoader()).build();
    byte[] bytes1 = fury2.serialize(newObj);
    Object deserialized = fury.deserialize(bytes1);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            MapFields.copyToCanEqual(deserialized, deserialized.getClass().newInstance()),
            MapFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));
    Assert.assertEquals(deserialized.getClass(), MapFields.class);

    byte[] bytes2 = fury.serialize(deserialized);
    Object obj2 = fury2.deserialize(bytes2);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            MapFields.copyToCanEqual(obj2, obj2.getClass().newInstance()),
            MapFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));

    byte[] objBytes = fury.serialize(mapFields);
    Object obj3 = fury2.deserialize(objBytes);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            MapFields.copyToCanEqual(obj3, obj3.getClass().newInstance()),
            MapFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));

    Assert.assertEquals(
        ((MapFields) (fury.deserialize(fury2.serialize(mapFields)))).toCanEqual(),
        mapFields.toCanEqual());
  }
}
