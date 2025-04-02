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

import static org.apache.fury.reflect.ReflectionUtils.getObjectFieldValue;
import static org.apache.fury.serializer.ClassUtils.loadClass;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.builder.MetaSharedCodecBuilder;
import org.apache.fury.codegen.CompileUnit;
import org.apache.fury.codegen.JaninoUtils;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.FuryBuilder;
import org.apache.fury.config.Language;
import org.apache.fury.meta.ClassDefEncoderTest;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.resolver.MetaContext;
import org.apache.fury.serializer.collection.UnmodifiableSerializersTest;
import org.apache.fury.test.bean.BeanA;
import org.apache.fury.test.bean.BeanB;
import org.apache.fury.test.bean.CollectionFields;
import org.apache.fury.test.bean.Foo;
import org.apache.fury.test.bean.MapFields;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests for {@link MetaSharedCodecBuilder} and {@link MetaSharedSerializer}, and protocol
 * interoperability between them.
 */
public class MetaSharedCompatibleTest extends FuryTestBase {
  public static Object serDeMetaSharedCheck(Fury fury, Object obj) {
    Object newObj = serDeMetaShared(fury, obj);
    Assert.assertEquals(newObj, obj);
    return newObj;
  }

  @DataProvider
  public static Object[][] config1() {
    return Sets.cartesianProduct(
            ImmutableSet.of(true, false), // referenceTracking
            ImmutableSet.of(true, false), // compress number
            ImmutableSet.of(true, false)) // enable codegen
        .stream()
        .map(List::toArray)
        .toArray(Object[][]::new);
  }

  @DataProvider
  public static Object[][] config2() {
    return Sets.cartesianProduct(
            ImmutableSet.of(true, false), // referenceTracking
            ImmutableSet.of(true, false), // compress number
            ImmutableSet.of(true, false), // fury1 enable codegen
            ImmutableSet.of(true, false) // fury2 enable codegen
            )
        .stream()
        .map(List::toArray)
        .toArray(Object[][]::new);
  }

  @DataProvider
  public static Object[][] config3() {
    return Sets.cartesianProduct(
            ImmutableSet.of(true, false), // referenceTracking
            ImmutableSet.of(true, false), // compress number
            ImmutableSet.of(true, false), // fury1 enable codegen
            ImmutableSet.of(true, false), // fury2 enable codegen
            ImmutableSet.of(true, false) // fury3 enable codegen
            )
        .stream()
        .map(List::toArray)
        .toArray(Object[][]::new);
  }

  private static FuryBuilder furyBuilder() {
    return Fury.builder()
        .withLanguage(Language.JAVA)
        .withMetaShare(true)
        .withCompatibleMode(CompatibleMode.COMPATIBLE)
        .requireClassRegistration(false)
        .withScopedMetaShare(false);
  }

  @Test(dataProvider = "config1")
  public void testWrite(boolean referenceTracking, boolean compressNumber, boolean enableCodegen) {
    Fury fury =
        furyBuilder()
            .withNumberCompressed(compressNumber)
            .withRefTracking(referenceTracking)
            .withCodegen(enableCodegen)
            .build();
    serDeMetaSharedCheck(fury, Foo.create());
    serDeMetaSharedCheck(fury, BeanB.createBeanB(2));
    serDeMetaSharedCheck(fury, BeanA.createBeanA(2));
  }

  @Test(dataProvider = "config2")
  public void testWriteCompatibleBasic(
      boolean referenceTracking,
      boolean compressNumber,
      boolean enableCodegen1,
      boolean enableCodegen2)
      throws Exception {
    Fury fury =
        furyBuilder()
            .withRefTracking(referenceTracking)
            .withNumberCompressed(compressNumber)
            .withCodegen(enableCodegen1)
            .build();
    Object foo = Foo.create();
    for (Class<?> fooClass :
        new Class<?>[] {
          Foo.createCompatibleClass1(), Foo.createCompatibleClass2(), Foo.createCompatibleClass3(),
        }) {
      Object newFoo = fooClass.newInstance();
      ReflectionUtils.unsafeCopy(foo, newFoo);
      MetaContext context = new MetaContext();
      Fury newFury =
          furyBuilder()
              .withRefTracking(referenceTracking)
              .withNumberCompressed(compressNumber)
              .withCodegen(enableCodegen2)
              .withClassLoader(fooClass.getClassLoader())
              .build();
      MetaContext context1 = new MetaContext();
      {
        newFury.getSerializationContext().setMetaContext(context1);
        byte[] foo1Bytes = newFury.serialize(newFoo);
        fury.getSerializationContext().setMetaContext(context);
        Object deserialized = fury.deserialize(foo1Bytes);
        Assert.assertEquals(deserialized.getClass(), Foo.class);
        Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(deserialized, newFoo));
        fury.getSerializationContext().setMetaContext(context);
        byte[] fooBytes = fury.serialize(deserialized);
        newFury.getSerializationContext().setMetaContext(context1);
        Assert.assertTrue(
            ReflectionUtils.objectFieldsEquals(newFury.deserialize(fooBytes), newFoo));
      }
      {
        fury.getSerializationContext().setMetaContext(context);
        byte[] bytes1 = fury.serialize(foo);
        newFury.getSerializationContext().setMetaContext(context1);
        Object o1 = newFury.deserialize(bytes1);
        Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(o1, foo));
        fury.getSerializationContext().setMetaContext(context);
        newFury.getSerializationContext().setMetaContext(context1);
        Object o2 = fury.deserialize(newFury.serialize(o1));
        List<String> fields =
            Arrays.stream(fooClass.getDeclaredFields())
                .map(f -> f.getDeclaringClass().getSimpleName() + f.getName())
                .collect(Collectors.toList());
        Assert.assertTrue(ReflectionUtils.objectFieldsEquals(new HashSet<>(fields), o2, foo));
      }
      {
        fury.getSerializationContext().setMetaContext(context);
        newFury.getSerializationContext().setMetaContext(context1);
        Object o3 = fury.deserialize(newFury.serialize(foo));
        Assert.assertTrue(ReflectionUtils.objectFieldsEquals(o3, foo));
      }
    }
  }

  @Test
  public void testWriteCompatibleCollectionSimple() throws Exception {
    BeanA beanA = BeanA.createBeanA(2);
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
            MetaSharedCompatibleTest.class + "testWriteCompatibleCollectionBasic_1");
    Fury fury1 =
        furyBuilder()
            .withCodegen(false)
            .withMetaShare(true)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withClassLoader(cls1.getClassLoader())
            .build();
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
            MetaSharedCompatibleTest.class + "testWriteCompatibleCollectionBasic_2");
    Object o2 = cls2.newInstance();
    ReflectionUtils.unsafeCopy(beanA, o2);
    Fury fury2 =
        furyBuilder()
            .withCodegen(false)
            .withMetaShare(true)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withClassLoader(cls2.getClassLoader())
            .build();

    MetaContext context1 = new MetaContext();
    MetaContext context2 = new MetaContext();
    fury1.getSerializationContext().setMetaContext(context1);
    byte[] objBytes = fury1.serialize(beanA);
    fury2.getSerializationContext().setMetaContext(context2);
    Object obj2 = fury2.deserialize(objBytes);
    Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(obj2, o2));
  }

  @Test(dataProvider = "config3")
  public void testWriteCompatibleCollectionBasic(
      boolean referenceTracking,
      boolean compressNumber,
      boolean enableCodegen1,
      boolean enableCodegen2,
      boolean enableCodegen3)
      throws Exception {
    BeanA beanA = BeanA.createBeanA(2);
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
            MetaSharedCompatibleTest.class + "testWriteCompatibleCollectionBasic_1");
    Fury fury1 =
        furyBuilder()
            .withRefTracking(referenceTracking)
            .withNumberCompressed(compressNumber)
            .withCodegen(enableCodegen2)
            .withClassLoader(cls1.getClassLoader())
            .build();
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
            MetaSharedCompatibleTest.class + "testWriteCompatibleCollectionBasic_2");
    Object o2 = cls2.newInstance();
    ReflectionUtils.unsafeCopy(beanA, o2);
    Fury fury2 =
        furyBuilder()
            .withRefTracking(referenceTracking)
            .withNumberCompressed(compressNumber)
            .withCodegen(enableCodegen3)
            .withClassLoader(cls2.getClassLoader())
            .build();

    MetaContext context1 = new MetaContext();
    MetaContext context2 = new MetaContext();
    fury2.getSerializationContext().setMetaContext(context2);
    byte[] bytes2 = fury2.serialize(o2);
    fury1.getSerializationContext().setMetaContext(context1);
    Object deserialized = fury1.deserialize(bytes2);
    Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(deserialized, o2));
    Assert.assertEquals(deserialized.getClass(), cls1);
    fury1.getSerializationContext().setMetaContext(context1);
    byte[] beanABytes = fury1.serialize(deserialized);
    fury2.getSerializationContext().setMetaContext(context2);
    Assert.assertTrue(ReflectionUtils.objectFieldsEquals(fury2.deserialize(beanABytes), o2));

    fury1.getSerializationContext().setMetaContext(context1);
    byte[] objBytes = fury1.serialize(beanA);
    fury2.getSerializationContext().setMetaContext(context2);
    Object obj2 = fury2.deserialize(objBytes);
    Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(obj2, o2));

    Fury fury =
        furyBuilder()
            .withRefTracking(referenceTracking)
            .withNumberCompressed(compressNumber)
            .withCodegen(enableCodegen1)
            .build();
    // fury <-> fury2 is a new channel, which needs a new context.
    MetaContext context = new MetaContext();
    MetaContext ctx2 = new MetaContext();
    fury.getSerializationContext().setMetaContext(context);
    fury2.getSerializationContext().setMetaContext(ctx2);
    Assert.assertEquals(fury.deserialize(fury2.serialize(beanA)), beanA);
  }

  @Test(dataProvider = "config2")
  public void testWriteCompatibleContainer(
      boolean referenceTracking,
      boolean compressNumber,
      boolean enableCodegen1,
      boolean enableCodegen2)
      throws Exception {
    Fury fury =
        furyBuilder()
            .withRefTracking(referenceTracking)
            .withNumberCompressed(compressNumber)
            .withCodegen(enableCodegen1)
            .build();
    MetaContext context = new MetaContext();
    BeanA beanA = BeanA.createBeanA(2);
    serDeMetaShared(fury, beanA);
    Class<?> cls = ClassUtils.createCompatibleClass1();
    Object newBeanA = cls.newInstance();
    ReflectionUtils.unsafeCopy(beanA, newBeanA);
    Fury newFury =
        furyBuilder()
            .withRefTracking(referenceTracking)
            .withNumberCompressed(compressNumber)
            .withCodegen(enableCodegen2)
            .withClassLoader(cls.getClassLoader())
            .build();
    MetaContext context1 = new MetaContext();
    newFury.getSerializationContext().setMetaContext(context1);
    byte[] newBeanABytes = newFury.serialize(newBeanA);
    fury.getSerializationContext().setMetaContext(context);
    Object deserialized = fury.deserialize(newBeanABytes);
    Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(deserialized, newBeanA));
    Assert.assertEquals(deserialized.getClass(), BeanA.class);
    fury.getSerializationContext().setMetaContext(context);
    byte[] beanABytes = fury.serialize(deserialized);
    newFury.getSerializationContext().setMetaContext(context1);
    Assert.assertTrue(
        ReflectionUtils.objectFieldsEquals(newFury.deserialize(beanABytes), newBeanA));

    fury.getSerializationContext().setMetaContext(context);
    byte[] objBytes = fury.serialize(beanA);
    newFury.getSerializationContext().setMetaContext(context1);
    Object obj2 = newFury.deserialize(objBytes);
    Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(obj2, newBeanA));

    newFury.getSerializationContext().setMetaContext(context1);
    fury.getSerializationContext().setMetaContext(context);
    Assert.assertEquals(fury.deserialize(newFury.serialize(beanA)), beanA);
  }

  @Test(dataProvider = "config2")
  public void testWriteCompatibleCollection(
      boolean referenceTracking,
      boolean compressNumber,
      boolean enableCodegen1,
      boolean enableCodegen2)
      throws Exception {
    Fury fury =
        furyBuilder()
            .withRefTracking(referenceTracking)
            .withNumberCompressed(compressNumber)
            .withCodegen(enableCodegen1)
            .build();
    CollectionFields collectionFields = UnmodifiableSerializersTest.createCollectionFields();
    {
      Object o = serDeMetaShared(fury, collectionFields);
      Object o1 = CollectionFields.copyToCanEqual(o, o.getClass().newInstance());
      Object o2 =
          CollectionFields.copyToCanEqual(
              collectionFields, collectionFields.getClass().newInstance());
      Assert.assertEquals(o1, o2);
    }
    Class<?> cls2 = ClassUtils.createCompatibleClass2();
    Object newObj = cls2.newInstance();
    ReflectionUtils.unsafeCopy(collectionFields, newObj);
    Fury fury2 =
        furyBuilder()
            .withRefTracking(referenceTracking)
            .withNumberCompressed(compressNumber)
            .withCodegen(enableCodegen2)
            .withClassLoader(cls2.getClassLoader())
            .build();
    MetaContext context2 = new MetaContext();
    fury2.getSerializationContext().setMetaContext(context2);
    byte[] bytes1 = fury2.serialize(newObj);
    MetaContext context = new MetaContext();
    fury.getSerializationContext().setMetaContext(context);
    Object deserialized = fury.deserialize(bytes1);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            CollectionFields.copyToCanEqual(deserialized, deserialized.getClass().newInstance()),
            CollectionFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));
    Assert.assertEquals(deserialized.getClass(), CollectionFields.class);

    fury.getSerializationContext().setMetaContext(context);
    byte[] bytes2 = fury.serialize(deserialized);
    fury2.getSerializationContext().setMetaContext(context2);
    Object obj2 = fury2.deserialize(bytes2);
    Assert.assertTrue(
        ReflectionUtils.objectFieldsEquals(
            CollectionFields.copyToCanEqual(obj2, obj2.getClass().newInstance()),
            CollectionFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));

    fury.getSerializationContext().setMetaContext(context);
    byte[] objBytes = fury.serialize(collectionFields);
    fury2.getSerializationContext().setMetaContext(context2);
    Object obj3 = fury2.deserialize(objBytes);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            CollectionFields.copyToCanEqual(obj3, obj3.getClass().newInstance()),
            CollectionFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));

    fury.getSerializationContext().setMetaContext(context);
    fury2.getSerializationContext().setMetaContext(context2);
    Assert.assertEquals(
        ((CollectionFields) (fury.deserialize(fury2.serialize(collectionFields)))).toCanEqual(),
        collectionFields.toCanEqual());
  }

  @Test(dataProvider = "config2")
  public void testWriteCompatibleMap(
      boolean referenceTracking,
      boolean compressNumber,
      boolean enableCodegen1,
      boolean enableCodegen2)
      throws Exception {
    Fury fury =
        furyBuilder()
            .withRefTracking(referenceTracking)
            .withNumberCompressed(compressNumber)
            .withCodegen(enableCodegen1)
            .build();
    MetaContext context = new MetaContext();
    MapFields mapFields = UnmodifiableSerializersTest.createMapFields();
    {
      Object o = serDeMetaShared(fury, mapFields);
      Object o1 = MapFields.copyToCanEqual(o, o.getClass().newInstance());
      Object o2 = MapFields.copyToCanEqual(mapFields, mapFields.getClass().newInstance());
      Assert.assertEquals(o1, o2);
    }
    Class<?> cls = ClassUtils.createCompatibleClass3();
    Object newObj = cls.newInstance();
    ReflectionUtils.unsafeCopy(mapFields, newObj);
    Fury fury2 =
        furyBuilder()
            .withRefTracking(referenceTracking)
            .withNumberCompressed(compressNumber)
            .withCodegen(enableCodegen2)
            .build();
    MetaContext context2 = new MetaContext();
    fury2.getSerializationContext().setMetaContext(context2);
    byte[] bytes1 = fury2.serialize(newObj);
    fury.getSerializationContext().setMetaContext(context);
    Object deserialized = fury.deserialize(bytes1);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            MapFields.copyToCanEqual(deserialized, deserialized.getClass().newInstance()),
            MapFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));
    Assert.assertEquals(deserialized.getClass(), MapFields.class);

    fury.getSerializationContext().setMetaContext(context);
    byte[] bytes2 = fury.serialize(deserialized);
    fury2.getSerializationContext().setMetaContext(context2);
    Object obj2 = fury2.deserialize(bytes2);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            MapFields.copyToCanEqual(obj2, obj2.getClass().newInstance()),
            MapFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));

    fury.getSerializationContext().setMetaContext(context);
    byte[] objBytes = fury.serialize(mapFields);
    fury2.getSerializationContext().setMetaContext(context2);
    Object obj3 = fury2.deserialize(objBytes);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            MapFields.copyToCanEqual(obj3, obj3.getClass().newInstance()),
            MapFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));

    fury2.getSerializationContext().setMetaContext(context2);
    fury.getSerializationContext().setMetaContext(context);
    Assert.assertEquals(
        ((MapFields) (fury.deserialize(fury2.serialize(mapFields)))).toCanEqual(),
        mapFields.toCanEqual());
  }

  public static class DuplicateFieldsClass1 {
    int intField1;
    int intField2;
  }

  @Test(dataProvider = "config2")
  public void testDuplicateFields(
      boolean referenceTracking,
      boolean compressNumber,
      boolean enableCodegen1,
      boolean enableCodegen2)
      throws Exception {
    String pkg = DuplicateFieldsClass1.class.getPackage().getName();
    Class<?> cls1 =
        loadClass(
            pkg,
            "DuplicateFieldsClass2",
            ""
                + "package "
                + pkg
                + ";\n"
                + "import java.util.*;\n"
                + "import java.math.*;\n"
                + "public class DuplicateFieldsClass2 extends MetaSharedCompatibleTest.DuplicateFieldsClass1 {\n"
                + "  int intField1;\n"
                + "}");
    Fury fury =
        furyBuilder()
            .withRefTracking(referenceTracking)
            .withNumberCompressed(compressNumber)
            .withCodegen(enableCodegen1)
            .withClassLoader(cls1.getClassLoader())
            .build();
    MetaContext context = new MetaContext();
    Object o1 = cls1.newInstance();
    for (Field field : ReflectionUtils.getFields(cls1, true)) {
      field.setAccessible(true);
      if (field.getDeclaringClass() == DuplicateFieldsClass1.class) {
        field.setInt(o1, 10);
      } else {
        field.setInt(o1, 100);
      }
    }
    {
      Object o = serDeMetaShared(fury, o1);
      Assert.assertTrue(ReflectionUtils.objectFieldsEquals(o, o1));
    }

    Class<?> cls2 =
        loadClass(
            pkg,
            "DuplicateFieldsClass2",
            ""
                + "package "
                + pkg
                + ";\n"
                + "import java.util.*;\n"
                + "import java.math.*;\n"
                + "public class DuplicateFieldsClass2 extends MetaSharedCompatibleTest.DuplicateFieldsClass1 {\n"
                + "  int intField1;\n"
                + "  int intField2;\n"
                + "}");
    Fury fury2 =
        furyBuilder()
            .withRefTracking(referenceTracking)
            .withNumberCompressed(compressNumber)
            .withCodegen(enableCodegen2)
            .withClassLoader(cls2.getClassLoader())
            .build();
    MetaContext context2 = new MetaContext();
    Object o2 = cls2.newInstance();
    for (Field field : ReflectionUtils.getFields(cls2, true)) {
      field.setAccessible(true);
      if (field.getDeclaringClass() == DuplicateFieldsClass1.class) {
        field.setInt(o2, 10);
      } else {
        field.setInt(o2, 100);
      }
    }
    {
      Object o = serDeMetaShared(fury2, o2);
      Assert.assertTrue(ReflectionUtils.objectFieldsEquals(o, o2));
    }
    {
      fury2.getSerializationContext().setMetaContext(context2);
      fury.getSerializationContext().setMetaContext(context);
      byte[] bytes2 = fury2.serialize(o2);
      Object o = fury.deserialize(bytes2);
      Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(o, o2));
      fury2.getSerializationContext().setMetaContext(context2);
      fury.getSerializationContext().setMetaContext(context);
      Object newObj2 = fury2.deserialize(fury.serialize(o));
      // `DuplicateFieldsClass2.intField2` of `newObj2` will be 0.
      Assert.assertFalse(ReflectionUtils.objectCommonFieldsEquals(newObj2, o2));
      for (Field field : ReflectionUtils.getFields(DuplicateFieldsClass1.class, true)) {
        field.setAccessible(true);
        Assert.assertEquals(field.get(newObj2), field.get(o2));
      }
    }
  }

  @Test(dataProvider = "config1")
  void testEmptySubClass(boolean referenceTracking, boolean compressNumber, boolean enableCodegen)
      throws Exception {
    String pkg = DuplicateFieldsClass1.class.getPackage().getName();
    Class<?> cls1 =
        loadClass(
            pkg,
            "DuplicateFieldsClass2",
            ""
                + "package "
                + pkg
                + ";\n"
                + "import java.util.*;\n"
                + "import java.math.*;\n"
                + "public class DuplicateFieldsClass2 extends MetaSharedCompatibleTest.DuplicateFieldsClass1 {\n"
                + "}");
    Fury fury =
        furyBuilder()
            .withRefTracking(referenceTracking)
            .withNumberCompressed(compressNumber)
            .withCodegen(enableCodegen)
            .withClassLoader(cls1.getClassLoader())
            .build();
    Object o1 = cls1.newInstance();
    for (Field field : ReflectionUtils.getFields(cls1, true)) {
      field.setAccessible(true);
      field.setInt(o1, 10);
    }
    Object o = serDeMetaShared(fury, o1);
    Assert.assertEquals(o.getClass(), o1.getClass());
    Assert.assertTrue(ReflectionUtils.objectFieldsEquals(o, o1));
  }

  @Test
  public void testBigClassNameObject() {
    Fury fury =
        builder()
            .withRefTracking(true)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withScopedMetaShare(false)
            .build();
    Object o =
        new ClassDefEncoderTest
            .TestClassLengthTestClassLengthTestClassLengthTestClassLengthTestClassLengthTestClassLengthTestClassLength
            .InnerClassTestLengthInnerClassTestLengthInnerClassTestLength();
    serDeMetaSharedCheck(fury, o);
  }

  CompileUnit aunit =
      new CompileUnit(
          "demo.pkg1",
          "A",
          (""
              + "package demo.pkg1;\n"
              + "import demo.test.*;\n"
              + "import demo.report.*;\n"
              + "public class A {\n"
              + "  int f1;\n"
              + "  B f2;\n"
              + "  C f3;\n"
              + "  public String toString() {return \"A\" + \",\" + f1 + \",\" + f2 + \",\" + f3;}\n"
              + "  public static A create() { A a = new A(); a.f1 = 10; a.f2 = new B(); a.f3 = new C(); return a;}\n"
              + "}"));

  CompileUnit bunit =
      new CompileUnit(
          "demo.test",
          "B",
          ("" + "package demo.test;\n" + "public class B {\n" + "  public int f1 = 100;\n" + "}"));

  CompileUnit cunit =
      new CompileUnit(
          "demo.report",
          "C",
          (""
              + "package demo.report;\n"
              + "public class C {\n"
              + "  public int f2 = 1000;\n"
              + "  public String toString() {return \"C{f2=\" + f2 + \"}\";}\n"
              + "}"));

  CompileUnit newAUnit =
      new CompileUnit(
          "example.pkg1",
          "A",
          (""
              + "package example.pkg1;\n"
              + "import example.test.*;\n"
              + "import example.report.*;\n"
              + "public class A {\n"
              + "  public int f1;\n"
              + "  public C f3;\n"
              + "  public String toString() {return \"A\" + \",\" + f1 + \",\" + f3;}\n"
              + "  public static A create() { A a = new A(); a.f1 = 10; a.f3 = new C(); return a;}\n"
              + "}"));

  CompileUnit newCUnit =
      new CompileUnit(
          "example.test",
          "C",
          (""
              + "package example.test;\n"
              + "public class C {\n"
              + "  public int f2;\n"
              + "  public String toString() {return \"C{f2=\" + f2 + \"}\";}\n"
              + "}"));

  @Test
  public void testRegisterToSameIdForRenamedClass() throws Exception {
    ClassLoader classLoader =
        JaninoUtils.compile(Thread.currentThread().getContextClassLoader(), aunit, bunit, cunit);
    byte[] serialized;
    {
      Class<?> A = classLoader.loadClass("demo.pkg1.A");
      Class<?> B = classLoader.loadClass("demo.test.B");
      Class<?> C = classLoader.loadClass("demo.report.C");
      Fury fury =
          builder()
              .withCompatibleMode(CompatibleMode.COMPATIBLE)
              .withClassLoader(classLoader)
              .build();
      fury.register(A);
      fury.register(B);
      fury.register(C);
      Object a = A.getMethod("create").invoke(null);
      System.out.println(a);
      serialized = fury.serialize(a);
    }
    {
      CompileUnit unit1 =
          new CompileUnit(
              "example.pkg1",
              "A",
              (""
                  + "package example.pkg1;\n"
                  + "import example.test.*;\n"
                  + "import example.report.*;\n"
                  + "public class A {\n"
                  + "  int f1;\n"
                  + "  B f2;\n"
                  + "  C f3;\n"
                  + "  public String toString() {return \"A\" + \",\" + f1 + \",\" + f2 + \",\" + f3;}\n"
                  + "  public static A create() { A a = new A(); a.f1 = 10; a.f2 = new B(); a.f3 = new C(); return a;}\n"
                  + "}"));
      CompileUnit unit2 =
          new CompileUnit(
              "example.report",
              "B",
              ("" + "package example.report;\n" + "public class B {\n" + "}"));
      CompileUnit unit3 =
          new CompileUnit(
              "example.test", "C", ("" + "package example.test;\n" + "public class C {\n" + "}"));
      classLoader =
          JaninoUtils.compile(Thread.currentThread().getContextClassLoader(), unit1, unit2, unit3);
      Class<?> A = classLoader.loadClass("example.pkg1.A");
      Class<?> B = classLoader.loadClass("example.report.B");
      Class<?> C = classLoader.loadClass("example.test.C");
      Fury fury =
          builder()
              .withCompatibleMode(CompatibleMode.COMPATIBLE)
              .withClassLoader(classLoader)
              .build();
      fury.register(A);
      fury.register(B);
      fury.register(C);
      Object newObj = fury.deserialize(serialized);
      System.out.println(newObj);
    }
  }

  @Test
  public void testInconsistentRegistrationID() throws Exception {
    ClassLoader classLoader =
        JaninoUtils.compile(Thread.currentThread().getContextClassLoader(), aunit, bunit, cunit);
    byte[] serialized;
    {
      Class<?> A = classLoader.loadClass("demo.pkg1.A");
      Class<?> B = classLoader.loadClass("demo.test.B");
      Class<?> C = classLoader.loadClass("demo.report.C");
      Fury fury =
          builder()
              .withCompatibleMode(CompatibleMode.COMPATIBLE)
              .withClassLoader(classLoader)
              .build();
      fury.register(A, 300);
      fury.register(B, 301);
      fury.register(C, 302);
      Object a = A.getMethod("create").invoke(null);
      System.out.println(a);
      serialized = fury.serialize(a);
    }
    {
      classLoader =
          JaninoUtils.compile(Thread.currentThread().getContextClassLoader(), newAUnit, newCUnit);
      Class<?> A = classLoader.loadClass("example.pkg1.A");
      Class<?> C = classLoader.loadClass("example.test.C");
      Fury fury =
          builder()
              .withCompatibleMode(CompatibleMode.COMPATIBLE)
              .withClassLoader(classLoader)
              .build();
      fury.register(A, 300);
      fury.register(C, 302);
      Object newObj = fury.deserialize(serialized);
      System.out.println(newObj);
      Object f3 = getObjectFieldValue(newObj, "f3");
      Assert.assertNotNull(f3);
      Assert.assertEquals(f3.toString(), "C{f2=1000}");
    }
  }

  @Test
  public void testInconsistentRegistrationName() throws Exception {
    ClassLoader classLoader =
        JaninoUtils.compile(Thread.currentThread().getContextClassLoader(), aunit, bunit, cunit);
    byte[] serialized;
    {
      Class<?> A = classLoader.loadClass("demo.pkg1.A");
      Class<?> B = classLoader.loadClass("demo.test.B");
      Class<?> C = classLoader.loadClass("demo.report.C");
      Fury fury =
          builder()
              .withCompatibleMode(CompatibleMode.COMPATIBLE)
              .withClassLoader(classLoader)
              .build();
      fury.register(A, "test.A");
      fury.register(B, "test.B");
      fury.register(C, "test.C");
      Object a = A.getMethod("create").invoke(null);
      System.out.println(a);
      serialized = fury.serialize(a);
    }
    {
      classLoader =
          JaninoUtils.compile(Thread.currentThread().getContextClassLoader(), newAUnit, newCUnit);
      Class<?> A = classLoader.loadClass("example.pkg1.A");
      Class<?> C = classLoader.loadClass("example.test.C");
      Fury fury =
          builder()
              .withCompatibleMode(CompatibleMode.COMPATIBLE)
              .withClassLoader(classLoader)
              .build();
      fury.register(A, "test.A");
      fury.register(C, "test.C");
      Object newObj = fury.deserialize(serialized);
      System.out.println(newObj);
      Object f3 = getObjectFieldValue(newObj, "f3");
      Assert.assertNotNull(f3);
      Assert.assertEquals(f3.toString(), "C{f2=1000}");
    }
  }
}
