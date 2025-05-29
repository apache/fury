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

import static org.apache.fory.reflect.ReflectionUtils.getObjectFieldValue;
import static org.apache.fory.serializer.ClassUtils.loadClass;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.fory.Fory;
import org.apache.fory.ForyTestBase;
import org.apache.fory.builder.MetaSharedCodecBuilder;
import org.apache.fory.codegen.CompileUnit;
import org.apache.fory.codegen.JaninoUtils;
import org.apache.fory.config.CompatibleMode;
import org.apache.fory.config.ForyBuilder;
import org.apache.fory.config.Language;
import org.apache.fory.meta.ClassDefEncoderTest;
import org.apache.fory.reflect.ReflectionUtils;
import org.apache.fory.resolver.MetaContext;
import org.apache.fory.serializer.collection.UnmodifiableSerializersTest;
import org.apache.fory.test.bean.BeanA;
import org.apache.fory.test.bean.BeanB;
import org.apache.fory.test.bean.CollectionFields;
import org.apache.fory.test.bean.Foo;
import org.apache.fory.test.bean.MapFields;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests for {@link MetaSharedCodecBuilder} and {@link MetaSharedSerializer}, and protocol
 * interoperability between them.
 */
public class MetaSharedCompatibleTest extends ForyTestBase {
  public static Object serDeMetaSharedCheck(Fory fory, Object obj) {
    Object newObj = serDeMetaShared(fory, obj);
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

  private static ForyBuilder foryBuilder() {
    return builder()
        .withLanguage(Language.JAVA)
        .withMetaShare(true)
        .withCompatibleMode(CompatibleMode.COMPATIBLE)
        .requireClassRegistration(false)
        .withScopedMetaShare(false);
  }

  @Test(dataProvider = "config1")
  public void testWrite(boolean referenceTracking, boolean compressNumber, boolean enableCodegen) {
    Fory fory =
        foryBuilder()
            .withNumberCompressed(compressNumber)
            .withRefTracking(referenceTracking)
            .withCodegen(enableCodegen)
            .build();
    serDeMetaSharedCheck(fory, Foo.create());
    serDeMetaSharedCheck(fory, BeanB.createBeanB(2));
    serDeMetaSharedCheck(fory, BeanA.createBeanA(2));
  }

  @Test(dataProvider = "config2")
  public void testWriteCompatibleBasic(
      boolean referenceTracking,
      boolean compressNumber,
      boolean enableCodegen1,
      boolean enableCodegen2)
      throws Exception {
    Fory fory =
        foryBuilder()
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
      Fory newFury =
          foryBuilder()
              .withRefTracking(referenceTracking)
              .withNumberCompressed(compressNumber)
              .withCodegen(enableCodegen2)
              .withClassLoader(fooClass.getClassLoader())
              .build();
      MetaContext context1 = new MetaContext();
      {
        newFury.getSerializationContext().setMetaContext(context1);
        byte[] foo1Bytes = newFury.serialize(newFoo);
        fory.getSerializationContext().setMetaContext(context);
        Object deserialized = fory.deserialize(foo1Bytes);
        Assert.assertEquals(deserialized.getClass(), Foo.class);
        Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(deserialized, newFoo));
        fory.getSerializationContext().setMetaContext(context);
        byte[] fooBytes = fory.serialize(deserialized);
        newFury.getSerializationContext().setMetaContext(context1);
        Assert.assertTrue(
            ReflectionUtils.objectFieldsEquals(newFury.deserialize(fooBytes), newFoo));
      }
      {
        fory.getSerializationContext().setMetaContext(context);
        byte[] bytes1 = fory.serialize(foo);
        newFury.getSerializationContext().setMetaContext(context1);
        Object o1 = newFury.deserialize(bytes1);
        Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(o1, foo));
        fory.getSerializationContext().setMetaContext(context);
        newFury.getSerializationContext().setMetaContext(context1);
        Object o2 = fory.deserialize(newFury.serialize(o1));
        List<String> fields =
            Arrays.stream(fooClass.getDeclaredFields())
                .map(f -> f.getDeclaringClass().getSimpleName() + f.getName())
                .collect(Collectors.toList());
        Assert.assertTrue(ReflectionUtils.objectFieldsEquals(new HashSet<>(fields), o2, foo));
      }
      {
        fory.getSerializationContext().setMetaContext(context);
        newFury.getSerializationContext().setMetaContext(context1);
        Object o3 = fory.deserialize(newFury.serialize(foo));
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
    Fory fury1 =
        foryBuilder()
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
    Fory fury2 =
        foryBuilder()
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
    Fory fury1 =
        foryBuilder()
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
    Fory fury2 =
        foryBuilder()
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

    Fory fory =
        foryBuilder()
            .withRefTracking(referenceTracking)
            .withNumberCompressed(compressNumber)
            .withCodegen(enableCodegen1)
            .build();
    // fory <-> fury2 is a new channel, which needs a new context.
    MetaContext context = new MetaContext();
    MetaContext ctx2 = new MetaContext();
    fory.getSerializationContext().setMetaContext(context);
    fury2.getSerializationContext().setMetaContext(ctx2);
    Assert.assertEquals(fory.deserialize(fury2.serialize(beanA)), beanA);
  }

  @Test(dataProvider = "config2")
  public void testWriteCompatibleContainer(
      boolean referenceTracking,
      boolean compressNumber,
      boolean enableCodegen1,
      boolean enableCodegen2)
      throws Exception {
    Fory fory =
        foryBuilder()
            .withRefTracking(referenceTracking)
            .withNumberCompressed(compressNumber)
            .withCodegen(enableCodegen1)
            .build();
    MetaContext context = new MetaContext();
    BeanA beanA = BeanA.createBeanA(2);
    serDeMetaShared(fory, beanA);
    Class<?> cls = ClassUtils.createCompatibleClass1();
    Object newBeanA = cls.newInstance();
    ReflectionUtils.unsafeCopy(beanA, newBeanA);
    Fory newFury =
        foryBuilder()
            .withRefTracking(referenceTracking)
            .withNumberCompressed(compressNumber)
            .withCodegen(enableCodegen2)
            .withClassLoader(cls.getClassLoader())
            .build();
    MetaContext context1 = new MetaContext();
    newFury.getSerializationContext().setMetaContext(context1);
    byte[] newBeanABytes = newFury.serialize(newBeanA);
    fory.getSerializationContext().setMetaContext(context);
    Object deserialized = fory.deserialize(newBeanABytes);
    Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(deserialized, newBeanA));
    Assert.assertEquals(deserialized.getClass(), BeanA.class);
    fory.getSerializationContext().setMetaContext(context);
    byte[] beanABytes = fory.serialize(deserialized);
    newFury.getSerializationContext().setMetaContext(context1);
    Assert.assertTrue(
        ReflectionUtils.objectFieldsEquals(newFury.deserialize(beanABytes), newBeanA));

    fory.getSerializationContext().setMetaContext(context);
    byte[] objBytes = fory.serialize(beanA);
    newFury.getSerializationContext().setMetaContext(context1);
    Object obj2 = newFury.deserialize(objBytes);
    Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(obj2, newBeanA));

    newFury.getSerializationContext().setMetaContext(context1);
    fory.getSerializationContext().setMetaContext(context);
    Assert.assertEquals(fory.deserialize(newFury.serialize(beanA)), beanA);
  }

  @Test(dataProvider = "config2")
  public void testWriteCompatibleCollection(
      boolean referenceTracking,
      boolean compressNumber,
      boolean enableCodegen1,
      boolean enableCodegen2)
      throws Exception {
    Fory fory =
        foryBuilder()
            .withRefTracking(referenceTracking)
            .withNumberCompressed(compressNumber)
            .withCodegen(enableCodegen1)
            .build();
    CollectionFields collectionFields = UnmodifiableSerializersTest.createCollectionFields();
    {
      Object o = serDeMetaShared(fory, collectionFields);
      Object o1 = CollectionFields.copyToCanEqual(o, o.getClass().newInstance());
      Object o2 =
          CollectionFields.copyToCanEqual(
              collectionFields, collectionFields.getClass().newInstance());
      Assert.assertEquals(o1, o2);
    }
    Class<?> cls2 = ClassUtils.createCompatibleClass2();
    Object newObj = cls2.newInstance();
    ReflectionUtils.unsafeCopy(collectionFields, newObj);
    Fory fury2 =
        foryBuilder()
            .withRefTracking(referenceTracking)
            .withNumberCompressed(compressNumber)
            .withCodegen(enableCodegen2)
            .withClassLoader(cls2.getClassLoader())
            .build();
    MetaContext context2 = new MetaContext();
    fury2.getSerializationContext().setMetaContext(context2);
    byte[] bytes1 = fury2.serialize(newObj);
    MetaContext context = new MetaContext();
    fory.getSerializationContext().setMetaContext(context);
    Object deserialized = fory.deserialize(bytes1);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            CollectionFields.copyToCanEqual(deserialized, deserialized.getClass().newInstance()),
            CollectionFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));
    Assert.assertEquals(deserialized.getClass(), CollectionFields.class);

    fory.getSerializationContext().setMetaContext(context);
    byte[] bytes2 = fory.serialize(deserialized);
    fury2.getSerializationContext().setMetaContext(context2);
    Object obj2 = fury2.deserialize(bytes2);
    Assert.assertTrue(
        ReflectionUtils.objectFieldsEquals(
            CollectionFields.copyToCanEqual(obj2, obj2.getClass().newInstance()),
            CollectionFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));

    fory.getSerializationContext().setMetaContext(context);
    byte[] objBytes = fory.serialize(collectionFields);
    fury2.getSerializationContext().setMetaContext(context2);
    Object obj3 = fury2.deserialize(objBytes);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            CollectionFields.copyToCanEqual(obj3, obj3.getClass().newInstance()),
            CollectionFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));

    fory.getSerializationContext().setMetaContext(context);
    fury2.getSerializationContext().setMetaContext(context2);
    Assert.assertEquals(
        ((CollectionFields) (fory.deserialize(fury2.serialize(collectionFields)))).toCanEqual(),
        collectionFields.toCanEqual());
  }

  @Test(dataProvider = "config2")
  public void testWriteCompatibleMap(
      boolean referenceTracking,
      boolean compressNumber,
      boolean enableCodegen1,
      boolean enableCodegen2)
      throws Exception {
    Fory fory =
        foryBuilder()
            .withRefTracking(referenceTracking)
            .withNumberCompressed(compressNumber)
            .withCodegen(enableCodegen1)
            .build();
    MetaContext context = new MetaContext();
    MapFields mapFields = UnmodifiableSerializersTest.createMapFields();
    {
      Object o = serDeMetaShared(fory, mapFields);
      Object o1 = MapFields.copyToCanEqual(o, o.getClass().newInstance());
      Object o2 = MapFields.copyToCanEqual(mapFields, mapFields.getClass().newInstance());
      Assert.assertEquals(o1, o2);
    }
    Class<?> cls = ClassUtils.createCompatibleClass3();
    Object newObj = cls.newInstance();
    ReflectionUtils.unsafeCopy(mapFields, newObj);
    Fory fury2 =
        foryBuilder()
            .withRefTracking(referenceTracking)
            .withNumberCompressed(compressNumber)
            .withCodegen(enableCodegen2)
            .build();
    MetaContext context2 = new MetaContext();
    fury2.getSerializationContext().setMetaContext(context2);
    byte[] bytes1 = fury2.serialize(newObj);
    fory.getSerializationContext().setMetaContext(context);
    Object deserialized = fory.deserialize(bytes1);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            MapFields.copyToCanEqual(deserialized, deserialized.getClass().newInstance()),
            MapFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));
    Assert.assertEquals(deserialized.getClass(), MapFields.class);

    fory.getSerializationContext().setMetaContext(context);
    byte[] bytes2 = fory.serialize(deserialized);
    fury2.getSerializationContext().setMetaContext(context2);
    Object obj2 = fury2.deserialize(bytes2);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            MapFields.copyToCanEqual(obj2, obj2.getClass().newInstance()),
            MapFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));

    fory.getSerializationContext().setMetaContext(context);
    byte[] objBytes = fory.serialize(mapFields);
    fury2.getSerializationContext().setMetaContext(context2);
    Object obj3 = fury2.deserialize(objBytes);
    Assert.assertTrue(
        ReflectionUtils.objectCommonFieldsEquals(
            MapFields.copyToCanEqual(obj3, obj3.getClass().newInstance()),
            MapFields.copyToCanEqual(newObj, newObj.getClass().newInstance())));

    fury2.getSerializationContext().setMetaContext(context2);
    fory.getSerializationContext().setMetaContext(context);
    Assert.assertEquals(
        ((MapFields) (fory.deserialize(fury2.serialize(mapFields)))).toCanEqual(),
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
    Fory fory =
        foryBuilder()
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
      Object o = serDeMetaShared(fory, o1);
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
    Fory fury2 =
        foryBuilder()
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
      fory.getSerializationContext().setMetaContext(context);
      byte[] bytes2 = fury2.serialize(o2);
      Object o = fory.deserialize(bytes2);
      Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(o, o2));
      fury2.getSerializationContext().setMetaContext(context2);
      fory.getSerializationContext().setMetaContext(context);
      Object newObj2 = fury2.deserialize(fory.serialize(o));
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
    Fory fory =
        foryBuilder()
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
    Object o = serDeMetaShared(fory, o1);
    Assert.assertEquals(o.getClass(), o1.getClass());
    Assert.assertTrue(ReflectionUtils.objectFieldsEquals(o, o1));
  }

  @Test
  public void testBigClassNameObject() {
    Fory fory =
        builder()
            .withRefTracking(true)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withScopedMetaShare(false)
            .build();
    Object o =
        new ClassDefEncoderTest
            .TestClassLengthTestClassLengthTestClassLengthTestClassLengthTestClassLengthTestClassLengthTestClassLength
            .InnerClassTestLengthInnerClassTestLengthInnerClassTestLength();
    serDeMetaSharedCheck(fory, o);
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
      Fory fory =
          builder()
              .withCompatibleMode(CompatibleMode.COMPATIBLE)
              .withClassLoader(classLoader)
              .build();
      fory.register(A);
      fory.register(B);
      fory.register(C);
      Object a = A.getMethod("create").invoke(null);
      System.out.println(a);
      serialized = fory.serialize(a);
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
      Fory fory =
          builder()
              .withCompatibleMode(CompatibleMode.COMPATIBLE)
              .withClassLoader(classLoader)
              .build();
      fory.register(A);
      fory.register(B);
      fory.register(C);
      Object newObj = fory.deserialize(serialized);
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
      Fory fory =
          builder()
              .withCompatibleMode(CompatibleMode.COMPATIBLE)
              .withClassLoader(classLoader)
              .build();
      fory.register(A, 300);
      fory.register(B, 301);
      fory.register(C, 302);
      Object a = A.getMethod("create").invoke(null);
      System.out.println(a);
      serialized = fory.serialize(a);
    }
    {
      classLoader =
          JaninoUtils.compile(Thread.currentThread().getContextClassLoader(), newAUnit, newCUnit);
      Class<?> A = classLoader.loadClass("example.pkg1.A");
      Class<?> C = classLoader.loadClass("example.test.C");
      Fory fory =
          builder()
              .withCompatibleMode(CompatibleMode.COMPATIBLE)
              .withClassLoader(classLoader)
              .build();
      fory.register(A, 300);
      fory.register(C, 302);
      Object newObj = fory.deserialize(serialized);
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
      Fory fory =
          builder()
              .withCompatibleMode(CompatibleMode.COMPATIBLE)
              .withClassLoader(classLoader)
              .build();
      fory.register(A, "test.A");
      fory.register(B, "test.B");
      fory.register(C, "test.C");
      Object a = A.getMethod("create").invoke(null);
      System.out.println(a);
      serialized = fory.serialize(a);
    }
    {
      classLoader =
          JaninoUtils.compile(Thread.currentThread().getContextClassLoader(), newAUnit, newCUnit);
      Class<?> A = classLoader.loadClass("example.pkg1.A");
      Class<?> C = classLoader.loadClass("example.test.C");
      Fory fory =
          builder()
              .withCompatibleMode(CompatibleMode.COMPATIBLE)
              .withClassLoader(classLoader)
              .build();
      fory.register(A, "test.A");
      fory.register(C, "test.C");
      Object newObj = fory.deserialize(serialized);
      System.out.println(newObj);
      Object f3 = getObjectFieldValue(newObj, "f3");
      Assert.assertNotNull(f3);
      Assert.assertEquals(f3.toString(), "C{f2=1000}");
    }
  }

  @Test
  public void testInheritance() throws Exception {
    // See issue https://github.com/apache/fory/issues/2210
    CompileUnit u1 =
        new CompileUnit(
            "example.test",
            "Parent",
            (""
                + "package example.test;\n"
                + "public class Parent {\n"
                + "  public Integer f1;\n"
                + "  public Integer f2;\n"
                + "}"));
    CompileUnit u2 =
        new CompileUnit(
            "example.test",
            "Child",
            (""
                + "package example.test;\n"
                + "public class Child extends Parent {\n"
                + "  public Integer f3;\n"
                + "}"));
    ClassLoader classLoader1 =
        JaninoUtils.compile(Thread.currentThread().getContextClassLoader(), u1, u2);
    Object o = classLoader1.loadClass("example.test.Child").newInstance();
    ReflectionUtils.setObjectFieldValue(o, "f1", 10);
    ReflectionUtils.setObjectFieldValue(o, "f2", 20);
    ReflectionUtils.setObjectFieldValue(o, "f3", 30);
    Fory fory =
        builder()
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withClassLoader(classLoader1)
            .build();
    fory.register(classLoader1.loadClass("example.test.Child"));
    fory.register(classLoader1.loadClass("example.test.Parent"));
    byte[] serialized = fory.serialize(o);
    CompileUnit u11 =
        new CompileUnit(
            "example.test",
            "Parent",
            (""
                + "package example.test;\n"
                + "public class Parent {\n"
                + "  public Integer f2;\n"
                + "}"));
    ClassLoader classLoader2 = JaninoUtils.compile(getClass().getClassLoader(), u11, u2);
    Fory fury2 =
        builder()
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withClassLoader(classLoader2)
            .build();
    fury2.register(classLoader2.loadClass("example.test.Child"));
    fury2.register(classLoader2.loadClass("example.test.Parent"));
    Object o1 = fury2.deserialize(serialized);
    Assert.assertNull(getObjectFieldValue(o1, "f1"));
    Assert.assertEquals(ReflectionUtils.getObjectFieldValue(o1, "f2"), 20);
    Assert.assertEquals(ReflectionUtils.getObjectFieldValue(o1, "f3"), 30);
  }
}
