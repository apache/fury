/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.builder;

import static io.fury.collection.Collections.ofArrayList;
import static io.fury.collection.Collections.ofHashMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.fury.Fury;
import io.fury.FuryTestBase;
import io.fury.Language;
import io.fury.codegen.CodeGenerator;
import io.fury.codegen.CompileUnit;
import io.fury.codegen.JaninoUtils;
import io.fury.serializer.CollectionSerializersTest;
import io.fury.test.bean.AccessBeans;
import io.fury.test.bean.BeanA;
import io.fury.test.bean.BeanB;
import io.fury.test.bean.Foo;
import io.fury.test.bean.Struct;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import org.codehaus.janino.ByteArrayClassLoader;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ObjectCodecBuilderTest extends FuryTestBase {

  @Test(dataProvider = "compressNumber")
  public void genCode(boolean compressNumber) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(false)
            .withNumberCompressed(compressNumber)
            .requireClassRegistration(false)
            .build();
    new ObjectCodecBuilder(Foo.class, fury).genCode();
    // System.out.println(code);
    new ObjectCodecBuilder(BeanA.class, fury).genCode();
    new ObjectCodecBuilder(BeanB.class, fury).genCode();
    new ObjectCodecBuilder(Struct.createStructClass("ObjectCodecBuilderTestStruct", 1), fury)
        .genCode();
  }

  @Test
  public void testDefaultPackage() throws Exception {
    CompileUnit unit =
        new CompileUnit(
            "",
            "A",
            ("" + "public class A {\n" + "  public int f1;\n" + "  public Object f2;\n" + "}"));
    ByteArrayClassLoader classLoader =
        JaninoUtils.compile(Thread.currentThread().getContextClassLoader(), unit);
    Class<?> clz = classLoader.loadClass("A");
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .withClassLoader(clz.getClassLoader())
            .requireClassRegistration(false)
            .build();
    Object obj = clz.newInstance();
    Field f1 = clz.getDeclaredField("f1");
    f1.setAccessible(true);
    f1.set(obj, 2);
    Object newObj = fury.deserialize(fury.serialize(obj));
    Assert.assertEquals(f1.get(newObj), 2);
  }

  @DataProvider(name = "codecConfig")
  public static Object[][] codecConfig() {
    return Sets.cartesianProduct(
            ImmutableSet.of(true, false), // referenceTracking
            ImmutableSet.of(true, false), // basicTypesReferenceIgnored
            ImmutableSet.of(true, false), // compressNumber
            ImmutableSet.of(1, 4, 7) // structFieldsRepeat
            )
        .stream()
        .map(List::toArray)
        .toArray(Object[][]::new);
  }

  @Test(dataProvider = "codecConfig")
  public void testSeqCodec(
      boolean referenceTracking,
      boolean basicTypesRefIgnored,
      boolean compressNumber,
      int fieldsRepeat) {
    Class<?> structClass = Struct.createStructClass("Struct" + fieldsRepeat, fieldsRepeat);
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTracking)
            .withClassLoader(structClass.getClassLoader())
            .ignoreBasicTypesRef(basicTypesRefIgnored)
            .withNumberCompressed(compressNumber)
            .requireClassRegistration(false)
            .build();
    Object struct = Struct.createPOJO(structClass);
    Assert.assertEquals(fury.deserialize(fury.serialize(struct)), struct);
    checkMethodSize(structClass, fury);
  }

  private void checkMethodSize(Class<?> clz, Fury fury) {
    ObjectCodecBuilder codecBuilder = new ObjectCodecBuilder(clz, fury);
    CompileUnit compileUnit =
        new CompileUnit(
            CodeGenerator.getPackage(clz), codecBuilder.codecClassName(clz), codecBuilder::genCode);
    byte[] bytecode =
        JaninoUtils.toBytecode(clz.getClassLoader(), compileUnit).values().iterator().next();
    JaninoUtils.CodeStats classStats = JaninoUtils.getClassStats(bytecode);
    System.out.println(classStats);
    classStats.methodsSize.entrySet().stream()
        .filter(e -> !e.getKey().equals("<init>"))
        .forEach(
            e ->
                Assert.assertTrue(
                    e.getValue() < 325,
                    String.format(
                        "Method %s for class %s has size %d > 325",
                        e.getKey(), compileUnit.getQualifiedClassName(), e.getValue())));
  }

  @Test
  public void testContainer() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
    CollectionSerializersTest.Container container = new CollectionSerializersTest.Container();
    container.list1 = ofArrayList(new CollectionSerializersTest.NotFinal(1));
    container.map1 = ofHashMap("k", new CollectionSerializersTest.NotFinal(2));
    serDeCheck(fury, container);
    checkMethodSize(NestedContainer.class, fury);
  }

  @Data
  public static class NestedContainer {
    private List<List<String>> list1;
    public Map<String, Set<List<String>>> map1;
  }

  @Test
  public void testNestedContainer() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
    NestedContainer nestedContainer = new NestedContainer();
    List<List<String>> list1 = new ArrayList<>();
    list1.add(new ArrayList<>(ImmutableList.of("a", "b")));
    list1.add(new ArrayList<>(ImmutableList.of("a", "b")));
    nestedContainer.list1 = list1;
    Map<String, Set<List<String>>> map1 = new HashMap<>();
    map1.put("k1", new HashSet<>(list1));
    map1.put("k2", new HashSet<>(list1));
    nestedContainer.map1 = map1;
    serDeCheck(fury, nestedContainer);
    checkMethodSize(NestedContainer.class, fury);
  }

  @Test
  public void testAccessLevel() {
    Fury fury = Fury.builder().requireClassRegistration(false).build();
    AccessBeans.PublicClass object = AccessBeans.createPublicClassObject();
    serDeCheckSerializer(fury, object, "Codec");
  }
}
