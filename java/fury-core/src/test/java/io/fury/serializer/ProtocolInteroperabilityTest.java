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

package io.fury.serializer;

import static com.google.common.collect.ImmutableList.of;
import static io.fury.TestUtils.mapOf;
import static io.fury.collection.Collections.ofArrayList;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.fury.Fury;
import io.fury.FuryTestBase;
import io.fury.Language;
import io.fury.resolver.MetaContext;
import io.fury.test.bean.BeanA;
import io.fury.test.bean.BeanB;
import io.fury.test.bean.MapFields;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/** Protocol interoperability test between interpreter mode and jit mode. */
public class ProtocolInteroperabilityTest extends FuryTestBase {
  @DataProvider(name = "fury")
  public static Object[][] fury() {
    return Sets.cartesianProduct(
            ImmutableSet.of(true, false), // referenceTracking
            ImmutableSet.of(true, false), // compressNumber
            ImmutableSet.of(CompatibleMode.SCHEMA_CONSISTENT, CompatibleMode.COMPATIBLE))
        .stream()
        .map(List::toArray)
        .map(
            c ->
                new Object[] {
                  Fury.builder()
                      .withLanguage(Language.JAVA)
                      .withRefTracking((Boolean) c[0])
                      .withNumberCompressed((Boolean) c[1])
                      .withCodegen(false)
                      .withCompatibleMode((CompatibleMode) c[2])
                      .requireClassRegistration(false)
                      .build(),
                  Fury.builder()
                      .withLanguage(Language.JAVA)
                      .withRefTracking((Boolean) c[0])
                      .withNumberCompressed((Boolean) c[1])
                      .withCodegen(true)
                      .withCompatibleMode((CompatibleMode) c[2])
                      .requireClassRegistration(false)
                      .build()
                })
        .toArray(Object[][]::new);
  }

  @Data
  @AllArgsConstructor
  public static class GenericBoundTest {
    public List<Collection<Collection<Integer>>> list1;
    public List<? extends Collection> list2;
    public List<? extends Collection<? extends Collection<Integer>>> list3;
  }

  @Test(dataProvider = "fury")
  public void testGenericCollectionBound(Fury fury, Fury furyJIT) {
    ArrayList<Integer> list = ofArrayList(1, 2);
    roundCheck(
        fury,
        furyJIT,
        new GenericBoundTest(
            ofArrayList(ofArrayList(list)),
            of(list),
            new ArrayList<>(of(new ArrayList<>(of(list))))));
  }

  @Data
  @AllArgsConstructor
  public static class GenericMapBoundTest {
    public Map<Map<Integer, Collection<Integer>>, Integer> map1;
    public Map<? extends Map, Integer> map2;
    public Map<? extends Map<Integer, ? extends Collection<Integer>>, Integer> map3;
  }

  @Test(dataProvider = "fury")
  public void testGenericMapBound(Fury fury, Fury furyJIT) {
    ArrayList<Integer> list = new ArrayList<>(of(1, 2));
    roundCheck(
        fury,
        furyJIT,
        new GenericMapBoundTest(
            new HashMap<>(mapOf(new HashMap<>(mapOf(1, list)), 1)),
            new HashMap<>(mapOf(new HashMap<>(mapOf(1, list)), 1)),
            new HashMap<>(mapOf(new HashMap<>(mapOf(1, list)), 1))));
  }

  @Data
  @AllArgsConstructor
  public static class SimpleCollectionTest {
    public List<Integer> integerList;
    public Collection<String> stringList;
  }

  @Test(dataProvider = "fury")
  public void testSimpleCollection(Fury fury, Fury furyJIT) {
    roundCheck(
        fury,
        furyJIT,
        new SimpleCollectionTest(new ArrayList<>(of(1, 2)), new LinkedList<>(of("a", "b"))));
    roundCheck(fury, furyJIT, new SimpleCollectionTest(of(1, 2), new HashSet<>(of("a", "b"))));
    roundCheck(fury, furyJIT, new SimpleCollectionTest(Arrays.asList(1, 2), of("a", "b")));
    roundCheck(fury, furyJIT, new SimpleCollectionTest(of(1, 2), new TreeSet<>(of("a", "b"))));
  }

  @Data
  @AllArgsConstructor
  public static class SimpleMapTest {
    public Map<String, Integer> map1;
    public Map<Integer, Integer> map2;
  }

  @Test(dataProvider = "fury")
  public void testSimpleMap(Fury fury, Fury furyJIT) {
    roundCheck(
        fury,
        furyJIT,
        new SimpleMapTest(new HashMap<>(mapOf("k", 2)), new LinkedHashMap<>(mapOf(1, 2))));
    roundCheck(fury, furyJIT, new SimpleMapTest(mapOf("k", 2), new TreeMap<>(mapOf(1, 2))));
  }

  @Data
  @AllArgsConstructor
  public static class NestedCollectionTest {
    public Collection<Integer> list1;
    public List<Collection<Collection<Integer>>> list2;
  }

  @Test(dataProvider = "fury")
  public void testNestedCollection(Fury fury, Fury furyJIT) {
    ArrayList<Integer> list = new ArrayList<>(of(1, 2));
    roundCheck(
        fury,
        furyJIT,
        new NestedCollectionTest(
            new ArrayList<>(list), new ArrayList<>(of(new ArrayList<>(of(list))))));
    roundCheck(
        fury,
        furyJIT,
        new NestedCollectionTest(
            new LinkedList<>(list), new ArrayList<>(of(new ArrayList<>(of(list))))));
  }

  @Data
  @AllArgsConstructor
  public static class NestedMapTest {
    public Map<Map<Integer, Map<Integer, Integer>>, Map<Integer, Integer>> map1;
    public Map<Integer, Integer> map2;
  }

  @Test(dataProvider = "fury")
  public void testNestedMap(Fury fury, Fury furyJIT) {
    roundCheck(
        fury,
        furyJIT,
        new NestedMapTest(
            new HashMap<>(mapOf(mapOf(1, new HashMap<>(mapOf(1, 2))), mapOf(1, 2))),
            new HashMap<>(mapOf(1, 2))));
  }

  @Test(dataProvider = "fury")
  public void testSimplePojo(Fury fury, Fury furyJIT) {
    roundCheck(fury, furyJIT, BeanB.createBeanB(2));
  }

  @Test(dataProvider = "fury")
  public void testNestedPojo(Fury fury, Fury furyJIT) {
    roundCheck(fury, furyJIT, BeanA.createBeanA(2));
  }

  @Test(dataProvider = "fury")
  public void testComplexCollection(Fury fury, Fury furyJIT) {
    CollectionSerializersTest.CollectionFieldsClass o =
        CollectionSerializersTest.createCollectionFieldsObject();
    roundCheck(fury, furyJIT, o, Object::toString);
  }

  @Test(dataProvider = "fury")
  public void testComplexMap(Fury fury, Fury furyJIT) {
    MapFields o = MapSerializersTest.createMapFieldsObject();
    roundCheck(fury, furyJIT, o);
  }

  @DataProvider(name = "metaShareFury")
  public static Object[][] metaShareFury() {
    return Sets.cartesianProduct(
            ImmutableSet.of(CompatibleMode.COMPATIBLE) // structFieldsRepeat
            )
        .stream()
        .map(List::toArray)
        .map(
            c ->
                new Object[] {
                  Fury.builder()
                      .withLanguage(Language.JAVA)
                      .withMetaContextShare(true)
                      .withCompatibleMode((CompatibleMode) c[0])
                      .withCodegen(false)
                      .requireClassRegistration(false)
                      .build(),
                  Fury.builder()
                      .withLanguage(Language.JAVA)
                      .withMetaContextShare(true)
                      .withCompatibleMode((CompatibleMode) c[0])
                      .withCodegen(true)
                      .requireClassRegistration(false)
                      .build()
                })
        .toArray(Object[][]::new);
  }

  @Test(dataProvider = "metaShareFury")
  public void testSimpleCollectionTestMetaShare(Fury fury, Fury furyJIT) {
    metaShareRoundCheck(
        fury, furyJIT, new SimpleCollectionTest(of(1, 2), new HashSet<>(of("a", "b"))));
  }

  @Test(dataProvider = "metaShareFury")
  public void testSimplePojoMetaShare(Fury fury, Fury furyJIT) {
    metaShareRoundCheck(fury, furyJIT, BeanB.createBeanB(2));
  }

  @Data
  @AllArgsConstructor
  public static class InnerPojo {
    public int anInt;
  }

  @Data
  @AllArgsConstructor
  public static class OuterPojo1 {
    public List<Integer> integers;
    public List<InnerPojo> list;
  }

  @Data
  @AllArgsConstructor
  public static class OuterPojo2 {
    public List<Integer> integers;
    public List<BeanB> list;
  }

  @Test(dataProvider = "metaShareFury")
  public void testSimpleNestedPojoMetaShare(Fury fury, Fury furyJIT) {
    metaShareRoundCheck(
        fury,
        furyJIT,
        new OuterPojo1(ofArrayList(1, 2), ofArrayList(new InnerPojo(10), new InnerPojo(10))));
    metaShareRoundCheck(
        fury,
        furyJIT,
        new OuterPojo2(new ArrayList<>(of(1, 2)), new ArrayList<>(of(BeanB.createBeanB(2)))));
  }

  @Test(dataProvider = "metaShareFury")
  public void testNestedPojoMetaShare(Fury fury, Fury furyJIT) {
    metaShareRoundCheck(fury, furyJIT, BeanA.createBeanA(2));
  }

  @Data
  @AllArgsConstructor
  public static class InnerTypeSerializerNotCreated {
    public int[][] intArray;
    public AtomicBoolean[] booleans;
  }

  @Test(dataProvider = "metaShareFury")
  public void testInnerTypeSerializerNotCreated(Fury fury, Fury furyJIT) {
    metaShareRoundCheck(
        fury,
        furyJIT,
        new InnerTypeSerializerNotCreated(
            new int[][] {{1, 2}, {3, 4}}, new AtomicBoolean[] {new AtomicBoolean(true)}),
        Object::toString);
  }

  private static void metaShareRoundCheck(Fury fury1, Fury fury2, Object o) {
    metaShareRoundCheck(fury1, fury2, o, Function.identity());
  }

  private static void metaShareRoundCheck(
      Fury fury1, Fury fury2, Object o, Function<Object, Object> compareHook) {
    MetaContext context1 = new MetaContext();
    MetaContext context2 = new MetaContext();
    for (int i = 0; i < 2; i++) {
      fury1.getSerializationContext().setMetaContext(context1);
      byte[] bytes1 = fury1.serialize(o);
      fury2.getSerializationContext().setMetaContext(context2);
      Object o1 = fury2.deserialize(bytes1);
      Assert.assertEquals(compareHook.apply(o1), compareHook.apply(o));
      fury2.getSerializationContext().setMetaContext(context2);
      byte[] bytes2 = fury2.serialize(o1);
      fury1.getSerializationContext().setMetaContext(context1);
      Object o2 = fury1.deserialize(bytes2);
      Assert.assertEquals(compareHook.apply(o2), compareHook.apply(o));
    }
  }
}
