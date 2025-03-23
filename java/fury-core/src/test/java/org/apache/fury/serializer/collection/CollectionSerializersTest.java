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

package org.apache.fury.serializer.collection;

import static org.apache.fury.collection.Collections.ofArrayList;
import static org.apache.fury.collection.Collections.ofHashMap;
import static org.apache.fury.collection.Collections.ofHashSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.LongStream;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.Language;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.reflect.TypeRef;
import org.apache.fury.serializer.collection.CollectionSerializers.JDKCompatibleCollectionSerializer;
import org.apache.fury.test.bean.Cyclic;
import org.apache.fury.type.GenericType;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.collections.Maps;

@SuppressWarnings("rawtypes")
public class CollectionSerializersTest extends FuryTestBase {

  @Test(dataProvider = "referenceTrackingConfig")
  public void testBasicList(boolean referenceTrackingConfig) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .build();
    List<String> data = new ArrayList<>(ImmutableList.of("a", "b", "c"));
    serDeCheckSerializer(fury, data, "ArrayList");
    serDeCheckSerializer(fury, Arrays.asList("a", "b", "c"), "ArraysAsList");
    serDeCheckSerializer(fury, new HashSet<>(data), "HashSet");
    serDeCheckSerializer(fury, new LinkedHashSet<>(data), "LinkedHashSet");
  }

  @Test(dataProvider = "furyCopyConfig")
  public void testBasicList(Fury fury) {
    List<Object> data = Arrays.asList(1, true, "test", Cyclic.create(true));
    copyCheck(fury, new ArrayList<>(data));
    copyCheck(fury, data);
    copyCheck(fury, new HashSet<>(data));
    copyCheck(fury, new LinkedHashSet<>(data));
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testBasicListNested(boolean referenceTrackingConfig) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .build();
    List<String> data0 = new ArrayList<>(ImmutableList.of("a", "b", "c"));
    List<List<String>> data = new ArrayList<>(ImmutableList.of(data0, data0));
    serDeCheckSerializer(fury, data, "ArrayList");
    serDeCheckSerializer(fury, Arrays.asList("a", "b", "c"), "ArraysAsList");
    serDeCheckSerializer(fury, new HashSet<>(data), "HashSet");
    serDeCheckSerializer(fury, new LinkedHashSet<>(data), "LinkedHashSet");
  }

  @Data
  public static class BasicListNestedJIT {
    public Set<List<List<String>>> data;
    public Set<Set<List<String>>> data1;
    public Collection<ArrayList<Object>> data2;
    public Collection<Set<Collection<String>>> data3;
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testBasicListNestedJIT(boolean referenceTracking) {
    Fury fury =
        Fury.builder()
            .withRefTracking(referenceTracking)
            .withCodegen(true)
            .requireClassRegistration(false)
            .build();
    List<List<List<String>>> list = new ArrayList<>();
    list.add(ofArrayList(ofArrayList("a", "b")));
    list.add(ofArrayList(ofArrayList("a", "b")));
    BasicListNestedJIT o = new BasicListNestedJIT();
    o.data = new HashSet<>(list);
    o.data1 = ofHashSet(ofHashSet(ofArrayList("a", "b")), ofHashSet(ofArrayList("a", "b")));
    o.data2 = ofHashSet(ofArrayList("a", "b"));
    o.data3 = ofHashSet(ofHashSet(ofArrayList("a", "b")), ofHashSet(ofArrayList("a", "b")));
    serDeCheckSerializer(fury, o, "Codec");
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testCollectionGenerics(boolean referenceTrackingConfig) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .build();
    List<String> data = new ArrayList<>(ImmutableList.of("a", "b", "c"));
    byte[] bytes1 = fury.serialize(data);
    fury.getGenerics().pushGenericType(GenericType.build(new TypeRef<List<String>>() {}));
    byte[] bytes2 = fury.serialize(data);
    Assert.assertTrue(bytes1.length > bytes2.length);
    assertEquals(fury.deserialize(bytes2), data);
    fury.getGenerics().popGenericType();
    Assert.assertThrows(RuntimeException.class, () -> fury.deserialize(bytes2));
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testSortedSet(boolean referenceTrackingConfig) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
            .build();
    // Test serialize Comparator
    TreeSet<String> set =
        new TreeSet<>(
            (Comparator<? super String> & Serializable)
                (s1, s2) -> {
                  int delta = s1.length() - s2.length();
                  if (delta == 0) {
                    return s1.compareTo(s2);
                  } else {
                    return delta;
                  }
                });
    set.add("str11");
    set.add("str2");
    assertEquals(set, serDe(fury, set));
    TreeSet<String> data = new TreeSet<>(ImmutableSet.of("a", "b", "c"));
    serDeCheckSerializer(fury, data, "SortedSet");
    byte[] bytes1 = fury.serialize(data);
    fury.getGenerics().pushGenericType(GenericType.build(new TypeRef<List<String>>() {}));
    byte[] bytes2 = fury.serialize(data);
    Assert.assertTrue(bytes1.length > bytes2.length);
    fury.getGenerics().popGenericType();
    Assert.assertThrows(RuntimeException.class, () -> fury.deserialize(bytes2));
  }

  @Test(dataProvider = "furyCopyConfig")
  public void testSortedSet(Fury fury) {
    AtomicInteger i = new AtomicInteger(1);
    TreeSet<String> set = new TreeSet<>(new TestComparator(i));

    TreeSet<String> copy = fury.copy(set);

    Comparator<? super String> comparator1 = set.comparator();
    Comparator<? super String> comparator2 = copy.comparator();
    Assert.assertEquals(comparator1, comparator2);
    set.add("str11");
    copy.add("str11");
    Assert.assertEquals(copy, set);
    Assert.assertNotSame(copy, set);
    i.set(-1);
    Assert.assertNotEquals(comparator1, comparator2);
    set.add("str2");
    copy.add("str2");
    Assert.assertNotEquals(Arrays.toString(copy.toArray()), Arrays.toString(set.toArray()));
    copy.add("str");
    Assert.assertEquals(Arrays.toString(copy.toArray()), "[str, str2, str11]");
  }

  private class TestComparator implements Comparator<String> {
    AtomicInteger i;

    public TestComparator(AtomicInteger i) {
      this.i = i;
    }

    @Override
    public int compare(String s1, String s2) {
      int delta = s1.length() - s2.length();
      if (delta == i.get()) {
        return s1.compareTo(s2);
      } else {
        return delta;
      }
    }

    @Override
    public boolean equals(Object obj) {
      return ((TestComparator) obj).i.get() == this.i.get();
    }
  }

  @Test
  public void testEmptyCollection() {
    serDeCheckSerializer(getJavaFury(), Collections.EMPTY_LIST, "EmptyListSerializer");
    serDeCheckSerializer(getJavaFury(), Collections.emptySortedSet(), "EmptySortedSetSerializer");
    serDeCheckSerializer(getJavaFury(), Collections.EMPTY_SET, "EmptySetSerializer");
  }

  @Test(dataProvider = "furyCopyConfig")
  public void testEmptyCollection(Fury fury) {
    copyCheckWithoutSame(fury, Collections.EMPTY_LIST);
    copyCheckWithoutSame(fury, Collections.emptySortedSet());
    copyCheckWithoutSame(fury, Collections.EMPTY_SET);
  }

  @Test
  public void testSingleCollection() {
    serDeCheckSerializer(getJavaFury(), Collections.singletonList(1), "SingletonList");
    serDeCheckSerializer(getJavaFury(), Collections.singleton(1), "SingletonSet");
  }

  @Test(dataProvider = "furyCopyConfig")
  public void testSingleCollection(Fury fury) {
    copyCheck(fury, Collections.singletonList(Cyclic.create(true)));
    copyCheck(fury, Collections.singleton(Cyclic.create(true)));
  }

  @Test
  public void tesSkipList() {
    serDeCheckSerializer(
        getJavaFury(),
        new ConcurrentSkipListSet<>(Arrays.asList("a", "b", "c")),
        "ConcurrentSkipListSet");
  }

  @Test(dataProvider = "furyCopyConfig")
  public void tesSkipList(Fury fury) {
    copyCheck(fury, new ConcurrentSkipListSet<>(Arrays.asList("a", "b", "c")));
  }

  @Test
  public void tesVectorSerializer() {
    serDeCheckSerializer(
        getJavaFury(), new Vector<>(Arrays.asList("a", "b", "c")), "VectorSerializer");
  }

  @Test(dataProvider = "furyCopyConfig")
  public void tesVectorSerializer(Fury fury) {
    copyCheck(fury, new Vector<>(Arrays.asList("a", 1, Cyclic.create(true))));
  }

  @Test
  public void tesArrayDequeSerializer() {
    serDeCheckSerializer(
        getJavaFury(), new ArrayDeque<>(Arrays.asList("a", "b", "c")), "ArrayDeque");
  }

  @Test(dataProvider = "furyCopyConfig")
  public void tesArrayDequeSerializer(Fury fury) {
    ImmutableList<Object> list = ImmutableList.of("a", 1, Cyclic.create(true));
    ArrayDeque<Object> deque = new ArrayDeque<>(list);
    ArrayDeque<Object> copy = fury.copy(deque);
    Assert.assertEquals(ImmutableList.copyOf(copy), list);
    Assert.assertNotSame(deque, copy);
  }

  public enum TestEnum {
    A,
    B,
    C,
    D
  }

  @Test
  public void tesEnumSetSerializer() {
    serDe(getJavaFury(), EnumSet.allOf(TestEnum.class));
    Assert.assertEquals(
        getJavaFury()
            .getClassResolver()
            .getSerializerClass(EnumSet.allOf(TestEnum.class).getClass()),
        CollectionSerializers.EnumSetSerializer.class);
    serDe(getJavaFury(), EnumSet.of(TestEnum.A));
    Assert.assertEquals(
        getJavaFury().getClassResolver().getSerializerClass(EnumSet.of(TestEnum.A).getClass()),
        CollectionSerializers.EnumSetSerializer.class);
    serDe(getJavaFury(), EnumSet.of(TestEnum.A, TestEnum.B));
    Assert.assertEquals(
        getJavaFury()
            .getClassResolver()
            .getSerializerClass(EnumSet.of(TestEnum.A, TestEnum.B).getClass()),
        CollectionSerializers.EnumSetSerializer.class);
    // TODO test enum which has enums exceed 128.
  }

  @Test(dataProvider = "furyCopyConfig")
  public void tesEnumSetSerializer(Fury fury) {
    copyCheck(fury, EnumSet.allOf(TestEnum.class));
    copyCheck(fury, EnumSet.of(TestEnum.A));
    copyCheck(fury, EnumSet.of(TestEnum.A, TestEnum.B));
  }

  @Test
  public void tesBitSetSerializer() {
    serDe(getJavaFury(), BitSet.valueOf(LongStream.range(0, 2).toArray()));
    Assert.assertEquals(
        getJavaFury()
            .getClassResolver()
            .getSerializerClass(BitSet.valueOf(LongStream.range(0, 2).toArray()).getClass()),
        CollectionSerializers.BitSetSerializer.class);
    serDe(getJavaFury(), BitSet.valueOf(LongStream.range(0, 128).toArray()));
    Assert.assertEquals(
        getJavaFury()
            .getClassResolver()
            .getSerializerClass(BitSet.valueOf(LongStream.range(0, 128).toArray()).getClass()),
        CollectionSerializers.BitSetSerializer.class);
  }

  @Test(dataProvider = "furyCopyConfig")
  public void tesBitSetSerializer(Fury fury) {
    copyCheck(fury, BitSet.valueOf(LongStream.range(0, 2).toArray()));
    copyCheck(fury, BitSet.valueOf(LongStream.range(0, 128).toArray()));
  }

  @Test
  public void tesPriorityQueueSerializer() {
    serDe(getJavaFury(), new PriorityQueue<>(Arrays.asList("a", "b", "c")));
    Assert.assertEquals(
        getJavaFury().getClassResolver().getSerializerClass(PriorityQueue.class),
        CollectionSerializers.PriorityQueueSerializer.class);
  }

  @Test(dataProvider = "furyCopyConfig")
  public void tesPriorityQueueSerializer(Fury fury) {
    ImmutableList<String> list = ImmutableList.of("a", "b", "c");
    PriorityQueue<String> copy = fury.copy(new PriorityQueue<>(list));
    Assert.assertEquals(ImmutableList.copyOf(copy), list);
  }

  @Test
  public void testCopyOnWriteArrayList() {
    final CopyOnWriteArrayList<String> list =
        new CopyOnWriteArrayList<>(new String[] {"a", "b", "c"});
    Assert.assertEquals(list, serDe(getJavaFury(), list));
    Assert.assertEquals(
        getJavaFury().getClassResolver().getSerializerClass(CopyOnWriteArrayList.class),
        CollectionSerializers.CopyOnWriteArrayListSerializer.class);
  }

  @Test(dataProvider = "furyCopyConfig")
  public void testCopyOnWriteArrayList(Fury fury) {
    final CopyOnWriteArrayList<Object> list =
        new CopyOnWriteArrayList<>(new Object[] {"a", "b", Cyclic.create(true)});
    copyCheck(fury, list);
  }

  @Data
  @AllArgsConstructor
  public static class CollectionViewTestStruct {
    Collection<String> collection;
    Set<String> set;
  }

  @Test(dataProvider = "javaFury")
  public void testSetFromMap(Fury fury) {
    Set<String> set = Collections.newSetFromMap(Maps.newConcurrentMap());
    set.add("a");
    set.add("b");
    set.add("c");
    serDeCheck(fury, set);
    Assert.assertEquals(
        fury.getClassResolver().getSerializerClass(set.getClass()),
        CollectionSerializers.SetFromMapSerializer.class);
    CollectionViewTestStruct struct1 = serDeCheck(fury, new CollectionViewTestStruct(set, set));
    if (fury.trackingRef()) {
      assertSame(struct1.collection, struct1.set);
    }
    set = Collections.newSetFromMap(new HashMap<String, Boolean>() {});
    set.add("a");
    set.add("b");
    serDeCheck(fury, set);
    CollectionViewTestStruct struct2 = serDeCheck(fury, new CollectionViewTestStruct(set, set));
    if (fury.trackingRef()) {
      assertSame(struct2.collection, struct2.set);
    }
  }

  @Test(dataProvider = "furyCopyConfig")
  public void testSetFromMapCopy(Fury fury) {
    final Set<Object> set = Collections.newSetFromMap(Maps.newConcurrentMap());
    set.add("a");
    set.add("b");
    set.add(Cyclic.create(true));
    copyCheck(fury, set);
  }

  @Test(dataProvider = "javaFury")
  public void testConcurrentMapKeySetViewMap(Fury fury) {
    final ConcurrentHashMap.KeySetView<String, Boolean> set = ConcurrentHashMap.newKeySet();
    set.add("a");
    set.add("b");
    set.add("c");
    Assert.assertEquals(set, serDe(fury, set));
    Assert.assertEquals(
        fury.getClassResolver().getSerializerClass(set.getClass()),
        CollectionSerializers.ConcurrentHashMapKeySetViewSerializer.class);
    CollectionViewTestStruct o = serDeCheck(fury, new CollectionViewTestStruct(set, set));
    if (fury.trackingRef()) {
      assertSame(o.collection, o.set);
    }
    ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
    map.put("k1", "v1");
    if (fury.trackingRef()) {
      ArrayList<Serializable> list = serDeCheck(fury, ofArrayList(map.keySet("v0"), map));
      assertSame(((ConcurrentHashMap.KeySetView) (list.get(0))).getMap(), list.get(1));
    }
  }

  @Test(dataProvider = "furyCopyConfig")
  public void testConcurrentMapKeySetViewMapCopy(Fury fury) {
    final ConcurrentHashMap.KeySetView<Object, Boolean> set = ConcurrentHashMap.newKeySet();
    set.add("a");
    set.add("b");
    set.add(Cyclic.create(true));
    copyCheck(fury, set);
  }

  @Test
  public void testSerializeJavaBlockingQueue() {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .build();
    // TODO(chaokunyang) add optimized serializers for blocking queue.
    {
      ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<>(10);
      queue.add(1);
      queue.add(2);
      queue.add(3);
      assertEquals(new ArrayList<>(serDe(fury, queue)), new ArrayList<>(queue));
    }
    {
      // If reference tracking is off, deserialization will throw
      // `java.lang.IllegalMonitorStateException`
      // when using fury `ObjectStreamSerializer`, maybe some internal state are shared.
      LinkedBlockingQueue<Integer> queue = new LinkedBlockingQueue<>(10);
      queue.add(1);
      queue.add(2);
      queue.add(3);
      assertEquals(new ArrayList<>(serDe(fury, queue)), new ArrayList<>(queue));
    }
  }

  @Test(dataProvider = "furyCopyConfig")
  public void testSerializeJavaBlockingQueue(Fury fury) {
    {
      ArrayBlockingQueue<Integer> queue = new ArrayBlockingQueue<>(10);
      queue.add(1);
      queue.add(2);
      queue.add(3);
      ArrayBlockingQueue<Integer> copy = fury.copy(queue);
      Assert.assertEquals(Arrays.toString(copy.toArray()), "[1, 2, 3]");
    }
    {
      // If reference tracking is off, deserialization will throw
      // `java.lang.IllegalMonitorStateException`
      // when using fury `ObjectStreamSerializer`, maybe some internal state are shared.
      LinkedBlockingQueue<Integer> queue = new LinkedBlockingQueue<>(10);
      queue.add(1);
      queue.add(2);
      queue.add(3);
      LinkedBlockingQueue<Integer> copy = fury.copy(queue);
      Assert.assertEquals(Arrays.toString(copy.toArray()), "[1, 2, 3]");
    }
  }

  @Test
  public void testCollectionNoJIT() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).withCodegen(false).build();
    serDeCheck(fury, new ArrayList<>(ImmutableList.of("a", "b", "c")));
    serDeCheck(fury, new ArrayList<>(ImmutableList.of(1, 2, 3)));
    serDeCheck(fury, new ArrayList<>(ImmutableList.of("a", 1, "b", 2)));
  }

  @Data
  public static class SimpleBeanCollectionFields {
    public List<String> list;
  }

  @Test(dataProvider = "javaFury")
  public void testSimpleBeanCollectionFields(Fury fury) {
    SimpleBeanCollectionFields obj = new SimpleBeanCollectionFields();
    obj.list = new ArrayList<>();
    obj.list.add("a");
    obj.list.add("b");
    Assert.assertEquals(serDe(fury, obj).toString(), obj.toString());
    if (fury.getConfig().isCodeGenEnabled()) {
      Assert.assertTrue(
          fury.getClassResolver()
              .getSerializerClass(SimpleBeanCollectionFields.class)
              .getName()
              .contains("Codec"));
    }
  }

  @Test(dataProvider = "furyCopyConfig")
  public void testSimpleBeanCollectionFieldsCopy(Fury fury) {
    SimpleBeanCollectionFields obj = new SimpleBeanCollectionFields();
    obj.list = new ArrayList<>();
    obj.list.add("a");
    obj.list.add("b");
    Assert.assertEquals(fury.copy(obj).toString(), obj.toString());
  }

  @Data
  @AllArgsConstructor
  public static class NotFinal {
    int f1;
  }

  @Data
  public static class Container {
    public List<NotFinal> list1;
    public Map<String, NotFinal> map1;
  }

  @Test(dataProvider = "javaFury")
  public void testContainer(Fury fury) {
    Container container = new Container();
    container.list1 = ofArrayList(new NotFinal(1));
    container.map1 = ofHashMap("k", new NotFinal(2));
    serDeCheck(fury, container);
  }

  @Test(dataProvider = "furyCopyConfig")
  public void testContainerCopy(Fury fury) {
    Container container = new Container();
    container.list1 = ofArrayList(new NotFinal(1));
    container.map1 = ofHashMap("k", new NotFinal(2));
    copyCheck(fury, container);
  }

  @Data
  public static class CollectionFieldsClass {
    public ArrayList<String> arrayList;
    public List<String> arrayList2;
    public Collection<String> arrayList3;
    public List<String> arrayAsList;
    public Collection<String> arrayAsList2;
    public LinkedList<String> linkedList;
    public List<String> linkedList2;
    public Collection<String> linkedList3;
    public HashSet<String> hashSet;
    public Set<String> hashSet2;
    public LinkedHashSet<String> linkedHashSet;
    public Set<String> linkedHashSet2;
    public TreeSet<String> treeSet;
    public SortedSet<String> treeSet2;
    public ConcurrentSkipListSet<String> skipListSet;
    public Set<String> skipListSet2;
    public Vector<String> vector;
    public List<String> vector2;
    public ArrayDeque<Integer> arrayDeque;
    public Collection<Integer> arrayDeque2;
    public BitSet bitSet1;
    public BitSet bitSet2;
    public PriorityQueue<String> priorityQueue;
    public Collection<String> priorityQueue2;
    public EnumSet<TestEnum> enumSet1;
    public EnumSet<TestEnum> enumSet2;
    public List<String> emptyList1;
    public Set<String> emptySet1;
    // TODO add support for common emtpy
    public Set<String> emptySortedSet;
    public List<String> singleList1;
    public Set<String> singleSet1;
  }

  public static CollectionFieldsClass createCollectionFieldsObject() {
    CollectionFieldsClass obj = new CollectionFieldsClass();
    ArrayList<String> arrayList = new ArrayList<>(ImmutableList.of("a", "b"));
    obj.arrayList = arrayList;
    obj.arrayList2 = arrayList;
    obj.arrayList3 = arrayList;
    obj.arrayAsList = Arrays.asList("a", "b");
    obj.arrayAsList2 = Arrays.asList("a", "b");
    LinkedList<String> linkedList = new LinkedList<>(Arrays.asList("a", "b"));
    obj.linkedList = linkedList;
    obj.linkedList2 = linkedList;
    obj.linkedList3 = linkedList;
    HashSet<String> hashSet = new HashSet<>(ImmutableSet.of("a", "b"));
    obj.hashSet = hashSet;
    obj.hashSet2 = hashSet;
    obj.linkedHashSet = new LinkedHashSet<>(hashSet);
    obj.linkedHashSet2 = new LinkedHashSet<>(hashSet);
    obj.treeSet = new TreeSet<>(hashSet);
    obj.treeSet2 = new TreeSet<>(hashSet);
    ConcurrentSkipListSet<String> skipListSet =
        new ConcurrentSkipListSet<>(Arrays.asList("a", "b", "c"));
    obj.skipListSet = skipListSet;
    obj.skipListSet2 = skipListSet;
    Vector<String> vector = new Vector<>(Arrays.asList("a", "b", "c"));
    obj.vector = vector;
    obj.vector2 = vector;
    ArrayDeque<Integer> arrayDeque = new ArrayDeque<>(Arrays.asList(1, 2));
    obj.arrayDeque = arrayDeque;
    obj.arrayDeque2 = arrayDeque;
    obj.bitSet1 = BitSet.valueOf(LongStream.range(0, 2).toArray());
    obj.bitSet2 = BitSet.valueOf(LongStream.range(0, 128).toArray());
    PriorityQueue<String> priorityQueue = new PriorityQueue<>(Arrays.asList("a", "b", "c"));
    obj.priorityQueue = priorityQueue;
    obj.priorityQueue2 = priorityQueue;
    obj.enumSet1 = EnumSet.allOf(TestEnum.class);
    obj.enumSet2 = EnumSet.of(TestEnum.A);
    obj.emptyList1 = Collections.emptyList();
    obj.emptySet1 = Collections.emptySet();
    obj.emptySortedSet = Collections.emptySortedSet();
    obj.singleList1 = Collections.singletonList("");
    obj.singleSet1 = Collections.singleton("");
    return obj;
  }

  @Test(dataProvider = "javaFury")
  public void testCollectionFieldSerializers(Fury fury) {
    CollectionFieldsClass obj = createCollectionFieldsObject();
    Assert.assertEquals(serDe(fury, obj).toString(), obj.toString());
    if (fury.getConfig().isCodeGenEnabled()) {
      Assert.assertTrue(
          fury.getClassResolver()
              .getSerializerClass(CollectionFieldsClass.class)
              .getName()
              .contains("Codec"));
    }
  }

  @Test(dataProvider = "furyCopyConfig")
  public void testCollectionFieldSerializersCopy(Fury fury) {
    CollectionFieldsClass obj = createCollectionFieldsObject();
    Assert.assertEquals(fury.copy(obj).toString(), obj.toString());
  }

  @Data
  @AllArgsConstructor
  public static class NestedCollection1 {
    public List<Collection<Integer>> list1;
  }

  @Test(dataProvider = "javaFury")
  public void testNestedCollection1(Fury fury) {
    ArrayList<Integer> list = ofArrayList(1, 2);
    NestedCollection1 o = new NestedCollection1(ofArrayList(list));
    Assert.assertEquals(serDe(fury, o), o);
  }

  @Test(dataProvider = "furyCopyConfig")
  public void testNestedCollection1Copy(Fury fury) {
    ArrayList<Integer> list = ofArrayList(1, 2);
    NestedCollection1 o = new NestedCollection1(ofArrayList(list));
    copyCheck(fury, o);
  }

  @Data
  @AllArgsConstructor
  public static class NestedCollection2 {
    public List<Collection<Collection<Integer>>> list1;
  }

  @Test(dataProvider = "javaFury")
  public void testNestedCollection2(Fury fury) {
    ArrayList<Integer> list = ofArrayList(1, 2);
    NestedCollection2 o = new NestedCollection2(ofArrayList(ofArrayList(list)));
    Assert.assertEquals(serDe(fury, o), o);
  }

  @Test(dataProvider = "furyCopyConfig")
  public void testNestedCollection2Copy(Fury fury) {
    ArrayList<Integer> list = ofArrayList(1, 2);
    NestedCollection2 o = new NestedCollection2(ofArrayList(ofArrayList(list)));
    copyCheck(fury, o);
  }

  public static class TestClassForDefaultCollectionSerializer extends AbstractCollection<String> {
    private final List<String> data = new ArrayList<>();

    @Override
    public Iterator<String> iterator() {
      return data.iterator();
    }

    @Override
    public int size() {
      return data.size();
    }

    @Override
    public boolean add(String s) {
      return data.add(s);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TestClassForDefaultCollectionSerializer strings = (TestClassForDefaultCollectionSerializer) o;
      return Objects.equals(data, strings.data);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(data);
    }
  }

  @Test
  public void testDefaultCollectionSerializer() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
    TestClassForDefaultCollectionSerializer collection =
        new TestClassForDefaultCollectionSerializer();
    collection.add("a");
    collection.add("b");
    serDeCheck(fury, collection);
    Assert.assertSame(
        fury.getClassResolver().getSerializerClass(TestClassForDefaultCollectionSerializer.class),
        CollectionSerializers.DefaultJavaCollectionSerializer.class);
  }

  @Test(dataProvider = "furyCopyConfig")
  public void testDefaultCollectionSerializer(Fury fury) {
    TestClassForDefaultCollectionSerializer collection =
        new TestClassForDefaultCollectionSerializer();
    collection.add("a");
    collection.add("b");
    copyCheck(fury, collection);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testJavaSerialization() {
    ImmutableSortedSet<Integer> set = ImmutableSortedSet.of(1, 2, 3);
    Class<? extends ImmutableSortedSet> setClass = set.getClass();
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(false)
            .requireClassRegistration(false)
            .build();
    MemoryBuffer buffer = MemoryUtils.buffer(32);
    JDKCompatibleCollectionSerializer javaSerializer =
        new JDKCompatibleCollectionSerializer(fury, setClass);
    javaSerializer.write(buffer, set);
    Object read = javaSerializer.read(buffer);
    assertEquals(set, read);

    assertSame(
        fury.getClassResolver().getSerializer(setClass).getClass(),
        GuavaCollectionSerializers.ImmutableSortedSetSerializer.class);
    buffer.writerIndex(0);
    buffer.readerIndex(0);
    assertEquals(set, fury.deserialize(fury.serialize(buffer, set)));
  }

  @Test(dataProvider = "furyCopyConfig")
  public void testJavaSerialization(Fury fury) {
    ImmutableSortedSet<Integer> set = ImmutableSortedSet.of(1, 2, 3);
    Class<? extends ImmutableSortedSet> setClass = set.getClass();
    JDKCompatibleCollectionSerializer javaSerializer =
        new JDKCompatibleCollectionSerializer(fury, setClass);
    Object copy = javaSerializer.copy(set);
    assertEquals(set, copy);
    Assert.assertNotSame(set, copy);
  }

  public static class SubListSerializer extends CollectionSerializer {

    public SubListSerializer(Fury fury, Class cls) {
      super(fury, cls, true);
    }

    @Override
    public Collection newCollection(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32();
      setNumElements(numElements);
      return new ArrayList<>(numElements);
    }
  }

  @Test
  public void testSubList() {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .requireClassRegistration(false)
            .withJdkClassSerializableCheck(false)
            .build();
    ArrayList<Integer> list = new ArrayList<>(ImmutableList.of(1, 2, 3, 4));
    fury.registerSerializer(
        list.subList(0, 2).getClass(), new SubListSerializer(fury, list.subList(0, 2).getClass()));
    serDeCheck(fury, list.subList(0, 2));

    //     ArrayList<Integer> list = new ArrayList<>(ImmutableList.of(1, 2, 3, 4));
    //     ByteArrayOutputStream bas = new ByteArrayOutputStream();
    //     Hessian2Output hessian2Output = new Hessian2Output(bas);
    //     serialize(hessian2Output, list.subList(0 ,2));
    //     Object o = deserialize(new Hessian2Input(new ByteArrayInputStream(bas.toByteArray())));
    //     System.out.println(o.getClass());
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testCollectionNullElements(boolean refTracking) {
    // When serialize a collection with all elements null directly, the declare type
    // will be equal to element type: null
    List data = new ArrayList<>();
    data.add(null);
    Fury f = Fury.builder().withLanguage(Language.JAVA).withRefTracking(refTracking).build();
    serDeCheck(f, data);
  }

  @Test(dataProvider = "furyCopyConfig")
  public void testCollectionNullElements(Fury fury) {
    List data = new ArrayList<>();
    data.add(null);
    copyCheck(fury, data);
  }

  @Data
  abstract static class Foo {
    private int f1;
  }

  static class Foo1 extends Foo {}

  @Data
  static class CollectionAbstractTest {
    private List<Foo> fooList;
  }

  @Test(dataProvider = "enableCodegen")
  public void testAbstractCollectionElementsSerialization(boolean enableCodegen) {
    Fury fury = Fury.builder().withCodegen(enableCodegen).requireClassRegistration(false).build();
    {
      CollectionAbstractTest test = new CollectionAbstractTest();
      test.fooList = new ArrayList<>(ImmutableList.of(new Foo1(), new Foo1()));
      serDeCheck(fury, test);
    }
    {
      CollectionAbstractTest test = new CollectionAbstractTest();
      test.fooList = new ArrayList<>(ofArrayList(new Foo1(), new Foo1()));
      serDeCheck(fury, test);
    }
  }

  @Test(dataProvider = "furyCopyConfig")
  public void testAbstractCollectionElementsSerialization(Fury fury) {
    {
      CollectionAbstractTest test = new CollectionAbstractTest();
      test.fooList = new ArrayList<>(ImmutableList.of(new Foo1(), new Foo1()));
      copyCheck(fury, test);
    }
    {
      CollectionAbstractTest test = new CollectionAbstractTest();
      test.fooList = new ArrayList<>(ofArrayList(new Foo1(), new Foo1()));
      copyCheck(fury, test);
    }
  }

  @Test(dataProvider = "enableCodegen")
  public void testCollectionAllNullElements(boolean enableCodegen) {
    Fury fury =
        Fury.builder()
            .withCodegen(true)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .build();
    List<Foo> fooList = new ArrayList<>();
    fooList.add(null);
    // serDeCheck(fury, fooList);

    CollectionAbstractTest obj = new CollectionAbstractTest();
    // fill elemTypeCache
    obj.fooList = ofArrayList(new Foo1());
    serDeCheck(fury, obj);

    obj.fooList = fooList;
    serDeCheck(fury, obj);

    fury =
        Fury.builder()
            .withCodegen(enableCodegen)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .build();
    serDeCheck(fury, obj);
  }
}
