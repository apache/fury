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
import com.google.common.reflect.TypeToken;
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
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.LongStream;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.Language;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.serializer.collection.CollectionSerializers.JDKCompatibleCollectionSerializer;
import org.apache.fury.type.GenericType;
import org.testng.Assert;
import org.testng.annotations.Test;

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
    fury.getGenerics().pushGenericType(GenericType.build(new TypeToken<List<String>>() {}));
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
    fury.getGenerics().pushGenericType(GenericType.build(new TypeToken<List<String>>() {}));
    byte[] bytes2 = fury.serialize(data);
    Assert.assertTrue(bytes1.length > bytes2.length);
    fury.getGenerics().popGenericType();
    Assert.assertThrows(RuntimeException.class, () -> fury.deserialize(bytes2));
  }

  @Test
  public void testEmptyCollection() {
    serDeCheckSerializer(getJavaFury(), Collections.EMPTY_LIST, "EmptyListSerializer");
    serDeCheckSerializer(getJavaFury(), Collections.emptySortedSet(), "EmptySortedSetSerializer");
    serDeCheckSerializer(getJavaFury(), Collections.EMPTY_SET, "EmptySetSerializer");
  }

  @Test
  public void testSingleCollection() {
    serDeCheckSerializer(getJavaFury(), Collections.singletonList(1), "SingletonList");
    serDeCheckSerializer(getJavaFury(), Collections.singleton(1), "SingletonSet");
  }

  @Test
  public void tesSkipList() {
    serDeCheckSerializer(
        getJavaFury(),
        new ConcurrentSkipListSet<>(Arrays.asList("a", "b", "c")),
        "ConcurrentSkipListSet");
  }

  @Test
  public void tesVectorSerializer() {
    serDeCheckSerializer(
        getJavaFury(), new Vector<>(Arrays.asList("a", "b", "c")), "VectorSerializer");
  }

  @Test
  public void tesArrayDequeSerializer() {
    serDeCheckSerializer(
        getJavaFury(), new ArrayDeque<>(Arrays.asList("a", "b", "c")), "ArrayDeque");
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

  @Test
  public void tesPriorityQueueSerializer() {
    serDe(getJavaFury(), new PriorityQueue<>(Arrays.asList("a", "b", "c")));
    Assert.assertEquals(
        getJavaFury().getClassResolver().getSerializerClass(PriorityQueue.class),
        CollectionSerializers.PriorityQueueSerializer.class);
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
}
