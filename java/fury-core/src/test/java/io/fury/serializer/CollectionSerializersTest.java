/*
 * Copyright 2023 The Fury authors
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import io.fury.Fury;
import io.fury.FuryTestBase;
import io.fury.Language;
import io.fury.type.GenericType;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.LongStream;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CollectionSerializersTest extends FuryTestBase {
  @Test(dataProvider = "referenceTrackingConfig")
  public void testBasicList(boolean referenceTrackingConfig) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withReferenceTracking(referenceTrackingConfig)
            .disableSecureMode()
            .build();
    List<String> data = new ArrayList<>(ImmutableList.of("a", "b", "c"));
    serDeCheckSerializer(fury, data, "ArrayList");
    serDeCheckSerializer(fury, Arrays.asList("a", "b", "c"), "ArraysAsList");
    serDeCheckSerializer(fury, new HashSet<>(data), "HashSet");
    serDeCheckSerializer(fury, new LinkedHashSet<>(data), "LinkedHashSet");
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testCollectionGenerics(boolean referenceTrackingConfig) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withReferenceTracking(referenceTrackingConfig)
            .disableSecureMode()
            .build();
    List<String> data = new ArrayList<>(ImmutableList.of("a", "b", "c"));
    byte[] bytes1 = fury.serialize(data);
    fury.getGenerics().pushGenericType(GenericType.build(new TypeToken<List<String>>() {}));
    byte[] bytes2 = fury.serialize(data);
    Assert.assertTrue(bytes1.length > bytes2.length);
    System.out.println(fury.deserialize(bytes2));
    fury.getGenerics().popGenericType();
    Assert.assertThrows(RuntimeException.class, () -> fury.deserialize(bytes2));
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testSortedSet(boolean referenceTrackingConfig) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withReferenceTracking(referenceTrackingConfig)
            .disableSecureMode()
            .build();
    TreeSet<String> data = new TreeSet<>(ImmutableSet.of("a", "b", "c"));
    serDeCheckSerializer(fury, data, "SortedSet");
    byte[] bytes1 = fury.serialize(data);
    fury.getGenerics().pushGenericType(GenericType.build(new TypeToken<List<String>>() {}));
    byte[] bytes2 = fury.serialize(data);
    Assert.assertTrue(bytes1.length > bytes2.length);
    System.out.println(fury.deserialize(bytes2));
    fury.getGenerics().popGenericType();
    Assert.assertThrows(RuntimeException.class, () -> fury.deserialize(bytes2));
  }

  @Test
  public void testEmptyCollection() {
    serDeCheckSerializer(javaFury, Collections.EMPTY_LIST, "EmptyListSerializer");
    serDeCheckSerializer(javaFury, Collections.emptySortedSet(), "EmptySortedSetSerializer");
    serDeCheckSerializer(javaFury, Collections.EMPTY_SET, "EmptySetSerializer");
  }

  @Test
  public void testSingleCollection() {
    serDeCheckSerializer(javaFury, Collections.singletonList(1), "SingletonList");
    serDeCheckSerializer(javaFury, Collections.singleton(1), "SingletonSet");
  }

  @Test
  public void tesSkipList() {
    serDeCheckSerializer(
        javaFury,
        new ConcurrentSkipListSet<>(Arrays.asList("a", "b", "c")),
        "ConcurrentSkipListSet");
  }

  @Test
  public void tesVectorSerializer() {
    serDeCheckSerializer(javaFury, new Vector<>(Arrays.asList("a", "b", "c")), "VectorSerializer");
  }

  @Test
  public void tesArrayDequeSerializer() {
    serDeCheckSerializer(javaFury, new ArrayDeque<>(Arrays.asList("a", "b", "c")), "ArrayDeque");
  }

  enum TestEnum {
    A,
    B,
    C,
    D
  }

  @Test
  public void tesEnumSetSerializer() {
    serDe(javaFury, EnumSet.allOf(TestEnum.class));
    Assert.assertEquals(
        javaFury.getClassResolver().getSerializerClass(EnumSet.allOf(TestEnum.class).getClass()),
        CollectionSerializers.EnumSetSerializer.class);
    serDe(javaFury, EnumSet.of(TestEnum.A));
    Assert.assertEquals(
        javaFury.getClassResolver().getSerializerClass(EnumSet.of(TestEnum.A).getClass()),
        CollectionSerializers.EnumSetSerializer.class);
    serDe(javaFury, EnumSet.of(TestEnum.A, TestEnum.B));
    Assert.assertEquals(
        javaFury
            .getClassResolver()
            .getSerializerClass(EnumSet.of(TestEnum.A, TestEnum.B).getClass()),
        CollectionSerializers.EnumSetSerializer.class);
    // TODO test enum which has enums exceed 128.
  }

  @Test
  public void tesBitSetSerializer() {
    serDe(javaFury, BitSet.valueOf(LongStream.range(0, 2).toArray()));
    Assert.assertEquals(
        javaFury
            .getClassResolver()
            .getSerializerClass(BitSet.valueOf(LongStream.range(0, 2).toArray()).getClass()),
        CollectionSerializers.BitSetSerializer.class);
    serDe(javaFury, BitSet.valueOf(LongStream.range(0, 128).toArray()));
    Assert.assertEquals(
        javaFury
            .getClassResolver()
            .getSerializerClass(BitSet.valueOf(LongStream.range(0, 128).toArray()).getClass()),
        CollectionSerializers.BitSetSerializer.class);
  }
}
