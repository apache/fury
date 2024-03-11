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

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.Language;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GuavaCollectionSerializersTest extends FuryTestBase {

  @Test(dataProvider = "trackingRefFury")
  public void testImmutableListSerializer(Fury fury) {
    serDe(fury, ImmutableList.of(1));
    Assert.assertEquals(
        fury.getClassResolver().getSerializerClass(ImmutableList.of(1).getClass()),
        GuavaCollectionSerializers.ImmutableListSerializer.class);
    serDe(fury, ImmutableList.of(1, 2));
    Assert.assertEquals(
        fury.getClassResolver().getSerializerClass(ImmutableList.of(1, 2).getClass()),
        GuavaCollectionSerializers.RegularImmutableListSerializer.class);
  }

  @Test(dataProvider = "trackingRefFury")
  public void testImmutableSetSerializer(Fury fury) {
    serDe(fury, ImmutableSet.of(1));
    Assert.assertEquals(
        fury.getClassResolver().getSerializerClass(ImmutableSet.of(1).getClass()),
        GuavaCollectionSerializers.ImmutableSetSerializer.class);
    serDe(fury, ImmutableSet.of(1, 2));
    Assert.assertEquals(
        fury.getClassResolver().getSerializerClass(ImmutableSet.of(1, 2).getClass()),
        GuavaCollectionSerializers.ImmutableSetSerializer.class);
  }

  @Test(dataProvider = "trackingRefFury")
  public void testImmutableSortedSetSerializer(Fury fury) {
    serDe(fury, ImmutableSortedSet.of(1, 2));
    Assert.assertEquals(
        fury.getClassResolver().getSerializerClass(ImmutableSortedSet.of(1, 2).getClass()),
        GuavaCollectionSerializers.ImmutableSortedSetSerializer.class);
  }

  @Test(dataProvider = "trackingRefFury")
  public void testImmutableMapSerializer(Fury fury) {
    serDe(fury, ImmutableMap.of("k1", 1, "k2", 2));
    Assert.assertEquals(
        fury.getClassResolver().getSerializerClass(ImmutableMap.of("k1", 1, "k2", 2).getClass()),
        GuavaCollectionSerializers.ImmutableMapSerializer.class);
  }

  @Test(dataProvider = "trackingRefFury")
  public void testImmutableBiMapSerializer(Fury fury) {
    serDe(fury, ImmutableBiMap.of("k1", 1));
    Assert.assertEquals(
        fury.getClassResolver().getSerializerClass(ImmutableBiMap.of("k1", 1).getClass()),
        GuavaCollectionSerializers.ImmutableBiMapSerializer.class);
    serDe(fury, ImmutableBiMap.of("k1", 1, "k2", 2));
    Assert.assertEquals(
        fury.getClassResolver().getSerializerClass(ImmutableBiMap.of("k1", 1, "k2", 2).getClass()),
        GuavaCollectionSerializers.ImmutableBiMapSerializer.class);
  }

  @Test(dataProvider = "trackingRefFury")
  public void testImmutableSortedMapSerializer(Fury fury) {
    serDe(fury, ImmutableSortedMap.of("k1", 1, "k2", 2));
    Assert.assertEquals(
        fury.getClassResolver()
            .getSerializerClass(ImmutableSortedMap.of("k1", 1, "k2", 2).getClass()),
        GuavaCollectionSerializers.ImmutableSortedMapSerializer.class);
  }

  @Test
  public void tesXlangSerialize() {
    Fury fury = Fury.builder().withLanguage(Language.XLANG).build();
    serDe(fury, ImmutableBiMap.of());
    serDe(fury, ImmutableBiMap.of(1, 2));
    serDe(fury, ImmutableBiMap.of(1, 2, 3, 4));

    serDe(fury, ImmutableList.of());
    serDe(fury, ImmutableList.of(1));
    serDe(fury, ImmutableList.of(1, 2));

    serDe(fury, ImmutableSet.of());
    serDe(fury, ImmutableSet.of(1));
    serDe(fury, ImmutableSet.of(1, 2, 3, 4));
  }

  @Data
  @AllArgsConstructor
  public static class Pojo {
    List<List<Object>> data;
  }

  @Test(dataProvider = "javaFury")
  void testNestedRefTracking(Fury fury) {
    Pojo pojo = new Pojo(ImmutableList.of(ImmutableList.of(1, 2), ImmutableList.of(2, 2)));
    byte[] bytes = fury.serialize(pojo);
    Pojo deserializedPojo = (Pojo) fury.deserialize(bytes);
    System.out.println(deserializedPojo);
  }
}
