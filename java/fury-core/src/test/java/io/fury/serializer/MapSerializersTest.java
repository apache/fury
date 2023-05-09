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

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import io.fury.Fury;
import io.fury.FuryTestBase;
import io.fury.Language;
import io.fury.type.GenericType;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.testng.Assert;
import org.testng.annotations.Test;

@SuppressWarnings("UnstableApiUsage")
public class MapSerializersTest extends FuryTestBase {

  @Test(dataProvider = "referenceTrackingConfig")
  public void testBasicMap(boolean referenceTrackingConfig) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withReferenceTracking(referenceTrackingConfig)
            .disableSecureMode()
            .build();
    Map<String, Integer> data = new HashMap<>(ImmutableMap.of("a", 1, "b", 2));
    serDeCheckSerializer(fury, data, "HashMap");
    serDeCheckSerializer(fury, new LinkedHashMap<>(data), "LinkedHashMap");
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testMapGenerics(boolean referenceTrackingConfig) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withReferenceTracking(referenceTrackingConfig)
            .disableSecureMode()
            .build();
    Map<String, Integer> data = new HashMap<>(ImmutableMap.of("a", 1, "b", 2));
    byte[] bytes1 = fury.serialize(data);
    fury.getGenerics().pushGenericType(GenericType.build(new TypeToken<Map<String, Integer>>() {}));
    byte[] bytes2 = fury.serialize(data);
    Assert.assertTrue(bytes1.length > bytes2.length);
    fury.getGenerics().popGenericType();
    Assert.assertThrows(RuntimeException.class, () -> fury.deserialize(bytes2));
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testSortedMap(boolean referenceTrackingConfig) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withReferenceTracking(referenceTrackingConfig)
            .disableSecureMode()
            .build();
    Map<String, Integer> data = new TreeMap<>(ImmutableMap.of("a", 1, "b", 2));
    serDeCheckSerializer(fury, data, "SortedMap");
    byte[] bytes1 = fury.serialize(data);
    fury.getGenerics().pushGenericType(GenericType.build(new TypeToken<Map<String, Integer>>() {}));
    byte[] bytes2 = fury.serialize(data);
    Assert.assertTrue(bytes1.length > bytes2.length);
    fury.getGenerics().popGenericType();
    Assert.assertThrows(RuntimeException.class, () -> fury.deserialize(bytes2));
  }

  @Test
  public void testEmptyMap() {
    serDeCheckSerializer(javaFury, Collections.EMPTY_MAP, "EmptyMapSerializer");
    serDeCheckSerializer(javaFury, Collections.emptySortedMap(), "EmptySortedMap");
  }

  @Test
  public void testSingleMap() {
    serDeCheckSerializer(javaFury, Collections.singletonMap("k", 1), "SingletonMap");
  }

  @Test
  public void testConcurrentMap() {
    Map<String, Integer> data = new TreeMap<>(ImmutableMap.of("a", 1, "b", 2));
    serDeCheckSerializer(javaFury, new ConcurrentHashMap<>(data), "ConcurrentHashMap");
    serDeCheckSerializer(javaFury, new ConcurrentSkipListMap<>(data), "ConcurrentSkipListMap");
  }

  @Test
  public void testEnumMap() {
    EnumMap<CollectionSerializersTest.TestEnum, Object> enumMap =
        new EnumMap<>(CollectionSerializersTest.TestEnum.class);
    enumMap.put(CollectionSerializersTest.TestEnum.A, 1);
    enumMap.put(CollectionSerializersTest.TestEnum.B, "str");
    serDe(javaFury, enumMap);
    Assert.assertEquals(
        javaFury.getClassResolver().getSerializerClass(enumMap.getClass()),
        MapSerializers.EnumMapSerializer.class);
  }
}
