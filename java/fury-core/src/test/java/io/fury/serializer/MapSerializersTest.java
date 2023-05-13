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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import io.fury.Fury;
import io.fury.FuryTestBase;
import io.fury.Language;
import io.fury.memory.MemoryBuffer;
import io.fury.memory.MemoryUtils;
import io.fury.type.GenericType;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
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

  public static class TestClassForDefaultMapSerializer extends AbstractMap<String, Object> {
    private final Set<Entry<String, Object>> data = new HashSet<>();

    @Override
    public Set<Entry<String, Object>> entrySet() {
      return data;
    }

    public static class MapEntry implements Entry<String, Object> {
      private final String k;
      private Object v;

      public MapEntry(String k, Object v) {
        this.k = k;
        this.v = v;
      }

      @Override
      public String getKey() {
        return k;
      }

      @Override
      public Object getValue() {
        return v;
      }

      @Override
      public Object setValue(Object value) {
        Object o = v;
        v = value;
        return o;
      }
    }

    @Override
    public Object put(String key, Object value) {
      return data.add(new TestClassForDefaultMapSerializer.MapEntry(key, value));
    }
  }

  @Test
  public void testDefaultMapSerializer() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).disableSecureMode().build();
    TestClassForDefaultMapSerializer map = new TestClassForDefaultMapSerializer();
    map.put("a", 1);
    map.put("b", 2);
    serDeCheck(fury, map);
    Assert.assertSame(
        fury.getClassResolver().getSerializerClass(TestClassForDefaultMapSerializer.class),
        MapSerializers.DefaultJavaMapSerializer.class);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testJDKCompatibleMapSerialization() {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .disableSecureMode()
            .withReferenceTracking(false)
            .build();
    ImmutableMap<String, Integer> set = ImmutableMap.of("a", 1, "b", 2);
    Class<? extends ImmutableMap> cls = set.getClass();
    MemoryBuffer buffer = MemoryUtils.buffer(32);
    MapSerializers.JDKCompatibleMapSerializer javaSerializer =
        new MapSerializers.JDKCompatibleMapSerializer(fury, cls);
    javaSerializer.write(buffer, set);
    Object read = javaSerializer.read(buffer);
    assertEquals(set, read);

    assertSame(
        fury.getClassResolver().getSerializer(cls).getClass(), ReplaceResolveSerializer.class);
    buffer.writerIndex(0);
    buffer.readerIndex(0);
    assertEquals(set, fury.deserialize(fury.serialize(buffer, set)));
  }
}
