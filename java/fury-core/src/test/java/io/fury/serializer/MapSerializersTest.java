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
import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import io.fury.Fury;
import io.fury.FuryTestBase;
import io.fury.Language;
import io.fury.serializer.CollectionSerializersTest.TestEnum;
import io.fury.test.bean.MapFields;
import io.fury.type.GenericType;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MapSerializersTest extends FuryTestBase {

  @Test(dataProvider = "referenceTrackingConfig")
  public void testBasicMap(boolean referenceTrackingConfig) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
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
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
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
            .withRefTracking(referenceTrackingConfig)
            .requireClassRegistration(false)
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

  @Data
  public static class BeanForMap {
    public Map<String, String> map = new TreeMap<>();

    {
      map.put("k1", "v1");
      map.put("k2", "v2");
    }
  }

  @Test
  public void testTreeMap() {
    boolean referenceTracking = true;
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTracking)
            .requireClassRegistration(false)
            .build();
    TreeMap<String, String> map =
        new TreeMap<>(
            (Comparator<? super String> & Serializable)
                (s1, s2) -> {
                  int delta = s1.length() - s2.length();
                  if (delta == 0) {
                    return s1.compareTo(s2);
                  } else {
                    return delta;
                  }
                });
    map.put("str1", "1");
    map.put("str2", "1");
    assertEquals(map, serDe(fury, map));
    BeanForMap beanForMap = new BeanForMap();
    assertEquals(beanForMap, serDe(fury, beanForMap));
  }

  @Test
  public void testEmptyMap() {
    serDeCheckSerializer(getJavaFury(), Collections.EMPTY_MAP, "EmptyMapSerializer");
    serDeCheckSerializer(getJavaFury(), Collections.emptySortedMap(), "EmptySortedMap");
  }

  @Test
  public void testSingleMap() {
    serDeCheckSerializer(getJavaFury(), Collections.singletonMap("k", 1), "SingletonMap");
  }

  @Test
  public void testConcurrentMap() {
    Map<String, Integer> data = new TreeMap<>(ImmutableMap.of("a", 1, "b", 2));
    serDeCheckSerializer(getJavaFury(), new ConcurrentHashMap<>(data), "ConcurrentHashMap");
    serDeCheckSerializer(getJavaFury(), new ConcurrentSkipListMap<>(data), "ConcurrentSkipListMap");
  }

  @Test
  public void testEnumMap() {
    EnumMap<TestEnum, Object> enumMap = new EnumMap<>(TestEnum.class);
    enumMap.put(TestEnum.A, 1);
    enumMap.put(TestEnum.B, "str");
    serDe(getJavaFury(), enumMap);
    Assert.assertEquals(
        getJavaFury().getClassResolver().getSerializerClass(enumMap.getClass()),
        MapSerializers.EnumMapSerializer.class);
  }

  private static Map<String, Integer> newInnerMap() {
    return new HashMap<String, Integer>() {
      {
        put("k1", 1);
        put("k2", 2);
      }
    };
  }

  @Test
  public void testNoArgConstructor() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
    Map<String, Integer> map = newInnerMap();
    Assert.assertEquals(jdkDeserialize(jdkSerialize(map)), map);
    serDeCheck(fury, map);
  }

  @Test
  public void testMapNoJIT() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).withCodegen(false).build();
    serDeCheck(fury, new HashMap<>(ImmutableMap.of("a", 1, "b", 2)));
    serDeCheck(fury, new HashMap<>(ImmutableMap.of("a", "v1", "b", "v2")));
    serDeCheck(fury, new HashMap<>(ImmutableMap.of(1, 2, 3, 4)));
  }

  @Test(dataProvider = "javaFury")
  public void testMapFieldSerializers(Fury fury) {
    MapFields obj = createMapFieldsObject();
    Assert.assertEquals(serDe(fury, obj), obj);
  }

  @Test(dataProvider = "javaFuryKVCompatible")
  public void testMapFieldsKVCompatible(Fury fury) {
    MapFields obj = createMapFieldsObject();
    Assert.assertEquals(serDe(fury, obj), obj);
  }

  public static MapFields createMapFieldsObject() {
    MapFields obj = new MapFields();
    Map<String, Integer> map = ImmutableMap.of("k1", 1, "k2", 2);
    obj.map = map;
    obj.map2 = new HashMap<>(map);
    obj.map3 = new HashMap<>(map);
    obj.mapKeyFinal = new HashMap<>(ImmutableMap.of("k1", map, "k2", new HashMap<>(map)));
    obj.mapValueFinal = new HashMap<>(map);
    obj.linkedHashMap = new LinkedHashMap<>(map);
    obj.linkedHashMap2 = new LinkedHashMap<>(map);
    obj.linkedHashMap3 = new LinkedHashMap<>(map);
    obj.sortedMap = new TreeMap<>(map);
    obj.sortedMap2 = new TreeMap<>(map);
    obj.sortedMap3 = new TreeMap<>(map);
    obj.concurrentHashMap = new ConcurrentHashMap<>(map);
    obj.concurrentHashMap2 = new ConcurrentHashMap<>(map);
    obj.concurrentHashMap3 = new ConcurrentHashMap<>(map);
    obj.skipListMap = new ConcurrentSkipListMap<>(map);
    obj.skipListMap2 = new ConcurrentSkipListMap<>(map);
    obj.skipListMap3 = new ConcurrentSkipListMap<>(map);
    EnumMap<TestEnum, Object> enumMap = new EnumMap<>(TestEnum.class);
    enumMap.put(TestEnum.A, 1);
    enumMap.put(TestEnum.B, "str");
    obj.enumMap = enumMap;
    obj.enumMap2 = enumMap;
    obj.emptyMap = Collections.emptyMap();
    obj.sortedEmptyMap = Collections.emptySortedMap();
    obj.singletonMap = Collections.singletonMap("k", "v");
    return obj;
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
      return data.add(new MapEntry(key, value));
    }
  }

  @Test(dataProvider = "enableCodegen")
  public void testDefaultMapSerializer(boolean enableCodegen) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withCodegen(enableCodegen)
            .requireClassRegistration(false)
            .build();
    TestClassForDefaultMapSerializer map = new TestClassForDefaultMapSerializer();
    map.put("a", 1);
    map.put("b", 2);
    Assert.assertSame(
        fury.getClassResolver().getSerializerClass(TestClassForDefaultMapSerializer.class),
        MapSerializers.DefaultJavaMapSerializer.class);
    serDeCheck(fury, map);
  }

  @Data
  @AllArgsConstructor
  public static class GenericMapBoundTest {
    // test k/v generics
    public Map<Map<Integer, Collection<Integer>>, ? extends Collection<Integer>> map1;
    // test k/v generics bounds
    public Map<? extends Map<Integer, ? extends Collection<Integer>>, ? extends Collection<Integer>>
        map2;
  }

  @Test
  public void testGenericMapBound() {
    Fury fury1 =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .requireClassRegistration(false)
            .withCodegen(false)
            .build();
    Fury fury2 =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .requireClassRegistration(false)
            .withCodegen(false)
            .build();
    ArrayList<Integer> list = new ArrayList<>(of(1, 2));
    roundCheck(
        fury1,
        fury2,
        new GenericMapBoundTest(
            new HashMap<>(mapOf(new HashMap<>(mapOf(1, list)), list)),
            new HashMap<>(mapOf(new HashMap<>(mapOf(1, list)), list))));
  }
}
