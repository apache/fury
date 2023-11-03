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

package io.fury.collection;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import io.fury.Fury;
import io.fury.FuryTestBase;
import io.fury.config.Language;
import io.fury.serializer.collection.MapSerializers;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

public class LazyMapTest extends FuryTestBase {

  @Test
  public void testMap() throws NoSuchFieldException, IllegalAccessException {
    Map<String, Integer> map = new HashMap<>();
    map.put("k1", 1);
    map.put("k2", 2);
    LazyMap<String, Integer> map1 = new LazyMap<>(new ArrayList<>(map.entrySet()));
    Field field = LazyMap.class.getDeclaredField("map");
    field.setAccessible(true);
    assertNull(field.get(map1));
    assertEquals(new HashMap<>(map1), map);
    assertEquals(map1.delegate(), map);
    assertEquals(map1.toString(), map.toString());
  }

  @Test
  public void testMapSerialization() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).build();
    Map<String, Integer> map = new HashMap<>();
    map.put("k1", 1);
    map.put("k2", 2);
    LazyMap<String, Integer> map1 = new LazyMap<>(new ArrayList<>(map.entrySet()));
    serDe(fury, map1);
    assertTrue(
        fury.getClassResolver().getSerializer(LazyMap.class)
            instanceof MapSerializers.LazyMapSerializer);
  }
}
