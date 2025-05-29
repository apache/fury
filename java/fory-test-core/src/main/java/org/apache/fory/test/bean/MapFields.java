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

package org.apache.fory.test.bean;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import lombok.Data;

@Data
public class MapFields {
  public Map map;
  public Map<String, Integer> map2;
  public HashMap<String, Integer> map3;
  public Map<String, Object> mapKeyFinal;
  public Map<Object, Integer> mapValueFinal;
  public Map linkedHashMap;
  public Map<String, Integer> linkedHashMap2;
  public LinkedHashMap<String, Integer> linkedHashMap3;
  public SortedMap sortedMap;
  public SortedMap<String, Integer> sortedMap2;
  public TreeMap<String, Integer> sortedMap3;
  public Map concurrentHashMap;
  public ConcurrentHashMap<String, Integer> concurrentHashMap2;
  public ConcurrentHashMap<String, Integer> concurrentHashMap3;
  public Map skipListMap;
  public ConcurrentSkipListMap skipListMap2;
  public ConcurrentSkipListMap<String, Integer> skipListMap3;
  public Map enumMap;
  public EnumMap enumMap2;
  public Map emptyMap;
  public Map sortedEmptyMap;
  public Map singletonMap;

  public static Object copyToCanEqual(Object o, Object newInstance) {
    return CollectionFields.copyToCanEqual(o, newInstance);
  }

  public MapFields toCanEqual() {
    return (MapFields) CollectionFields.copyToCanEqual(this, new MapFields());
  }
}
