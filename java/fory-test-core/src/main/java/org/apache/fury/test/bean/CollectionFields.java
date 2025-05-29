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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import lombok.Data;

@Data
public class CollectionFields {
  public Collection collection;
  public Collection<Integer> collection2;
  public List<Integer> collection3;

  public Collection randomAccessList;
  public Collection<String> randomAccessList2;
  public List<String> randomAccessList3;

  public Collection list;
  public Collection<String> list2;
  public List<String> list3;

  public Collection set;
  public Collection<String> set2;
  public Set<String> set3;

  public Collection sortedSet;
  public Collection<String> sortedSet2;
  public SortedSet<String> sortedSet3;

  public Map map;
  public Map<String, String> map2;

  public Map sortedMap;
  public Map<Integer, Integer> sortedMap2;
  public SortedMap<Integer, Integer> sortedMap3;

  public CollectionFields toCanEqual() {
    return (CollectionFields) copyToCanEqual(this, new CollectionFields());
  }

  /** Copy the object, so that inner fields are equal able. */
  public static Object copyToCanEqual(Object raw, Object newObj) {
    for (Field field : raw.getClass().getDeclaredFields()) {
      field.setAccessible(true);
      try {
        Object fieldValue = field.get(raw);
        if (fieldValue != null) {
          boolean hasEquals =
              Arrays.stream(fieldValue.getClass().getDeclaredMethods())
                  .anyMatch(m -> m.getName().equals("equals"));
          if (hasEquals) {
            continue;
          }
          if (fieldValue instanceof LinkedHashMap) {
            continue;
          }
          if (fieldValue instanceof SortedSet) {
            field.set(newObj, new TreeSet<>((SortedSet) fieldValue));
          } else if (fieldValue instanceof Set) {
            field.set(newObj, new HashSet<>((Collection) fieldValue));
          } else if (fieldValue instanceof List) {
            field.set(newObj, new ArrayList((List) fieldValue));
          } else if (fieldValue instanceof Collection) {
            field.set(newObj, new ArrayList((Collection) fieldValue));
          } else if (fieldValue instanceof SortedMap) {
            field.set(newObj, new TreeMap((Map) fieldValue));
          } else if (fieldValue instanceof Map) {
            field.set(newObj, new HashMap((Map) fieldValue));
          } else {
            throw new RuntimeException("Unexpected type " + field);
          }
        }
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    return newObj;
  }
}
