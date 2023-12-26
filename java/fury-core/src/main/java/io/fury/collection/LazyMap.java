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

package org.apache.fury.collection;

import com.google.common.collect.ForwardingMap;
import org.apache.fury.util.Preconditions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * A map which populate lazily until the first map query happens to reduce map#put cost.
 *
 * @author chaokunyang
 */
public class LazyMap<K, V> extends ForwardingMap<K, V> {
  private List<Entry<K, V>> entries;

  public LazyMap() {
    entries = new ArrayList<>();
  }

  public LazyMap(int size) {
    entries = new ArrayList<>(size);
  }

  public LazyMap(List<Entry<K, V>> entries) {
    this.entries = entries;
  }

  private Map<K, V> map;

  @Override
  public Map<K, V> delegate() {
    if (map == null) {
      map = new HashMap<>(entries.size());
      for (Entry<K, V> entry : entries) {
        map.put(entry.getKey(), entry.getValue());
      }
    }
    return map;
  }

  @Override
  public V put(K key, V value) {
    if (map == null) {
      // avoid map put cost when deserialization this map.
      entries.add(new MapEntry<>(key, value));
      return null;
    } else {
      return map.put(key, value);
    }
  }

  public void setEntries(List<Entry<K, V>> entries) {
    Preconditions.checkArgument(map == null);
    this.entries = entries;
  }

  public String toString() {
    Iterator<Entry<K, V>> i = entries.iterator();
    if (!i.hasNext()) {
      return "{}";
    }

    StringBuilder sb = new StringBuilder();
    sb.append('{');
    for (; ; ) {
      Entry<K, V> e = i.next();
      K key = e.getKey();
      V value = e.getValue();
      sb.append(key == this ? "(this Map)" : key);
      sb.append('=');
      sb.append(value == this ? "(this Map)" : value);
      if (!i.hasNext()) {
        return sb.append('}').toString();
      }
      sb.append(',').append(' ');
    }
  }
}
