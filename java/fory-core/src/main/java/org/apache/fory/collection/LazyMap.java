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

package org.apache.fory.collection;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.fory.util.Preconditions;

/** A map which populate lazily until the first map query happens to reduce map#put cost. */
public class LazyMap<K, V> implements Map<K, V> {
  private List<Entry<? extends K, ? extends V>> entries;
  private Map<K, V> map;

  public LazyMap() {
    entries = new ArrayList<>();
  }

  public LazyMap(int size) {
    entries = new ArrayList<>(size);
  }

  public LazyMap(List<Entry<? extends K, ? extends V>> entries) {
    this.entries = entries;
  }

  public Map<K, V> delegate() {
    Map<K, V> m = this.map;
    if (m == null) {
      List<Entry<? extends K, ? extends V>> e = this.entries;
      m = new HashMap<>(e.size());
      for (Entry<? extends K, ? extends V> entry : e) {
        m.put(entry.getKey(), entry.getValue());
      }
      this.map = m;
    }
    return m;
  }

  public void setEntries(List<Entry<? extends K, ? extends V>> entries) {
    Preconditions.checkArgument(map == null);
    this.entries = entries;
  }

  @Override
  public V put(K key, V value) {
    Map<K, V> m = map;
    if (m == null) {
      // avoid map put cost when deserialization this map.
      entries.add(new MapEntry<>(key, value));
      return null;
    } else {
      return m.put(key, value);
    }
  }

  @Override
  public V getOrDefault(Object key, V defaultValue) {
    return delegate().getOrDefault(key, defaultValue);
  }

  @Override
  public void forEach(BiConsumer<? super K, ? super V> action) {
    Map<K, V> m = map;
    if (m == null) {
      for (Entry<? extends K, ? extends V> entry : entries) {
        action.accept(entry.getKey(), entry.getValue());
      }
    } else {
      m.forEach(action);
    }
  }

  @Override
  public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
    delegate().replaceAll(function);
  }

  @Override
  public V putIfAbsent(K key, V value) {
    return delegate().putIfAbsent(key, value);
  }

  @Override
  public boolean remove(Object key, Object value) {
    return delegate().remove(key, value);
  }

  @Override
  public V remove(Object key) {
    return delegate().remove(key);
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    return delegate().replace(key, oldValue, newValue);
  }

  @Override
  public V replace(K key, V value) {
    return delegate().replace(key, value);
  }

  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    return delegate().computeIfAbsent(key, mappingFunction);
  }

  @Override
  public V computeIfPresent(
      K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return delegate().computeIfPresent(key, remappingFunction);
  }

  @Override
  public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return delegate().compute(key, remappingFunction);
  }

  @Override
  public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
    return delegate().merge(key, value, remappingFunction);
  }

  @Override
  public int size() {
    Map<K, V> m = map;
    return m == null ? entries.size() : m.size();
  }

  @Override
  public boolean isEmpty() {
    Map<K, V> m = map;
    return m == null ? entries.isEmpty() : m.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return delegate().containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return delegate().containsValue(value);
  }

  @Override
  public V get(Object key) {
    return delegate().get(key);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    Map<K, V> map = this.map;
    if (map == null) {
      // avoid map put cost when deserialization this map.
      entries.addAll(m.entrySet());
    } else {
      map.putAll(m);
    }
  }

  @Override
  public void clear() {
    Map<K, V> m = map;
    if (m == null) {
      entries.clear();
    } else {
      m.clear();
    }
  }

  @Override
  public Set<K> keySet() {
    return delegate().keySet();
  }

  @Override
  public Collection<V> values() {
    return delegate().values();
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return delegate().entrySet();
  }

  @Override
  public boolean equals(Object obj) {
    Map<K, V> m = map;
    if (m != null) {
      return m.equals(obj);
    }

    if (obj == this) {
      return true;
    }

    if (!(obj instanceof Map)) {
      return false;
    }
    Map<?, ?> map = (Map<?, ?>) obj;
    List<Entry<? extends K, ? extends V>> entries = this.entries;
    if (map.size() != entries.size()) {
      return false;
    }

    try {
      for (Entry<? extends K, ? extends V> e : entries) {
        K key = e.getKey();
        V value = e.getValue();
        if (value == null) {
          if (!(map.get(key) == null && map.containsKey(key))) {
            return false;
          }
        } else {
          if (!value.equals(map.get(key))) {
            return false;
          }
        }
      }
    } catch (ClassCastException | NullPointerException unused) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    Map<K, V> m = map;
    if (m != null) {
      return m.hashCode();
    }

    int h = 0;
    for (Entry<? extends K, ? extends V> entry : entries) {
      h += entry.hashCode();
    }
    return h;
  }

  @Override
  public String toString() {
    Iterator<Entry<? extends K, ? extends V>> i = entries.iterator();
    if (!i.hasNext()) {
      return "{}";
    }

    StringBuilder sb = new StringBuilder();
    sb.append('{');
    for (; ; ) {
      Entry<? extends K, ? extends V> e = i.next();
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
