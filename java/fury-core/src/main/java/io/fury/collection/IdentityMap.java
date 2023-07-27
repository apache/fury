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

public final class IdentityMap<K, V> extends FuryObjectMap<K, V> {
  public IdentityMap() {
    super();
  }

  public IdentityMap(int initialCapacity) {
    super(initialCapacity);
  }

  public IdentityMap(int initialCapacity, float loadFactor) {
    super(initialCapacity, loadFactor);
  }

  protected int place(K item) {
    return System.identityHashCode(item) & mask;
  }

  public <T extends K> V get(T key) {
    int mask = this.mask;
    K[] keyTable = this.keyTable;
    for (int i = place(key); ; i = i + 1 & mask) {
      K other = keyTable[i];
      if (other == null) {
        return null;
      }
      if (other == key) {
        return valueTable[i];
      }
    }
  }

  public V get(K key, V defaultValue) {
    int mask = this.mask;
    K[] keyTable = this.keyTable;
    for (int i = place(key); ; i = i + 1 & mask) {
      K other = keyTable[i];
      if (other == null) {
        return defaultValue;
      }
      if (other == key) {
        return valueTable[i];
      }
    }
  }

  int locateKey(K key) {
    if (key == null) {
      throw new IllegalArgumentException("key cannot be null.");
    }
    K[] keyTable = this.keyTable;
    for (int i = place(key); ; i = i + 1 & mask) {
      K other = keyTable[i];
      if (other == null) {
        return -(i + 1); // Empty space is available.
      }
      if (other == key) {
        return i; // Same key was found.
      }
    }
  }

  public int hashCode() {
    int h = size;
    K[] keyTable = this.keyTable;
    V[] valueTable = this.valueTable;
    for (int i = 0, n = keyTable.length; i < n; i++) {
      K key = keyTable[i];
      if (key != null) {
        h += System.identityHashCode(key);
        V value = valueTable[i];
        if (value != null) {
          h += value.hashCode();
        }
      }
    }
    return h;
  }
}
