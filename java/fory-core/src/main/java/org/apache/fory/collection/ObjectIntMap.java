/* Copyright (c) 2008-2023, Nathan Sweet
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 * - Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 * disclaimer in the documentation and/or other materials provided with the distribution.
 * - Neither the name of Esoteric Software nor the names of its contributors may be used to endorse or promote products derived
 * from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE. */

package org.apache.fory.collection;

import static org.apache.fory.collection.ForyObjectMap.MASK_NUMBER;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

// Derived from
// https://github.com/EsotericSoftware/kryo/blob/135df69526615bb3f6b34846e58ba3fec3b631c3/src/com/esotericsoftware/kryo/util/ObjectIntMap.java.

@SuppressWarnings("unchecked")
public class ObjectIntMap<K> {
  public int size;

  K[] keyTable;
  int[] valueTable;

  float loadFactor;
  int threshold;
  protected int shift;

  protected int mask;

  public ObjectIntMap() {
    this(51, 0.5f);
  }

  public ObjectIntMap(int initialCapacity, float loadFactor) {
    if (loadFactor <= 0f || loadFactor >= 1f) {
      throw new IllegalArgumentException("loadFactor must be > 0 and < 1: " + loadFactor);
    }
    this.loadFactor = loadFactor;

    int tableSize = ForyObjectMap.tableSize(initialCapacity, loadFactor);
    threshold = (int) (tableSize * loadFactor);
    mask = tableSize - 1;
    shift = Long.numberOfLeadingZeros(mask);

    keyTable = (K[]) new Object[tableSize];
    valueTable = new int[tableSize];
  }

  protected int place(K item) {
    return (int) (item.hashCode() * MASK_NUMBER >>> shift);
  }

  int locateKey(K key) {
    K[] keyTable = this.keyTable;
    int mask = this.mask;
    for (int i = place(key); ; i = i + 1 & mask) {
      K other = keyTable[i];
      if (other == null) {
        return -(i + 1); // Empty space is available.
      }
      if (other.equals(key)) {
        return i; // Same key was found.
      }
    }
  }

  public void put(K key, int value) {
    int i = locateKey(key);
    if (i >= 0) { // Existing key was found.
      valueTable[i] = value;
      return;
    }
    i = -(i + 1); // Empty space was found.
    keyTable[i] = key;
    valueTable[i] = value;
    if (++size >= threshold) {
      resize(keyTable.length << 1);
    }
  }

  private void putResize(K key, int value) {
    K[] keyTable = this.keyTable;
    int mask = this.mask;
    for (int i = place(key); ; i = (i + 1) & mask) {
      if (keyTable[i] == null) {
        keyTable[i] = key;
        valueTable[i] = value;
        return;
      }
    }
  }

  public int get(K key, int defaultValue) {
    int i = locateKey(key);
    return i < 0 ? defaultValue : valueTable[i];
  }

  public int remove(K key, int defaultValue) {
    int i = locateKey(key);
    if (i < 0) {
      return defaultValue;
    }
    K[] keyTable = this.keyTable;
    int[] valueTable = this.valueTable;
    int oldValue = valueTable[i];
    int mask = this.mask, next = i + 1 & mask;
    while ((key = keyTable[next]) != null) {
      int placement = place(key);
      if ((next - placement & mask) > (i - placement & mask)) {
        keyTable[i] = key;
        valueTable[i] = valueTable[next];
        i = next;
      }
      next = next + 1 & mask;
    }
    keyTable[i] = null;
    size--;
    return oldValue;
  }

  public boolean isEmpty() {
    return size == 0;
  }

  public void clearApproximate(int maximumCapacity) {
    // approximate table size
    int tableSize =
        ForyObjectMap.nextPowerOfTwo(Math.max(2, (int) (maximumCapacity / loadFactor + 1)));
    if (keyTable.length <= tableSize) {
      clear();
      return;
    }
    size = 0;
    resize(tableSize);
  }

  public void clear() {
    if (size == 0) {
      return;
    }
    size = 0;
    ObjectArray.clearObjectArray(keyTable, 0, keyTable.length);
  }

  public boolean containsKey(K key) {
    return locateKey(key) >= 0;
  }

  final void resize(int newSize) {
    int oldCapacity = keyTable.length;
    threshold = (int) (newSize * loadFactor);
    mask = newSize - 1;
    shift = Long.numberOfLeadingZeros(mask);

    K[] oldKeyTable = keyTable;
    int[] oldValueTable = valueTable;

    keyTable = (K[]) new Object[newSize];
    valueTable = new int[newSize];

    if (size > 0) {
      for (int i = 0; i < oldCapacity; i++) {
        K key = oldKeyTable[i];
        if (key != null) {
          putResize(key, oldValueTable[i]);
        }
      }
    }
  }

  public Map<K, Integer> toHashMap() {
    Map<K, Integer> map = new HashMap<>(size);
    K[] keyTable = this.keyTable;
    int[] valueTable = this.valueTable;
    for (int i = 0, n = keyTable.length; i < n; i++) {
      K k = keyTable[i];
      if (k != null) {
        map.put(k, valueTable[i]);
      }
    }
    return map;
  }

  public void forEach(BiConsumer<? super K, Integer> action) {
    K[] keyTable = this.keyTable;
    int[] valueTable = this.valueTable;
    for (int i = 0, n = keyTable.length; i < n; i++) {
      K k = keyTable[i];
      if (k != null) {
        action.accept(k, valueTable[i]);
      }
    }
  }

  public int hashCode() {
    int h = size;
    K[] keyTable = this.keyTable;
    int[] valueTable = this.valueTable;
    for (int i = 0, n = keyTable.length; i < n; i++) {
      K key = keyTable[i];
      if (key != null) {
        h += key.hashCode() + valueTable[i];
      }
    }
    return h;
  }

  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof ObjectIntMap)) {
      return false;
    }
    ObjectIntMap other = (ObjectIntMap) obj;
    if (other.size != size) {
      return false;
    }
    K[] keyTable = this.keyTable;
    int[] valueTable = this.valueTable;
    for (int i = 0, n = keyTable.length; i < n; i++) {
      K key = keyTable[i];
      if (key != null) {
        int otherValue = other.get(key, 0);
        if (otherValue == 0 && !other.containsKey(key)) {
          return false;
        }
        if (otherValue != valueTable[i]) {
          return false;
        }
      }
    }
    return true;
  }

  public String toString(String separator) {
    return toString(separator, false);
  }

  public String toString() {
    return toString(", ", true);
  }

  private String toString(String separator, boolean braces) {
    if (size == 0) {
      return braces ? "{}" : "";
    }
    StringBuilder buffer = new StringBuilder(32);
    if (braces) {
      buffer.append('{');
    }
    K[] keyTable = this.keyTable;
    int[] valueTable = this.valueTable;
    int i = keyTable.length;
    while (i-- > 0) {
      K key = keyTable[i];
      if (key == null) {
        continue;
      }
      buffer.append(key);
      buffer.append('=');
      buffer.append(valueTable[i]);
      break;
    }
    while (i-- > 0) {
      K key = keyTable[i];
      if (key == null) {
        continue;
      }
      buffer.append(separator);
      buffer.append(key);
      buffer.append('=');
      buffer.append(valueTable[i]);
    }
    if (braces) {
      buffer.append('}');
    }
    return buffer.toString();
  }
}
