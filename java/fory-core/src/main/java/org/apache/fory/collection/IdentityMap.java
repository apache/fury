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

// Derived from
// https://github.com/EsotericSoftware/kryo/blob/135df69526615bb3f6b34846e58ba3fec3b631c3/src/com/esotericsoftware/kryo/util/IdentityMap.java.

public final class IdentityMap<K, V> extends ForyObjectMap<K, V> {
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
