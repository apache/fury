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
// https://github.com/EsotericSoftware/kryo/blob/135df69526615bb3f6b34846e58ba3fec3b631c3/src/com/esotericsoftware/kryo/util/IdentityObjectIntMap.java.

public final class IdentityObjectIntMap<K> extends ObjectIntMap<K> {

  private MapStatistics stat;

  public IdentityObjectIntMap(int initialCapacity, float loadFactor) {
    super(initialCapacity, loadFactor);
    stat = new MapStatistics();
  }

  protected int place(K item) {
    return System.identityHashCode(item) & mask;
  }

  int locateKey(K key) {
    K[] keyTable = this.keyTable;
    int mask = this.mask;
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

  public int get(K key, int defaultValue) {
    K[] keyTable = this.keyTable;
    int mask = this.mask;
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

  public void put(K key, int value) {
    K[] keyTable = this.keyTable;
    int mask = this.mask;
    for (int i = place(key); ; i = i + 1 & mask) {
      K other = keyTable[i];
      if (other == null) {
        keyTable[i] = key;
        valueTable[i] = value;
        if (++size >= threshold) {
          resize(keyTable.length << 1);
        }
        return;
      }
      if (other == key) {
        valueTable[i] = value;
        return;
      }
    }
  }

  /**
   * If key doesn't exist in map, return {@link Integer#MIN_VALUE}, otherwise don't update map, just
   * return previous value.
   */
  public int putOrGet(K key, int value) {
    K[] keyTable = this.keyTable;
    int mask = this.mask;
    for (int i = place(key); ; i = i + 1 & mask) {
      K other = keyTable[i];
      if (other == null) {
        keyTable[i] = key;
        valueTable[i] = value;
        if (++size >= threshold) {
          resize(keyTable.length << 1);
        }
        return Integer.MIN_VALUE;
      }
      if (other == key) {
        return valueTable[i];
      }
    }
  }

  public int profilingPutOrGet(K key, int value) {
    K[] keyTable = this.keyTable;
    int mask = this.mask;
    for (int i = place(key); ; i = i + 1 & mask) {
      stat.totalProbeProfiled++;
      K other = keyTable[i];
      if (other == null) {
        keyTable[i] = key;
        valueTable[i] = value;
        if (++size >= threshold) {
          resize(keyTable.length << 1);
        }
        int probed = stat.totalProbeProfiled - stat.lastProbeProfiled;
        stat.maxProbeProfiled = Math.max(probed, stat.maxProbeProfiled);
        stat.lastProbeProfiled = stat.totalProbeProfiled;
        return Integer.MIN_VALUE;
      }
      if (other == key) {
        int probed = stat.totalProbeProfiled - stat.lastProbeProfiled;
        stat.maxProbeProfiled = Math.max(probed, stat.maxProbeProfiled);
        stat.lastProbeProfiled = stat.totalProbeProfiled;
        return valueTable[i];
      }
    }
  }

  public MapStatistics getAndResetStatistics() {
    MapStatistics result = stat;
    stat = new MapStatistics();
    return result;
  }

  public int hashCode() {
    int h = size;
    K[] keyTable = this.keyTable;
    int[] valueTable = this.valueTable;
    for (int i = 0, n = keyTable.length; i < n; i++) {
      K key = keyTable[i];
      if (key != null) {
        h += System.identityHashCode(key) + valueTable[i];
      }
    }
    return h;
  }
}
