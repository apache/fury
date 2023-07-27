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
