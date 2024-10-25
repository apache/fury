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

package org.apache.fury.collection;

import org.apache.fury.annotation.Internal;
import org.apache.fury.util.Preconditions;

// Derived from
// https://github.com/EsotericSoftware/kryo/blob/135df69526615bb3f6b34846e58ba3fec3b631c3/src/com/esotericsoftware/kryo/util/IntMap.java.
@SuppressWarnings("unchecked")
@Internal
public final class LongLongMap<V> {
  private static final class LongLongKey {
    private final long k1;

    public LongLongKey(long k1, long k2) {
      this.k1 = k1;
      this.k2 = k2;
    }

    private final long k2;

    @Override
    public String toString() {
      return "LongLongKey{" +
        "k1=" + k1 +
        ", k2=" + k2 +
        '}';
    }
  }

  public int size;
  LongLongKey[] keyTable;
  V[] valueTable;
  private final float loadFactor;
  private int threshold;

  /**
   * Used by {@link #place(long, long)} to bit shift the upper bits of a {@code long} into a usable range
   * (&gt;= 0 and &lt;= {@link #mask}). The shift can be negative, which is convenient to match the
   * number of bits in mask: if mask is a 7-bit number, a shift of -7 shifts the upper 7 bits into
   * the lowest 7 positions. This class sets the shift &gt; 32 and &lt; 64, which if used with an
   * int will still move the upper bits of an int to the lower bits due to Java's implicit modulus
   * on shifts.
   *
   * <p>{@link #mask} can also be used to mask the low bits of a number, which may be faster for
   * some hashcodes, if {@link #place(long, long)} is overridden.
   */
  private int shift;

  /**
   * A bitmask used to confine hash codes to the size of the table. Must be all 1 bits in its low
   * positions, ie a power of two minus 1. If {@link #place(long, long)} is override, this can be used
   * instead of {@link #shift} to isolate usable bits of a hash.
   */
  private int mask;

  /**
   * Creates a new map with the specified initial capacity and load factor. This map will hold
   * initialCapacity items before growing the backing table.
   *
   * @param initialCapacity If not a power of two, it is increased to the next nearest power of two.
   */
  public LongLongMap(int initialCapacity, float loadFactor) {
    Preconditions.checkArgument(0 <= loadFactor && loadFactor <= 1,
      "loadFactor %s must be > 0 and < 1", loadFactor);
    this.loadFactor = loadFactor;
    int tableSize = FuryObjectMap.tableSize(initialCapacity, loadFactor);
    threshold = (int) (tableSize * loadFactor);
    mask = tableSize - 1;
    shift = Long.numberOfLeadingZeros(mask);
    keyTable = new LongLongKey[tableSize];
    valueTable = (V[]) new Object[tableSize];
  }


  private int place(long k1, long k2) {
    return (int) ((k1 * 31 + k2) * 0x9E3779B97F4A7C15L >>> shift);
  }

  /**
   * Returns the index of the key if already present, else -(index + 1) for the next empty index.
   * This can be overridden in this pacakge to compare for equality differently than {@link
   * Object#equals(Object)}.
   */
  private int locateKey(long k1, long k2) {
    LongLongKey[] keyTable = this.keyTable;
    int mask = this.mask;
    for (int i = place(k1, k2); ; i = i + 1 & mask) {
      LongLongKey other = keyTable[i];
      if (other == null) {
        return -(i + 1); // Empty space is available.
      }
      if (other.k1 == k1 && other.k2 == k2) {
        return i; // Same key was found.
      }
    }
  }

  public V put(long k1, long k2, V value) {
    assert k1 != 0;
    int i = locateKey(k1, k2);
    if (i >= 0) { // Existing key was found.
      V[] valueTable = this.valueTable;
      V oldValue = valueTable[i];
      valueTable[i] = value;
      return oldValue;
    }
    i = -(i + 1); // Empty space was found.
    keyTable[i] = new LongLongKey(k1, k2);
    valueTable[i] = value;
    if (++size >= threshold) {
      resize(keyTable.length << 1);
    }
    return null;
  }

  public V get(long k1, long k2) {
    assert k1 != 0;
    LongLongKey[] keyTable = this.keyTable;
    for (int i = place(k1, k2); ; i = i + 1 & mask) {
      LongLongKey other = keyTable[i];
      if (other == null) {
        return null;
      }
      if (other.k1 == k1 && other.k2 == k2) {
        return valueTable[i];
      }
    }
  }

  private void resize(int newSize) {
    int oldCapacity = keyTable.length;
    threshold = (int) (newSize * loadFactor);
    mask = newSize - 1;
    shift = Long.numberOfLeadingZeros(mask);
    LongLongKey[] oldKeyTable = keyTable;
    V[] oldValueTable = valueTable;
    keyTable = new LongLongKey[newSize];
    valueTable = (V[]) new Object[newSize];
    if (size > 0) {
      for (int i = 0; i < oldCapacity; i++) {
        LongLongKey key = oldKeyTable[i];
        if (key != null) {
          putResize(key.k1, key.k2, oldValueTable[i]);
        }
      }
    }
  }

  private void putResize(long k1, long k2, V value) {
    LongLongKey[] keyTable = this.keyTable;
    for (int i = place(k1, k2); ; i = (i + 1) & mask) {
      if (keyTable[i] == null) {
        keyTable[i] = new LongLongKey(k1, k2);
        valueTable[i] = value;
        return;
      }
    }
  }
}
