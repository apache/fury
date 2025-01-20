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

package org.apache.fury.resolver;

import java.util.Arrays;
import org.apache.fury.annotation.Internal;
import org.apache.fury.memory.LittleEndian;
import org.apache.fury.memory.Platform;
import org.apache.fury.meta.MetaString;
import org.apache.fury.meta.MetaStringDecoder;
import org.apache.fury.util.MurmurHash3;

@Internal
public final class MetaStringBytes {
  static final short DEFAULT_DYNAMIC_WRITE_STRING_ID = -1;
  private static final int HEADER_MASK = 0xff;

  final byte[] bytes;
  final long hashCode;
  final MetaString.Encoding encoding;
  final long first8Bytes;
  final long second8Bytes;
  short dynamicWriteStringId = DEFAULT_DYNAMIC_WRITE_STRING_ID;

  /**
   * Create a binary MetaString.
   *
   * @param bytes String encoded bytes.
   * @param hashCode String hash code. This should be unique and has no hash collision, and be
   *     deterministic, so we can use cache to reduce hash loop up for read.
   */
  MetaStringBytes(final byte[] bytes, long hashCode) {
    assert hashCode != 0;
    this.bytes = bytes;
    this.hashCode = hashCode;
    int header = (int) (hashCode & HEADER_MASK);
    this.encoding = MetaString.Encoding.fromInt(header);
    byte[] data = bytes;
    if (bytes.length < 16) {
      data = new byte[16];
      System.arraycopy(bytes, 0, data, 0, bytes.length);
    }
    first8Bytes = LittleEndian.getInt64(data, Platform.BYTE_ARRAY_OFFSET);
    second8Bytes = LittleEndian.getInt64(data, Platform.BYTE_ARRAY_OFFSET + 8);
  }

  static MetaStringBytes of(MetaString metaString) {
    byte[] bytes = metaString.getBytes();
    // Set seed to ensure hash is deterministic.
    long hashCode = MurmurHash3.murmurhash3_x64_128(bytes, 0, bytes.length, 47)[0];
    hashCode = Math.abs(hashCode);
    if (hashCode == 0) {
      // Ensure hashcode is not 0, so we can do some optimization to avoid boxing.
      hashCode += 256; // last byte is reserved for header.
    }
    hashCode &= 0xffffffffffffff00L;
    MetaString.Encoding encoding = metaString.getEncoding();
    int header = encoding.getValue() & HEADER_MASK;
    hashCode = hashCode | header;
    return new MetaStringBytes(bytes, hashCode);
  }

  public String decode(char specialChar1, char specialChar2) {
    return decode(new MetaStringDecoder(specialChar1, specialChar2));
  }

  public String decode(MetaStringDecoder decoder) {
    return decoder.decode(bytes, encoding);
  }

  @Override
  public boolean equals(Object o) {
    // MetaStringBytes is used internally, skip unnecessary parameter check.
    // if (this == o) {
    //   return true;
    // }
    // if (o == null || getClass() != o.getClass()) {
    //   return false;
    // }
    MetaStringBytes that = (MetaStringBytes) o;
    // Skip compare data for equality for performance.
    // Enum string such as classname are very common, compare hashcode only will have better
    // performance.
    // Java hashcode is 32-bit, so comparing hashCode equality is necessary here.
    return hashCode == that.hashCode;
  }

  @Override
  public int hashCode() {
    // equals will compare 8 byte hash code.
    return (int) (hashCode >> 1);
  }

  @Override
  public String toString() {
    return "MetaStringBytes{"
        + "hashCode="
        + hashCode
        + ", size="
        + bytes.length
        + ", bytes="
        + Arrays.toString(bytes)
        + '}';
  }
}
