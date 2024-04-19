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
import org.apache.fury.meta.MetaString;
import org.apache.fury.meta.MetaStringDecoder;
import org.apache.fury.util.MurmurHash3;

@Internal
final class MetaStringBytes {
  static final int STRIP_LAST_CHAR = 0b1000;
  static final short DEFAULT_DYNAMIC_WRITE_STRING_ID = -1;

  final byte[] bytes;
  final long hashCode;
  short dynamicWriteStringId = DEFAULT_DYNAMIC_WRITE_STRING_ID;

  /**
   * Create a binary MetaString.
   *
   * @param bytes String encoded bytes.
   * @param hashCode String hash code. This should be unique and has no hash collision, and be
   *     deterministic, so we can use cache to reduce hash loop up for read.
   */
  public MetaStringBytes(byte[] bytes, long hashCode) {
    assert hashCode != 0;
    this.bytes = bytes;
    this.hashCode = hashCode;
  }

  public MetaStringBytes(MetaString metaString) {
    this.bytes = metaString.getBytes();
    // Set seed to ensure hash is deterministic.
    long hashCode = MurmurHash3.murmurhash3_x64_128(bytes, 0, bytes.length, 47)[0];
    if (hashCode == 0) {
      // Ensure hashcode is not 0, so we can do some optimization to avoid boxing.
      hashCode += 256; // last byte is reserved for header.
    }
    hashCode &= 0xffffffffffffff00L;
    int header = metaString.getEncoding().getValue();
    String decoded =
        new MetaStringDecoder(metaString.getSpecialChar1(), metaString.getSpecialChar2())
            .decode(bytes, metaString.getEncoding(), bytes.length * 8);
    if (decoded.length() > metaString.getString().length()) {
      header |= STRIP_LAST_CHAR;
    }
    this.hashCode = hashCode | header;
  }

  public String decode(char specialChar1, char specialChar2) {
    int header = (int) (hashCode & 0xff);
    int encodingFlags = header & 0b111;
    MetaString.Encoding encoding = MetaString.Encoding.values()[encodingFlags];
    String str =
        new MetaStringDecoder(specialChar1, specialChar2).decode(bytes, encoding, bytes.length * 8);
    if ((header & STRIP_LAST_CHAR) != 0) {
      str = str.substring(0, str.length() - 1);
    }
    return str;
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
    // TODO support other str encoding.
    String str = new String(bytes);
    ;
    return "string: " + str + " " + "size: " + bytes.length + " " + Arrays.toString(bytes);
  }
}
