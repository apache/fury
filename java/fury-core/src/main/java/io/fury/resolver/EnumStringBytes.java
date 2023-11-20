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

package io.fury.resolver;

import io.fury.annotation.Internal;
import io.fury.util.MurmurHash3;
import io.fury.util.Preconditions;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

@Internal
public final class EnumStringBytes {
  static final short DEFAULT_DYNAMIC_WRITE_STRING_ID = -1;

  final byte[] bytes;
  final long hashCode;
  short dynamicWriteStringId = DEFAULT_DYNAMIC_WRITE_STRING_ID;

  /**
   * Create a binary EnumString.
   *
   * @param bytes String encoded bytes.
   * @param hashCode String hash code. This should be unique and has no hash collision, and be
   *     deterministic, so we can use cache to reduce hash loop up for read.
   */
  public EnumStringBytes(byte[] bytes, long hashCode) {
    assert hashCode != 0;
    this.bytes = bytes;
    this.hashCode = hashCode;
  }

  public EnumStringBytes(String string) {
    byte[] classNameBytes = string.getBytes(StandardCharsets.UTF_8);
    Preconditions.checkArgument(classNameBytes.length <= Short.MAX_VALUE);
    // Set seed to ensure hash is deterministic.
    long hashCode =
        MurmurHash3.murmurhash3_x64_128(classNameBytes, 0, classNameBytes.length, 47)[0];
    if (hashCode == 0) {
      // Ensure hashcode is not 0, so we can do some optimization to avoid boxing.
      hashCode += 1;
    }
    this.bytes = classNameBytes;
    this.hashCode = hashCode;
  }

  @Override
  public boolean equals(Object o) {
    // EnumStringBytes is used internally, skip unnecessary parameter check.
    // if (this == o) {
    //   return true;
    // }
    // if (o == null || getClass() != o.getClass()) {
    //   return false;
    // }
    EnumStringBytes that = (EnumStringBytes) o;
    // Skip compare data for equality for performance.
    // Enum string such as classname are very common, compare hashcode only will have better
    // performance.
    // Java hashcode is 32-bit, so comparing hashCode equality is necessary here.
    return hashCode == that.hashCode;
  }

  public byte[] getBytes() {
    return bytes;
  }

  @Override
  public int hashCode() {
    // equals will compare 8 byte hash code.
    return (int) hashCode;
  }

  @Override
  public String toString() {
    // TODO support other str encoding.
    String str = new String(bytes);
    ;
    return "string: " + str + " " + "size: " + bytes.length + " " + Arrays.toString(bytes);
  }
}
