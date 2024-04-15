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

package org.apache.fury.meta;

import java.util.Arrays;
import java.util.Objects;

public class MetaString {
  public enum Encoding {
    LOWER_SPECIAL(0x00),
    LOWER_UPPER_DIGIT_SPECIAL(0x01),
    REP_FIRST_TO_LOWER_SPECIAL(0x02),
    REP_ALL_TO_LOWER_SPECIAL(0x03),
    UTF_8(0x04); // Using UTF-8 as the fallback

    private final int value;

    Encoding(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    public static Encoding fromInt(int value) {
      for (Encoding encoding : values()) {
        if (encoding.getValue() == value) {
          return encoding;
        }
      }
      throw new IllegalArgumentException("Encoding flag not recognized: " + value);
    }
  }

  private final Encoding encoding;
  private final char specialChar1;
  private final char specialChar2;
  private final byte[] bytes;
  private final int numBits;

  public MetaString(Encoding encoding, byte[] bytes, int numBits) {
    this(encoding, '_', '$', bytes, numBits);
  }

  public MetaString(
      Encoding encoding, char specialChar1, char specialChar2, byte[] bytes, int numBits) {
    this.encoding = encoding;
    this.specialChar1 = specialChar1;
    this.specialChar2 = specialChar2;
    this.bytes = bytes;
    this.numBits = numBits;
  }

  public Encoding getEncoding() {
    return encoding;
  }

  public char getSpecialChar1() {
    return specialChar1;
  }

  public char getSpecialChar2() {
    return specialChar2;
  }

  public byte[] getBytes() {
    return bytes;
  }

  public int getNumBits() {
    return numBits;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MetaString that = (MetaString) o;
    return specialChar1 == that.specialChar1
        && specialChar2 == that.specialChar2
        && numBits == that.numBits
        && encoding == that.encoding
        && Arrays.equals(bytes, that.bytes);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(encoding, specialChar1, specialChar2, numBits);
    result = 31 * result + Arrays.hashCode(bytes);
    return result;
  }

  @Override
  public String toString() {
    return "MetaString{"
        + "encoding="
        + encoding
        + ", specialChar1="
        + specialChar1
        + ", specialChar2="
        + specialChar2
        + ", bytes="
        + Arrays.toString(bytes)
        + ", numBits="
        + numBits
        + '}';
  }
}
