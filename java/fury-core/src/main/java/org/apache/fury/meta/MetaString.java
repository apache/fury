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

/**
 * Represents a string with metadata that describes its encoding. It supports different encodings
 * including special mechanisms for lower-case alphabets with special characters, and upper-case and
 * digit encoding.
 */
public class MetaString {
  /** Defines the types of supported encodings for MetaStrings. */
  public enum Encoding {
    UTF_8(0x00), // Using UTF-8 as the fallback
    LOWER_SPECIAL(0x01),
    LOWER_UPPER_DIGIT_SPECIAL(0x02),
    FIRST_TO_LOWER_SPECIAL(0x03),
    ALL_TO_LOWER_SPECIAL(0x04);

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

  private final String string;
  private final Encoding encoding;
  private final char specialChar1;
  private final char specialChar2;
  private final byte[] bytes;
  private final int numChars;
  private final int numBits;

  /**
   * Constructs a MetaString with the specified encoding and data.
   *
   * @param encoding The type of encoding used for the string data.
   * @param bytes The encoded string data as a byte array.
   * @param numBits The number of bits used for encoding.
   */
  public MetaString(
      String string,
      Encoding encoding,
      char specialChar1,
      char specialChar2,
      byte[] bytes,
      int numChars,
      int numBits) {
    this.string = string;
    this.encoding = encoding;
    this.specialChar1 = specialChar1;
    this.specialChar2 = specialChar2;
    this.bytes = bytes;
    this.numChars = numChars;
    this.numBits = numBits;
  }

  public String getString() {
    return string;
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

  public int getNumChars() {
    return numChars;
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
        && numChars == that.numChars
        && numBits == that.numBits
        && encoding == that.encoding
        && Arrays.equals(bytes, that.bytes);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(encoding, specialChar1, specialChar2, numChars, numBits);
    result = 31 * result + Arrays.hashCode(bytes);
    return result;
  }

  @Override
  public String toString() {
    return "MetaString{"
        + "str="
        + string
        + ", encoding="
        + encoding
        + ", specialChar1="
        + specialChar1
        + ", specialChar2="
        + specialChar2
        + ", bytes="
        + Arrays.toString(bytes)
        + ", numChars="
        + numChars
        + ", numBits="
        + numBits
        + '}';
  }
}
