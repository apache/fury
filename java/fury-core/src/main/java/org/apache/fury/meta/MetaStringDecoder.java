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

import java.nio.charset.StandardCharsets;
import org.apache.fury.meta.MetaString.Encoding;
import org.apache.fury.util.StringUtils;

/** Decodes MetaString objects back into their original plain text form. */
public class MetaStringDecoder {
  private final char specialChar1;
  private final char specialChar2;

  /**
   * Creates a MetaStringDecoder with specified special characters used for decoding.
   *
   * @param specialChar1 The first special character used in custom decoding.
   * @param specialChar2 The second special character used in custom decoding.
   */
  public MetaStringDecoder(char specialChar1, char specialChar2) {
    this.specialChar1 = specialChar1;
    this.specialChar2 = specialChar2;
  }

  /**
   * Decode data based on passed <code>encoding</code>. The data must be encoded using passed
   * encoding.
   *
   * @param encodedData encoded data using passed <code>encoding</code>.
   * @param encoding encoding the passed data.
   * @param numBits total bits for encoded data.
   * @return Decoded string.
   */
  public String decode(byte[] encodedData, Encoding encoding, int numBits) {
    switch (encoding) {
      case LOWER_SPECIAL:
        return decodeLowerSpecial(encodedData, numBits);
      case LOWER_UPPER_DIGIT_SPECIAL:
        return decodeLowerUpperDigitSpecial(encodedData, numBits);
      case FIRST_TO_LOWER_SPECIAL:
        return decodeRepFirstLowerSpecial(encodedData, numBits);
      case ALL_TO_LOWER_SPECIAL:
        return decodeRepAllToLowerSpecial(encodedData, numBits);
      case UTF_8:
        return new String(encodedData, StandardCharsets.UTF_8);
      default:
        throw new IllegalStateException("Unexpected encoding flag: " + encoding);
    }
  }

  /** Decoding method for {@link Encoding#LOWER_SPECIAL}. */
  private String decodeLowerSpecial(byte[] data, int numBits) {
    StringBuilder decoded = new StringBuilder();
    int bitIndex = 0;
    int bitMask = 0b11111; // 5 bits for mask
    while (bitIndex + 5 <= numBits) {
      int byteIndex = bitIndex / 8;
      int intraByteIndex = bitIndex % 8;
      // Extract the 5-bit character value across byte boundaries if needed
      int charValue =
          ((data[byteIndex] & 0xFF) << 8)
              | (byteIndex + 1 < data.length ? (data[byteIndex + 1] & 0xFF) : 0);
      charValue = ((byte) ((charValue >> (11 - intraByteIndex)) & bitMask));
      bitIndex += 5;
      decoded.append(decodeLowerSpecialChar(charValue));
    }

    return decoded.toString();
  }

  /** Decoding method for {@link Encoding#LOWER_UPPER_DIGIT_SPECIAL}. */
  private String decodeLowerUpperDigitSpecial(byte[] data, int numBits) {
    StringBuilder decoded = new StringBuilder();
    int bitIndex = 0;
    int bitMask = 0b111111; // 6 bits for mask
    while (bitIndex + 6 <= numBits) {
      int byteIndex = bitIndex / 8;
      int intraByteIndex = bitIndex % 8;

      // Extract the 6-bit character value across byte boundaries if needed
      int charValue =
          ((data[byteIndex] & 0xFF) << 8)
              | (byteIndex + 1 < data.length ? (data[byteIndex + 1] & 0xFF) : 0);
      charValue = ((byte) ((charValue >> (10 - intraByteIndex)) & bitMask));
      bitIndex += 6;
      decoded.append(decodeLowerUpperDigitSpecialChar(charValue));
    }
    return decoded.toString();
  }

  /** Decoding special char for LOWER_SPECIAL based on encoding mapping. */
  private char decodeLowerSpecialChar(int charValue) {
    if (charValue >= 0 && charValue <= 25) {
      return (char) ('a' + charValue);
    } else if (charValue == 26) {
      return '.';
    } else if (charValue == 27) {
      return '_';
    } else if (charValue == 28) {
      return '$';
    } else if (charValue == 29) {
      return '|';
    } else {
      throw new IllegalArgumentException("Invalid character value for LOWER_SPECIAL: " + charValue);
    }
  }

  /** Decoding special char for LOWER_UPPER_DIGIT_SPECIAL based on encoding mapping. */
  private char decodeLowerUpperDigitSpecialChar(int charValue) {
    if (charValue >= 0 && charValue <= 25) {
      return (char) ('a' + charValue);
    } else if (charValue >= 26 && charValue <= 51) {
      return (char) ('A' + (charValue - 26));
    } else if (charValue >= 52 && charValue <= 61) {
      return (char) ('0' + (charValue - 52));
    } else if (charValue == 62) {
      return specialChar1;
    } else if (charValue == 63) {
      return specialChar2;
    } else {
      throw new IllegalArgumentException(
          "Invalid character value for LOWER_UPPER_DIGIT_SPECIAL: " + charValue);
    }
  }

  private String decodeRepFirstLowerSpecial(byte[] data, int numBits) {
    String str = decodeLowerSpecial(data, numBits);
    return StringUtils.capitalize(str);
  }

  private String decodeRepAllToLowerSpecial(byte[] data, int numBits) {
    String str = decodeLowerSpecial(data, numBits);
    StringBuilder builder = new StringBuilder();
    char[] chars = str.toCharArray();
    for (int i = 0; i < chars.length; i++) {
      if (chars[i] == '|') {
        char c = chars[++i];
        builder.append(Character.toUpperCase(c));
      } else {
        builder.append(chars[i]);
      }
    }
    return builder.toString();
  }
}
