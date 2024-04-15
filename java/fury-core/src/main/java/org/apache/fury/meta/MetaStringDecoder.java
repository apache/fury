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
import java.util.Arrays;
import org.apache.fury.util.StringUtils;

public class MetaStringDecoder {
  private static final int FLAG_OFFSET = 8;

  private final char specialChar1;
  private final char specialChar2;

  public MetaStringDecoder(char specialChar1, char specialChar2) {
    this.specialChar1 = specialChar1;
    this.specialChar2 = specialChar2;
  }

  public String decode(byte[] encodedData, int numBits) {
    if (encodedData.length == 0) {
      return "";
    }
    // The very first byte signifies the encoding used
    MetaString.Encoding chosenEncoding = MetaString.Encoding.fromInt(encodedData[0] & 0xFF);
    // Extract actual data, skipping the first byte (encoding flag)
    encodedData = Arrays.copyOfRange(encodedData, 1, encodedData.length);
    return decode(encodedData, chosenEncoding, numBits);
  }

  public String decode(byte[] encodedData, MetaString.Encoding chosenEncoding, int numBits) {
    switch (chosenEncoding) {
      case LOWER_SPECIAL:
        return decodeLowerSpecial(encodedData, false, numBits);
      case LOWER_UPPER_DIGIT_SPECIAL:
        return decodeLowerUpperDigitSpecial(encodedData, false, numBits);
      case FIRST_TO_LOWER_SPECIAL:
        return decodeRepFirstLowerSpecial(encodedData, numBits);
      case ALL_TO_LOWER_SPECIAL:
        return decodeRepAllToLowerSpecial(encodedData, numBits);
      case UTF_8:
        return new String(encodedData, StandardCharsets.UTF_8);
      default:
        throw new IllegalStateException("Unexpected encoding flag: " + chosenEncoding);
    }
  }

  // Function to adjust indices by removing flag position if present
  private byte getValueWithFlagOffset(byte encodedValue, boolean hasFlag) {
    return hasFlag ? (byte) (encodedValue & ~(1 << FLAG_OFFSET)) : encodedValue;
  }

  // Decoding method for LOWER_SPECIAL
  private String decodeLowerSpecial(byte[] data, boolean hasFlag, int numBits) {
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
      charValue =
          getValueWithFlagOffset((byte) ((charValue >> (11 - intraByteIndex)) & bitMask), hasFlag);
      bitIndex += 5;
      decoded.append(decodeLowerSpecialChar(charValue));
    }

    return decoded.toString();
  }

  // Decoding method for LOWER_UPPER_DIGIT_SPECIAL
  private String decodeLowerUpperDigitSpecial(byte[] data, boolean hasFlag, int numBits) {
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
      charValue =
          getValueWithFlagOffset((byte) ((charValue >> (10 - intraByteIndex)) & bitMask), hasFlag);
      bitIndex += 6;
      decoded.append(decodeLowerUpperDigitSpecialChar(charValue));
    }

    return decoded.toString();
  }

  // Decoding special char for LOWER_SPECIAL based on your custom mapping
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

  // Decoding special char for LOWER_UPPER_DIGIT_SPECIAL based on your custom mapping
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
    String str = decodeLowerSpecial(data, false, numBits);
    return StringUtils.capitalize(str);
  }

  private String decodeRepAllToLowerSpecial(byte[] data, int numBits) {
    String str = decodeLowerSpecial(data, false, numBits);
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
