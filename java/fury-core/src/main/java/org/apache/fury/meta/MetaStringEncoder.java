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

public class MetaStringEncoder {
  private final char specialChar1;
  private final char specialChar2;

  public MetaStringEncoder(char specialChar1, char specialChar2) {
    this.specialChar1 = specialChar1;
    this.specialChar2 = specialChar2;
  }

  public MetaString encode(String input) {
    if (input.isEmpty()) {
      return new MetaString(Encoding.LOWER_SPECIAL, new byte[0], 0);
    }
    Encoding encoding = computeEncoding(input);
    return encode(input, encoding);
  }

  public MetaString encode(String input, Encoding encoding) {
    int length = input.length();
    switch (encoding) {
      case LOWER_SPECIAL:
        return new MetaString(encoding, encodeLowerSpecial(input), length * 5);
      case LOWER_UPPER_DIGIT_SPECIAL:
        return new MetaString(encoding, encodeLowerUpperDigitSpecial(input), length * 6);
      case REP_FIRST_TO_LOWER_SPECIAL:
        return new MetaString(encoding, encodeRepFirstToLowerSpecial(input), length * 5);
      case REP_ALL_TO_LOWER_SPECIAL:
        char[] chars = input.toCharArray();
        int upperCount = countUppers(chars);
        return new MetaString(
            encoding, encodeRepAllToLowerSpecial(chars, upperCount), (upperCount + length) * 5);
      default:
        byte[] bytes = input.getBytes(StandardCharsets.UTF_8);
        return new MetaString(Encoding.UTF_8, bytes, bytes.length * 8);
    }
  }

  public Encoding computeEncoding(String input) {
    if (input.isEmpty()) {
      return Encoding.LOWER_SPECIAL;
    }
    char[] chars = input.toCharArray();
    if (canBeLowerSpecialEncoded(chars)) {
      return Encoding.LOWER_SPECIAL;
    } else if (canBeLowerUpperDigitSpecialEncoded(chars)) {
      if (countDigits(chars) != 0) {
        return Encoding.LOWER_UPPER_DIGIT_SPECIAL;
      } else {
        int upperCount = countUppers(chars);
        if (upperCount == 1 && Character.isUpperCase(chars[0])) {
          return Encoding.REP_FIRST_TO_LOWER_SPECIAL;
        }
        if ((chars.length + upperCount) * 5 < (chars.length * 6)) {
          return Encoding.REP_ALL_TO_LOWER_SPECIAL;
        } else {
          return Encoding.LOWER_UPPER_DIGIT_SPECIAL;
        }
      }
    }
    return Encoding.UTF_8;
  }

  private boolean canBeLowerSpecialEncoded(String input) {
    return canBeLowerSpecialEncoded(input.toCharArray());
  }

  private boolean canBeLowerSpecialEncoded(char[] chars) {
    for (char c : chars) {
      if (c >= 'a' && c <= 'z') continue;
      if (c == specialChar1 || c == specialChar2 || c == '|') continue;
      // Character outside of LOWER_SPECIAL set
      return false;
    }
    return true;
  }

  private boolean canBeLowerUpperDigitSpecialEncoded(String input) {
    return canBeLowerUpperDigitSpecialEncoded(input.toCharArray());
  }

  private int countDigits(char[] chars) {
    int count = 0;
    for (char c : chars) {
      if (Character.isDigit(c)) {
        count++;
      }
    }
    return count;
  }

  private int countUppers(char[] chars) {
    int upperCount = 0;
    for (char c : chars) {
      if (Character.isUpperCase(c)) {
        upperCount++;
      }
    }
    return upperCount;
  }

  private boolean canBeLowerUpperDigitSpecialEncoded(char[] chars) {
    for (char c : chars) {
      if (c >= 'a' && c <= 'z') continue;
      if (c >= 'A' && c <= 'Z') continue;
      if (c >= '0' && c <= '9') continue;
      if (c == specialChar1 || c == specialChar2) continue;
      // Character outside of LOWER_UPPER_DIGIT_SPECIAL set
      return false;
    }
    return true;
  }

  public byte[] encodeLowerSpecial(String input) {
    return encodeGeneric(input, 5, true);
  }

  public byte[] encodeLowerUpperDigitSpecial(String input) {
    return encodeGeneric(input, 6, false);
  }

  public byte[] encodeRepFirstToLowerSpecial(String input) {
    return encodeRepFirstToLowerSpecial(input.toCharArray());
  }

  public byte[] encodeRepFirstToLowerSpecial(char[] chars) {
    chars[0] = Character.toLowerCase(chars[0]);
    return encodeGeneric(chars, 6, false);
  }

  public byte[] encodeRepAllToLowerSpecial(char[] chars) {
    return encodeRepAllToLowerSpecial(chars, countUppers(chars));
  }

  public byte[] encodeRepAllToLowerSpecial(char[] chars, int upperCount) {
    char[] newChars = new char[chars.length + upperCount];
    int newIdx = 0;
    for (char aChar : chars) {
      if (Character.isUpperCase(aChar)) {
        newChars[newIdx++] = '|';
        newChars[newIdx++] = Character.toLowerCase(aChar);
      } else {
        newChars[newIdx++] = aChar;
      }
    }
    return encodeGeneric(newChars, 5, true);
  }

  private byte[] encodeGeneric(String input, int bitsPerChar, boolean lowerSpecial) {
    return encodeGeneric(input.toCharArray(), bitsPerChar, lowerSpecial);
  }

  private byte[] encodeGeneric(char[] chars, int bitsPerChar, boolean lowerSpecial) {
    int totalBits = chars.length * bitsPerChar;
    int byteLength = (totalBits + 7) / 8; // Calculate number of needed bytes
    byte[] bytes = new byte[byteLength];
    int currentBit = 0;
    for (char c : chars) {
      int value =
          (lowerSpecial) ? charToValueLowerSpecial(c) : charToValueLowerUpperDigitSpecial(c);
      // Encode the value in bitsPerChar bits
      for (int i = bitsPerChar - 1; i >= 0; i--) {
        if ((value & (1 << i)) != 0) {
          // Set the bit in the byte array
          int bytePos = currentBit / 8;
          int bitPos = currentBit % 8;
          bytes[bytePos] |= (byte) (1 << (7 - bitPos));
        }
        currentBit++;
      }
    }

    return bytes;
  }

  private int charToValueLowerSpecial(char c) {
    if (c >= 'a' && c <= 'z') {
      return c - 'a';
    } else if (c == '.') {
      return 26;
    } else if (c == '_') {
      return 27;
    } else if (c == '$') {
      return 28;
    } else if (c == '|') {
      return 29;
    } else {
      throw new IllegalArgumentException("Unsupported character for LOWER_SPECIAL encoding: " + c);
    }
  }

  private int charToValueLowerUpperDigitSpecial(char c) {
    if (c >= 'a' && c <= 'z') {
      return c - 'a';
    } else if (c >= 'A' && c <= 'Z') {
      return 26 + (c - 'A');
    } else if (c >= '0' && c <= '9') {
      return 52 + (c - '0');
    } else if (c == specialChar1) {
      return 62;
    } else if (c == specialChar2) {
      return 63;
    } else {
      throw new IllegalArgumentException(
          "Unsupported character for LOWER_UPPER_DIGIT_SPECIAL encoding: " + c);
    }
  }
}