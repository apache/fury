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

package org.apache.fury.util;

import com.google.common.io.BaseEncoding;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class StringUtils {

  /** Converts a bytes array into a hexadecimal string. */
  public static String encodeHexString(final byte[] data) {
    return BaseEncoding.base16().lowerCase().encode(data);
  }

  /** Format a string template by replacing all `${xxx}` into provided values. */
  public static String format(String str, Object... args) {
    // TODO(chaokunyang) optimize performance.
    // TODO(chaokunyang) support `$xxx`.
    StringBuilder builder = new StringBuilder(str);
    if (args.length % 2 != 0) {
      throw new IllegalArgumentException(
          "args length must be multiple of 2, but get " + args.length);
    }
    Map<String, String> values = new HashMap<>();
    for (int i = 0; i < args.length; i += 2) {
      values.put(args[i].toString(), args[i + 1].toString());
    }

    for (Map.Entry<String, String> entry : values.entrySet()) {
      int start;
      String pattern = "${" + entry.getKey() + "}";
      String value = entry.getValue();

      // Replace every occurrence of %(key) with value
      while ((start = builder.indexOf(pattern)) != -1) {
        builder.replace(start, start + pattern.length(), value);
      }
    }

    return builder.toString();
  }

  public static String stripBlankLines(String str) {
    StringBuilder builder = new StringBuilder();
    String[] split = str.split("\n");
    for (String s : split) {
      if (!s.trim().isEmpty()) {
        builder.append(s).append('\n');
      }
    }
    if (builder.charAt(builder.length() - 1) == '\n') {
      return builder.toString();
    } else {
      return builder.substring(0, builder.length() - 1);
    }
  }

  public static String random(int size) {
    return random(size, new Random());
  }

  public static String random(int size, int rand) {
    return random(size, new Random(rand));
  }

  public static String random(int size, Random random) {
    char[] chars = new char[size];
    char start = ' ';
    char end = 'z' + 1;
    int gap = end - start;
    for (int i = 0; i < size; i++) {
      chars[i] = (char) (start + random.nextInt(gap));
    }
    return new String(chars);
  }

  /**
   * Capitalizes a String changing the first character to title case as per {@link
   * Character#toTitleCase(int)}. No other characters are changed.
   */
  // Copied from `org.apache.commons.lang3.StringUtils` to avoid introducing a new dependency.
  public static String capitalize(final String str) {
    int strLen;
    if (str == null || (strLen = str.length()) == 0) {
      return str;
    }

    final int firstCodepoint = str.codePointAt(0);
    final int newCodePoint = Character.toTitleCase(firstCodepoint);
    if (firstCodepoint == newCodePoint) {
      // already capitalized
      return str;
    }

    final int[] newCodePoints = new int[strLen]; // cannot be longer than the char array
    int outOffset = 0;
    newCodePoints[outOffset++] = newCodePoint; // copy the first codepoint
    for (int inOffset = Character.charCount(firstCodepoint); inOffset < strLen; ) {
      final int codepoint = str.codePointAt(inOffset);
      newCodePoints[outOffset++] = codepoint; // copy the remaining ones
      inOffset += Character.charCount(codepoint);
    }
    return new String(newCodePoints, 0, outOffset);
  }

  /** Uncapitalizes a String, changing the first character to lower case. */
  // Copied from `org.apache.commons.lang3.StringUtils` to avoid introducing a new dependency.
  public static String uncapitalize(final String str) {
    int strLen;
    if (str == null || (strLen = str.length()) == 0) {
      return str;
    }

    final int firstCodepoint = str.codePointAt(0);
    final int newCodePoint = Character.toLowerCase(firstCodepoint);
    if (firstCodepoint == newCodePoint) {
      // already capitalized
      return str;
    }

    final int[] newCodePoints = new int[strLen]; // cannot be longer than the char array
    int outOffset = 0;
    newCodePoints[outOffset++] = newCodePoint; // copy the first codepoint
    for (int inOffset = Character.charCount(firstCodepoint); inOffset < strLen; ) {
      final int codepoint = str.codePointAt(inOffset);
      newCodePoints[outOffset++] = codepoint; // copy the remaining ones
      inOffset += Character.charCount(codepoint);
    }
    return new String(newCodePoints, 0, outOffset);
  }

  /**
   * Checks if a CharSequence is empty (""), null or whitespace only. Whitespace is defined by
   * Character.isWhitespace(char).
   */
  // Copied from `org.apache.commons.lang3.StringUtils` to avoid introducing a new dependency.
  public static boolean isBlank(final CharSequence cs) {
    int strLen;
    if (cs == null || (strLen = cs.length()) == 0) {
      return true;
    }
    for (int i = 0; i < strLen; i++) {
      if (!Character.isWhitespace(cs.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  // Copied from `org.apache.commons.lang3.StringUtils` to avoid introducing a new dependency.
  public static boolean isNotBlank(final CharSequence cs) {
    return !isBlank(cs);
  }

  public static void shuffle(StringBuilder sb) {
    shuffle(sb, 7);
  }

  public static void shuffle(StringBuilder sb, int seed) {
    Random rand = new Random(seed);
    for (int i = sb.length() - 1; i > 1; i--) {
      int swapWith = rand.nextInt(i);
      char tmp = sb.charAt(swapWith);
      sb.setCharAt(swapWith, sb.charAt(i));
      sb.setCharAt(i, tmp);
    }
  }
}
