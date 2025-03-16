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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.fury.exception.InsecureException;

/** A class to record which classes are not allowed for serialization. */
class DisallowedList {
  private static final String DISALLOWED_LIST_TXT_PATH = "fury/disallowed.txt";
  // when disallowed.txt changed, update this hash by result of `sha256sum disallowed.txt`
  private static final String SHA256_HASH =
      "30dc5228f52b02f61aff35a94d29ccd903abbf490d8231810c5e1c0321c56557";
  private static final Set<String> DEFAULT_DISALLOWED_LIST_SET;

  static {
    try (InputStream is =
        DisallowedList.class.getClassLoader().getResourceAsStream(DISALLOWED_LIST_TXT_PATH)) {
      if (is != null) {
        byte[] fileBytes = readAllBytes(is);
        String calculatedHash = calculateSHA256(fileBytes);
        if (!SHA256_HASH.equals(calculatedHash)) {
          // add a check to avoid some malicious overwrite disallowed.txt
          throw new SecurityException("Disallowed list has been tampered");
        }
        DEFAULT_DISALLOWED_LIST_SET =
            Arrays.stream(
                    new String(fileBytes, StandardCharsets.UTF_8).split(System.lineSeparator()))
                .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                .collect(Collectors.toSet());
      } else {
        throw new IllegalStateException(
            String.format("Read disallowed list %s failed", DISALLOWED_LIST_TXT_PATH));
      }
    } catch (IOException e) {
      throw new IllegalStateException(
          String.format("Read disallowed list %s failed", DISALLOWED_LIST_TXT_PATH), e);
    }
  }

  private static byte[] readAllBytes(InputStream inputStream) throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    int nRead;
    byte[] data = new byte[1024];
    while ((nRead = inputStream.read(data, 0, data.length)) != -1) {
      buffer.write(data, 0, nRead);
    }
    buffer.flush();
    return buffer.toByteArray();
  }

  private static String calculateSHA256(byte[] input) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hashBytes = digest.digest(input);
      StringBuilder hexString = new StringBuilder();
      for (byte b : hashBytes) {
        String hex = Integer.toHexString(0xff & b);
        if (hex.length() == 1) {
          hexString.append('0');
        }
        hexString.append(hex);
      }
      return hexString.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("SHA-256 algorithm not available", e);
    }
  }

  /**
   * Determine whether the current Class is in the default disallowed list.
   *
   * <p>Note that if Class exists in the disallowed list, {@link InsecureException} will be thrown.
   *
   * @param clsName Class Name that needs to be judged.
   * @throws InsecureException If the class is in the disallowed list.
   */
  static void checkNotInDisallowedList(String clsName) {
    if (DEFAULT_DISALLOWED_LIST_SET.contains(clsName)) {
      throw new InsecureException(String.format("%s hit disallowed list", clsName));
    }
  }
}
