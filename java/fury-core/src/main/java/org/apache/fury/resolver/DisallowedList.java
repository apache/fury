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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.fury.exception.InsecureException;

/** A class to record which classes are not allowed for serialization. */
class DisallowedList {
  private static final String DISALLOWED_LIST_TXT_PATH = "fury/disallowed.txt";
  private static final Set<String> DEFAULT_DISALLOWED_LIST_SET;

  static {
    try (InputStream is =
        DisallowedList.class.getClassLoader().getResourceAsStream(DISALLOWED_LIST_TXT_PATH)) {
      if (is != null) {
        DEFAULT_DISALLOWED_LIST_SET =
            new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
                .lines()
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
