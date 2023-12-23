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

package io.fury.collection;

import java.util.Arrays;
import java.util.List;

public class ImmutableList {

  /** Private constructor. This is a static utility class. */
  private ImmutableList() {}

  /**
   * Returns an immutable list containing the given elements, in order.
   *
   * @throws NullPointerException if any element is null
   */
  @SafeVarargs
  public static <E> List<E> of(E... elements) {
    checkElementsNotNull(elements, elements.length);
    return java.util.Collections.unmodifiableList(Arrays.asList(elements));
  }

  private static <E> void checkElementsNotNull(E[] array, int length) {
    for (int i = 0; i < length; i++) {
      if (array[i] == null) {
        throw new NullPointerException("at index " + i);
      }
    }
  }
}
