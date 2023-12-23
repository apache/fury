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

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

public class ImmutableSet {

  /**
   * Private constructor.
   * This is a static utility class.
   */
  private ImmutableSet() { }

    /**
     * Returns an immutable set containing the given elements, in order.
     *
     * @throws NullPointerException if any element is null
     */
    @SafeVarargs
    public static <E> Set<E> of(E... elements) {
      return construct(new HashSet<>(elements.length), elements);
    }

    /**
     * Returns an immutable set containing the given elements, in order.
     *
     * @throws NullPointerException if any element is null
     */
    @SafeVarargs
    public static <E> Set<E> ofSorted(E... elements) {
      return construct(new TreeSet<>(), elements);
    }

    @SafeVarargs
    private static <E> Set<E> construct(Set<E> set, E... elements) {
      for (int i = 0; i < elements.length; i++) {
        checkElementNotNull(elements[i], i);
        set.add(elements[i]);
      }
      return java.util.Collections.unmodifiableSet(set);
    }


  private static <E> void checkElementNotNull(E element, int index) {
    if (element == null) {
      throw new NullPointerException("at index " + index);
    }
  }

}
