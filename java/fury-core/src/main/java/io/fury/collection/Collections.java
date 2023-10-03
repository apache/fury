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

import static io.fury.util.unsafe._Collections.setArrayListElements;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Collections {
  /**
   * Returns a sequential {@link Stream} of the contents of {@code iterable}, delegating to {@link
   * Collection#stream} if possible.
   */
  public static <T> Stream<T> stream(Iterable<T> iterable) {
    return (iterable instanceof Collection)
        ? ((Collection<T>) iterable).stream()
        : StreamSupport.stream(iterable.spliterator(), false);
  }

  /** Create an {@link ArrayList} from provided elements. */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static <T> ArrayList<T> ofArrayList(T... elements) {
    ArrayList list = new ArrayList(elements.length);
    setArrayListElements(list, elements);
    return list;
  }

  /** Create a {@link HashMap} from provided kv pairs. */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static <K, V> HashMap<K, V> ofHashMap(Object... kv) {
    if (kv.length % 2 != 0) {
      throw new IllegalArgumentException(
          String.format("entries got %d objects, which aren't pairs", kv.length));
    }
    int size = kv.length / 2;
    HashMap map = new HashMap<>(size);
    for (int i = 0; i < kv.length; i += 2) {
      map.put(kv[i], kv[i + 1]);
    }
    return map;
  }

  public static <E> HashSet<E> ofHashSet(E... elements) {
    HashSet<E> set = new HashSet<>(elements.length);
    java.util.Collections.addAll(set, elements);
    return set;
  }
}
