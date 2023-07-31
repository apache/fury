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

import java.util.HashMap;

/**
 * Map factory to create maps.
 *
 * @author chaokunyang
 */
public class Maps {

  /** Create a {@link HashMap} from provided kv pairs. */
  @SuppressWarnings({"rawtypes", "unchecked"})
  public static <K, V> HashMap<K, V> ofHashMap(Object... kv) {
    if (kv.length % 2 != 0) {
      throw new IllegalArgumentException(
          String.format("entries got %d objects, which aren't pairs", kv.length));
    }
    int size = kv.length / 2;
    HashMap map = new HashMap<>(size);
    for (int i = 0; i < size; i += 2) {
      map.put(kv[i], kv[i + 1]);
    }
    return map;
  }
}
