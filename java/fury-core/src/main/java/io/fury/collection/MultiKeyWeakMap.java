/*
 * Copyright 2023 The Fury authors
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import com.google.common.base.FinalizableReferenceQueue;
import com.google.common.base.FinalizableWeakReference;
import com.google.common.collect.Sets;
import java.lang.ref.Reference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Hash table based implementation with weak keys. An entry in a MultiKeyWeakMap will automatically
 * be removed when all keys are no longer in ordinary use. More precisely, the presence of a mapping
 * for the given keys will not prevent the keys from being discarded by the garbage collector.
 *
 * @param <T> the type of values maintained by this map
 * @see java.util.WeakHashMap
 * @author chaokunyang
 */
public class MultiKeyWeakMap<T> {
  private static final FinalizableReferenceQueue REFERENCE_QUEUE = new FinalizableReferenceQueue();
  private static final Set<Reference<?>> REFERENCES = Sets.newConcurrentHashSet();
  private final Map<Object, T> map;

  public MultiKeyWeakMap() {
    map = new ConcurrentHashMap<>();
  }

  public void put(Object[] keys, T value) {
    map.put(createKey(keys), value);
  }

  public T get(Object[] keys) {
    List<KeyReference> keyRefs = createKey(keys);
    T t = map.get(keyRefs);
    keyRefs.forEach(REFERENCES::remove);
    return t;
  }

  private List<KeyReference> createKey(Object[] keys) {
    boolean[] reclaimedFlags = new boolean[keys.length];
    List<KeyReference> keyRefs = new ArrayList<>();
    for (int i = 0; i < keys.length; i++) {
      keyRefs.add(new KeyReference(keys[i], keyRefs, reclaimedFlags, i));
    }
    return keyRefs;
  }

  private final class KeyReference extends FinalizableWeakReference<Object> {
    private final boolean[] reclaimedFlags;
    private final int index;
    private final List<KeyReference> keyRefs;
    private final int hashcode;

    public KeyReference(
        Object obj, List<KeyReference> keyRefs, boolean[] reclaimedFlags, int index) {
      super(obj, REFERENCE_QUEUE);
      this.reclaimedFlags = reclaimedFlags;
      this.index = index;
      this.keyRefs = keyRefs;
      hashcode = obj.hashCode();
      REFERENCES.add(this);
    }

    @Override
    public void finalizeReferent() {
      reclaimedFlags[index] = true;
      REFERENCES.remove(this);
      if (IntStream.range(0, reclaimedFlags.length).allMatch(i -> reclaimedFlags[i])) {
        map.remove(keyRefs);
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      @SuppressWarnings("unchecked")
      KeyReference that = (KeyReference) o;
      Object referent1 = this.get();
      if (referent1 != null) {
        return referent1.equals(that.get());
      } else {
        // referent not exists, continue compare is meaningless.
        return false;
      }
    }

    @Override
    public int hashCode() {
      return hashcode;
    }

    @Override
    public String toString() {
      return "KeyReference{"
          + "reclaimedFlags="
          + Arrays.toString(reclaimedFlags)
          + ", index="
          + index
          + ", keyRefs="
          + keyRefs.stream().map(Reference::get).collect(Collectors.toList())
          + ", hashcode="
          + hashcode
          + '}';
    }
  }
}
