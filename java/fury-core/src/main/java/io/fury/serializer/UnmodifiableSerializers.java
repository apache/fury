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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.serializer;

import com.google.common.base.Preconditions;
import io.fury.Fury;
import io.fury.memory.MemoryBuffer;
import io.fury.serializer.CollectionSerializers.CollectionSerializer;
import io.fury.util.LoggerFactory;
import io.fury.util.Platform;
import io.fury.util.ReflectionUtils;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.slf4j.Logger;

/** Serializer for unmodifiable Collections and Maps created via Collections. */
// Modified from
// https://github.com/magro/kryo-serializers/blob/master/src/main/java/de/javakaffee/kryoserializers/UnmodifiableCollectionSerializer.java
// but faster by using unsafe instead of reflection.
public class UnmodifiableSerializers {
  private static final Logger LOG = LoggerFactory.getLogger(UnmodifiableSerializers.class);
  private static Field SOURCE_COLLECTION_FIELD;
  private static Field SOURCE_MAP_FIELD;
  private static final long SOURCE_COLLECTION_FIELD_OFFSET;
  private static final long SOURCE_MAP_FIELD_OFFSET;

  static {
    try {
      // UnmodifiableList/Set/Etc.. extends UnmodifiableCollection
      SOURCE_COLLECTION_FIELD =
          Collections.unmodifiableCollection(new ArrayList<>()).getClass().getDeclaredField("c");
      // UnmodifiableSortedMap/UnmodifiableNavigableMap extends UnmodifiableMap
      SOURCE_MAP_FIELD =
          Collections.unmodifiableMap(new HashMap<>()).getClass().getDeclaredField("m");
    } catch (Exception e) {
      LOG.warn(
          "Could not access source collection field in "
              + "java.util.Collections$UnmodifiableCollection: {}.",
          e.toString());
    }
    SOURCE_COLLECTION_FIELD_OFFSET = ReflectionUtils.getFieldOffset(SOURCE_COLLECTION_FIELD);
    SOURCE_MAP_FIELD_OFFSET = ReflectionUtils.getFieldOffset(SOURCE_MAP_FIELD);
  }

  public static final class UnmodifiableCollectionSerializer
      extends CollectionSerializer<Collection> {
    private final UnmodifiableFactory unmodifiableFactory;

    public UnmodifiableCollectionSerializer(
        Fury fury, Class<Collection> cls, UnmodifiableFactory unmodifiableFactory) {
      super(fury, cls, false, false);
      this.unmodifiableFactory = unmodifiableFactory;
    }

    @Override
    public void write(MemoryBuffer buffer, Collection value) {
      Preconditions.checkArgument(value.getClass() == type);
      Object fieldValue = Platform.getObject(value, unmodifiableFactory.sourceFieldOffset);
      fury.writeReferencableToJava(buffer, fieldValue);
    }

    @Override
    public Collection read(MemoryBuffer buffer) {
      final Object sourceCollection = fury.readReferencableFromJava(buffer);
      return (Collection) unmodifiableFactory.create(sourceCollection);
    }
  }

  public static final class UnmodifiableMapSerializer extends MapSerializers.MapSerializer<Map> {
    private final UnmodifiableFactory unmodifiableFactory;

    public UnmodifiableMapSerializer(
        Fury fury, Class<Map> cls, UnmodifiableFactory unmodifiableFactory) {
      super(fury, cls, false, false);
      this.unmodifiableFactory = unmodifiableFactory;
    }

    @Override
    public void write(MemoryBuffer buffer, Map value) {
      Preconditions.checkArgument(value.getClass() == type);
      Object fieldValue = Platform.getObject(value, unmodifiableFactory.sourceFieldOffset);
      fury.writeReferencableToJava(buffer, fieldValue);
    }

    @Override
    public Map read(MemoryBuffer buffer) {
      final Object sourceCollection = fury.readReferencableFromJava(buffer);
      return (Map) unmodifiableFactory.create(sourceCollection);
    }
  }

  enum UnmodifiableFactory {
    COLLECTION(
        Collections.unmodifiableCollection(Collections.singletonList("")).getClass(),
        SOURCE_COLLECTION_FIELD_OFFSET) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.unmodifiableCollection((Collection<?>) sourceCollection);
      }
    },
    RANDOM_ACCESS_LIST(
        Collections.unmodifiableList(new ArrayList<Void>()).getClass(),
        SOURCE_COLLECTION_FIELD_OFFSET) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.unmodifiableList((List<?>) sourceCollection);
      }
    },
    LIST(
        Collections.unmodifiableList(new LinkedList<Void>()).getClass(),
        SOURCE_COLLECTION_FIELD_OFFSET) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.unmodifiableList((List<?>) sourceCollection);
      }
    },
    SET(
        Collections.unmodifiableSet(new HashSet<Void>()).getClass(),
        SOURCE_COLLECTION_FIELD_OFFSET) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.unmodifiableSet((Set<?>) sourceCollection);
      }
    },
    SORTED_SET(
        Collections.unmodifiableSortedSet(new TreeSet<>()).getClass(),
        SOURCE_COLLECTION_FIELD_OFFSET) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.unmodifiableSortedSet((SortedSet<?>) sourceCollection);
      }
    },
    MAP(
        Collections.unmodifiableMap(new HashMap<Void, Void>()).getClass(),
        SOURCE_MAP_FIELD_OFFSET) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.unmodifiableMap((Map<?, ?>) sourceCollection);
      }
    },
    SORTED_MAP(
        Collections.unmodifiableSortedMap(new TreeMap<>()).getClass(), SOURCE_MAP_FIELD_OFFSET) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.unmodifiableSortedMap((SortedMap<?, ?>) sourceCollection);
      }
    };

    private final Class<?> type;
    private final long sourceFieldOffset;

    UnmodifiableFactory(Class<?> type, long sourceFieldOffset) {
      this.type = type;
      this.sourceFieldOffset = sourceFieldOffset;
    }

    public abstract Object create(Object sourceCollection);

    public boolean isCollection() {
      return Collection.class.isAssignableFrom(type);
    }

    static UnmodifiableFactory valueOfType(final Class<?> type) {
      for (final UnmodifiableFactory item : values()) {
        if (item.type == type) {
          return item;
        }
      }
      throw new IllegalArgumentException("The type " + type + " is not supported.");
    }
  }

  /**
   * Registers serializers for unmodifiable Collections created via {@link Collections}, including
   * {@link Map}s.
   *
   * @see Collections#unmodifiableCollection(Collection)
   * @see Collections#unmodifiableList(List)
   * @see Collections#unmodifiableSet(Set)
   * @see Collections#unmodifiableSortedSet(SortedSet)
   * @see Collections#unmodifiableMap(Map)
   * @see Collections#unmodifiableSortedMap(SortedMap)
   */
  @SuppressWarnings("unchecked")
  public static void registerSerializers(Fury fury) {
    if (SOURCE_COLLECTION_FIELD != null && SOURCE_MAP_FIELD != null) {
      for (UnmodifiableFactory factory : UnmodifiableFactory.values()) {
        if (factory.isCollection()) {
          fury.registerSerializer(
              factory.type,
              new UnmodifiableCollectionSerializer(
                  fury, (Class<Collection>) factory.type, factory));
        } else {
          fury.registerSerializer(
              factory.type,
              new UnmodifiableMapSerializer(fury, (Class<Map>) factory.type, factory));
        }
      }
    }
  }
}
