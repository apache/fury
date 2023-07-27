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

package io.fury.serializer;

import io.fury.Fury;
import io.fury.exception.FuryException;
import io.fury.memory.MemoryBuffer;
import io.fury.serializer.CollectionSerializers.CollectionSerializer;
import io.fury.serializer.MapSerializers.MapSerializer;
import io.fury.util.LoggerFactory;
import io.fury.util.Platform;
import io.fury.util.ReflectionUtils;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
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
import java.util.stream.Collectors;
import org.slf4j.Logger;

/** Serializer for synchronized Collections and Maps created via Collections. */
// modified from
// https://github.com/magro/kryo-serializers/blob/master/src/main/java/de/javakaffee/kryoserializers/SynchronizedCollectionSerializer.java
// but faster by using unsafe instead of reflection.
@SuppressWarnings({"rawtypes", "unchecked"})
public class SynchronizedSerializers {
  private static final Logger LOG = LoggerFactory.getLogger(SynchronizedSerializers.class);
  private static Field SOURCE_COLLECTION_FIELD;
  private static Field SOURCE_MAP_FIELD;
  private static final long SOURCE_COLLECTION_FIELD_OFFSET;
  private static final long SOURCE_MAP_FIELD_OFFSET;

  static {
    try {
      // SynchronizedList/Set/Etc.. extends SynchronizedCollection
      SOURCE_COLLECTION_FIELD =
          Collections.synchronizedCollection(Collections.emptyList())
              .getClass()
              .getDeclaredField("c");
      // SynchronizedSortedMap/SynchronizedNavigableMap extends SynchronizedMap
      SOURCE_MAP_FIELD =
          Collections.synchronizedMap(Collections.emptyMap()).getClass().getDeclaredField("m");
    } catch (Exception e) {
      LOG.warn(
          "Could not access source collection field in "
              + "java.util.Collections$SynchronizedCollection/SynchronizedMap {}.",
          e.toString());
    }
    SOURCE_COLLECTION_FIELD_OFFSET = ReflectionUtils.getFieldOffset(SOURCE_COLLECTION_FIELD);
    SOURCE_MAP_FIELD_OFFSET = ReflectionUtils.getFieldOffset(SOURCE_MAP_FIELD);
  }

  public static final class SynchronizedCollectionSerializer
      extends CollectionSerializer<Collection> {
    private final SynchronizedFactory synchronizedFactory;

    public SynchronizedCollectionSerializer(
        Fury fury, Class cls, SynchronizedFactory synchronizedFactory) {
      super(fury, cls, false, false);
      this.synchronizedFactory = synchronizedFactory;
    }

    @Override
    public void write(MemoryBuffer buffer, Collection object) {
      // the ordinal could be replaced by s.th. else (e.g. a explicitly managed "id")
      Object unwrapped = Platform.getObject(object, synchronizedFactory.sourceFieldOffset);
      fury.writeRef(buffer, unwrapped);
    }

    @Override
    public Collection read(MemoryBuffer buffer) {
      final Object sourceCollection = fury.readRef(buffer);
      return (Collection) synchronizedFactory.create(sourceCollection);
    }
  }

  public static final class SynchronizedMapSerializer extends MapSerializer<Map> {
    private final SynchronizedFactory synchronizedFactory;

    public SynchronizedMapSerializer(
        Fury fury, Class cls, SynchronizedFactory synchronizedFactory) {
      super(fury, cls, false, false);
      this.synchronizedFactory = synchronizedFactory;
    }

    @Override
    public void write(MemoryBuffer buffer, Map object) {
      // the ordinal could be replaced by s.th. else (e.g. a explicitly managed "id")
      Object unwrapped = Platform.getObject(object, synchronizedFactory.sourceFieldOffset);
      fury.writeRef(buffer, unwrapped);
    }

    @Override
    public Map read(MemoryBuffer buffer) {
      final Object sourceCollection = fury.readRef(buffer);
      return (Map) synchronizedFactory.create(sourceCollection);
    }
  }

  enum SynchronizedFactory {
    COLLECTION(
        Collections.synchronizedCollection(Arrays.asList("")).getClass(),
        SOURCE_COLLECTION_FIELD_OFFSET) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.synchronizedCollection((Collection<?>) sourceCollection);
      }
    },
    RANDOM_ACCESS_LIST(
        Collections.synchronizedList(new ArrayList<Void>()).getClass(),
        SOURCE_COLLECTION_FIELD_OFFSET) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.synchronizedList((List<?>) sourceCollection);
      }
    },
    LIST(
        Collections.synchronizedList(new LinkedList<Void>()).getClass(),
        SOURCE_COLLECTION_FIELD_OFFSET) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.synchronizedList((List<?>) sourceCollection);
      }
    },
    SET(
        Collections.synchronizedSet(new HashSet<Void>()).getClass(),
        SOURCE_COLLECTION_FIELD_OFFSET) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.synchronizedSet((Set<?>) sourceCollection);
      }
    },
    SORTED_SET(
        Collections.synchronizedSortedSet(new TreeSet<>()).getClass(),
        SOURCE_COLLECTION_FIELD_OFFSET) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.synchronizedSortedSet((SortedSet<?>) sourceCollection);
      }
    },
    MAP(
        Collections.synchronizedMap(new HashMap<Void, Void>()).getClass(),
        SOURCE_MAP_FIELD_OFFSET) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.synchronizedMap((Map<?, ?>) sourceCollection);
      }
    },
    SORTED_MAP(
        Collections.synchronizedSortedMap(new TreeMap<>()).getClass(), SOURCE_MAP_FIELD_OFFSET) {
      @Override
      public Object create(final Object sourceCollection) {
        return Collections.synchronizedSortedMap((SortedMap<?, ?>) sourceCollection);
      }
    };

    private final Class<?> type;
    private final long sourceFieldOffset;

    SynchronizedFactory(final Class<?> type, long sourceFieldOffset) {
      this.type = type;
      this.sourceFieldOffset = sourceFieldOffset;
    }

    public boolean isCollection() {
      return Collection.class.isAssignableFrom(type);
    }

    public abstract Object create(Object sourceCollection);

    static SynchronizedFactory valueOfType(final Class<?> type) {
      for (final SynchronizedFactory item : values()) {
        if (item.type.equals(type)) {
          return item;
        }
      }
      throw new IllegalArgumentException("The type " + type + " is not supported.");
    }
  }

  /**
   * Registering serializers for synchronized Collections and Maps created via {@link Collections}.
   *
   * @see Collections#synchronizedCollection(Collection)
   * @see Collections#synchronizedList(List)
   * @see Collections#synchronizedSet(Set)
   * @see Collections#synchronizedSortedSet(SortedSet)
   * @see Collections#synchronizedMap(Map)
   * @see Collections#synchronizedSortedMap(SortedMap)
   */
  public static void registerSerializers(Fury fury) {
    if (SOURCE_COLLECTION_FIELD != null && SOURCE_MAP_FIELD != null) {
      Set<? extends Class<?>> classSet =
          Arrays.stream(SynchronizedFactory.values()).map(c -> c.type).collect(Collectors.toSet());
      if (classSet.size() != SynchronizedFactory.values().length) {
        throw new FuryException(
            String.format(
                "Enum types %s duplicate.", Arrays.toString(SynchronizedFactory.values())));
      }
      for (SynchronizedFactory factory : SynchronizedFactory.values()) {
        if (factory.isCollection()) {
          fury.registerSerializer(
              factory.type, new SynchronizedCollectionSerializer(fury, factory.type, factory));
        } else {
          fury.registerSerializer(
              factory.type, new SynchronizedMapSerializer(fury, factory.type, factory));
        }
      }
    }
  }
}
