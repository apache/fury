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

package io.fury.serializer.collection;

import io.fury.Fury;
import io.fury.collection.Tuple2;
import io.fury.memory.MemoryBuffer;
import io.fury.serializer.Serializer;
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
import java.util.function.Function;
import org.slf4j.Logger;

/** Serializer for synchronized Collections and Maps created via Collections. */
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

    private final Function factory;
    private final long offset;

    public SynchronizedCollectionSerializer(Fury fury, Class cls, Function factory, long offset) {
      super(fury, cls, false);
      this.factory = factory;
      this.offset = offset;
    }

    @Override
    public void write(MemoryBuffer buffer, Collection object) {
      // the ordinal could be replaced by s.th. else (e.g. a explicitly managed "id")
      Object unwrapped = Platform.getObject(object, offset);
      synchronized (object) {
        fury.writeRef(buffer, unwrapped);
      }
    }

    @Override
    public Collection read(MemoryBuffer buffer) {
      final Object sourceCollection = fury.readRef(buffer);
      return (Collection) factory.apply(sourceCollection);
    }
  }

  public static final class SynchronizedMapSerializer extends MapSerializer<Map> {
    private final Function factory;
    private final long offset;

    public SynchronizedMapSerializer(Fury fury, Class cls, Function factory, long offset) {
      super(fury, cls, false);
      this.factory = factory;
      this.offset = offset;
    }

    @Override
    public void write(MemoryBuffer buffer, Map object) {
      // the ordinal could be replaced by s.th. else (e.g. a explicitly managed "id")
      Object unwrapped = Platform.getObject(object, offset);
      synchronized (object) {
        fury.writeRef(buffer, unwrapped);
      }
    }

    @Override
    public Map read(MemoryBuffer buffer) {
      final Object sourceCollection = fury.readRef(buffer);
      return (Map) factory.apply(sourceCollection);
    }
  }

  static Serializer createSerializer(Fury fury, Class<?> cls) {
    for (Tuple2<Class<?>, Function> factory : synchronizedFactories()) {
      if (factory.f0 == cls) {
        return createSerializer(fury, factory);
      }
    }
    throw new IllegalArgumentException("Unsupported type " + cls);
  }

  private static Serializer<?> createSerializer(Fury fury, Tuple2<Class<?>, Function> factory) {
    if (Collection.class.isAssignableFrom(factory.f0)) {
      return new SynchronizedCollectionSerializer(
          fury, factory.f0, factory.f1, SOURCE_COLLECTION_FIELD_OFFSET);
    } else {
      return new SynchronizedMapSerializer(fury, factory.f0, factory.f1, SOURCE_MAP_FIELD_OFFSET);
    }
  }

  static Tuple2<Class<?>, Function>[] synchronizedFactories() {
    Tuple2<Class<?>, Function> collectionFactory =
        Tuple2.of(
            Collections.synchronizedCollection(Arrays.asList("")).getClass(),
            o -> Collections.synchronizedCollection((Collection) o));
    Tuple2<Class<?>, Function> randomListFactory =
        Tuple2.of(
            Collections.synchronizedList(new ArrayList<Void>()).getClass(),
            o -> Collections.synchronizedList((List<?>) o));
    Tuple2<Class<?>, Function> listFactory =
        Tuple2.of(
            Collections.synchronizedList(new LinkedList<Void>()).getClass(),
            o -> Collections.synchronizedList((List<?>) o));
    Tuple2<Class<?>, Function> setFactory =
        Tuple2.of(
            Collections.synchronizedSet(new HashSet<Void>()).getClass(),
            o -> Collections.synchronizedSet((Set<?>) o));
    Tuple2<Class<?>, Function> sortedsetFactory =
        Tuple2.of(
            Collections.synchronizedSortedSet(new TreeSet<>()).getClass(),
            o -> Collections.synchronizedSortedSet((TreeSet<?>) o));
    Tuple2<Class<?>, Function> mapFactory =
        Tuple2.of(
            Collections.synchronizedMap(new HashMap<Void, Void>()).getClass(),
            o -> Collections.synchronizedMap((Map) o));
    Tuple2<Class<?>, Function> sortedmapFactory =
        Tuple2.of(
            Collections.synchronizedSortedMap(new TreeMap<>()).getClass(),
            o -> Collections.synchronizedSortedMap((SortedMap) o));
    return new Tuple2[] {
      collectionFactory,
      randomListFactory,
      listFactory,
      setFactory,
      sortedsetFactory,
      mapFactory,
      sortedmapFactory
    };
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
      for (Tuple2<Class<?>, Function> factory : synchronizedFactories()) {
        fury.registerSerializer(factory.f0, createSerializer(fury, factory));
      }
    }
  }
}
