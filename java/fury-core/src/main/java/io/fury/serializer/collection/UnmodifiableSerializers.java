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
import io.fury.util.Preconditions;
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
import java.util.function.Function;
import org.slf4j.Logger;

/** Serializer for unmodifiable Collections and Maps created via Collections. */
@SuppressWarnings({"rawtypes", "unchecked"})
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
    private final Function factory;
    private final long offset;

    public UnmodifiableCollectionSerializer(Fury fury, Class cls, Function factory, long offset) {
      super(fury, cls, false);
      this.factory = factory;
      this.offset = offset;
    }

    @Override
    public void write(MemoryBuffer buffer, Collection value) {
      Preconditions.checkArgument(value.getClass() == type);
      Object fieldValue = Platform.getObject(value, offset);
      fury.writeRef(buffer, fieldValue);
    }

    @Override
    public Collection read(MemoryBuffer buffer) {
      final Object sourceCollection = fury.readRef(buffer);
      return (Collection) factory.apply(sourceCollection);
    }
  }

  public static final class UnmodifiableMapSerializer extends MapSerializer<Map> {
    private final Function factory;
    private final long offset;

    public UnmodifiableMapSerializer(Fury fury, Class cls, Function factory, long offset) {
      super(fury, cls, false);
      this.factory = factory;
      this.offset = offset;
    }

    @Override
    public void write(MemoryBuffer buffer, Map value) {
      Preconditions.checkArgument(value.getClass() == type);
      Object fieldValue = Platform.getObject(value, offset);
      fury.writeRef(buffer, fieldValue);
    }

    @Override
    public Map read(MemoryBuffer buffer) {
      final Object sourceCollection = fury.readRef(buffer);
      return (Map) factory.apply(sourceCollection);
    }
  }

  static Serializer createSerializer(Fury fury, Class<?> cls) {
    for (Tuple2<Class<?>, Function> factory : unmodifiableFactories()) {
      if (factory.f0 == cls) {
        return createSerializer(fury, factory);
      }
    }
    throw new IllegalArgumentException("Unsupported type " + cls);
  }

  private static Serializer<?> createSerializer(Fury fury, Tuple2<Class<?>, Function> factory) {
    if (Collection.class.isAssignableFrom(factory.f0)) {
      return new UnmodifiableCollectionSerializer(
          fury, factory.f0, factory.f1, SOURCE_COLLECTION_FIELD_OFFSET);
    } else {
      return new UnmodifiableMapSerializer(fury, factory.f0, factory.f1, SOURCE_MAP_FIELD_OFFSET);
    }
  }

  static Tuple2<Class<?>, Function>[] unmodifiableFactories() {
    Tuple2<Class<?>, Function> collectionFactory =
        Tuple2.of(
            Collections.unmodifiableCollection(Collections.singletonList("")).getClass(),
            o -> Collections.unmodifiableCollection((Collection) o));
    Tuple2<Class<?>, Function> randomAccessListFactory =
        Tuple2.of(
            Collections.unmodifiableList(new ArrayList<Void>()).getClass(),
            o -> Collections.unmodifiableList((List<?>) o));
    Tuple2<Class<?>, Function> listFactory =
        Tuple2.of(
            Collections.unmodifiableList(new LinkedList<Void>()).getClass(),
            o -> Collections.unmodifiableList((List<?>) o));
    Tuple2<Class<?>, Function> setFactory =
        Tuple2.of(
            Collections.unmodifiableSet(new HashSet<Void>()).getClass(),
            o -> Collections.unmodifiableSet((Set<?>) o));
    Tuple2<Class<?>, Function> sortedsetFactory =
        Tuple2.of(
            Collections.unmodifiableSortedSet(new TreeSet<>()).getClass(),
            o -> Collections.unmodifiableSortedSet((SortedSet<?>) o));
    Tuple2<Class<?>, Function> mapFactory =
        Tuple2.of(
            Collections.unmodifiableMap(new HashMap<>()).getClass(),
            o -> Collections.unmodifiableMap((Map) o));
    Tuple2<Class<?>, Function> sortedmapFactory =
        Tuple2.of(
            Collections.unmodifiableSortedMap(new TreeMap<>()).getClass(),
            o -> Collections.unmodifiableSortedMap((SortedMap) o));
    return new Tuple2[] {
      collectionFactory,
      randomAccessListFactory,
      listFactory,
      setFactory,
      sortedsetFactory,
      mapFactory,
      sortedmapFactory
    };
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
  public static void registerSerializers(Fury fury) {
    if (SOURCE_COLLECTION_FIELD != null && SOURCE_MAP_FIELD != null) {
      for (Tuple2<Class<?>, Function> factory : unmodifiableFactories()) {
        fury.registerSerializer(factory.f0, createSerializer(fury, factory));
      }
    }
  }
}
