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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import io.fury.Fury;
import io.fury.config.CompatibleMode;
import io.fury.memory.MemoryBuffer;
import io.fury.resolver.ClassResolver;
import io.fury.resolver.FieldResolver;
import io.fury.util.ReflectionUtils;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Serializers for subclasses of common JDK container types. Subclasses of {@link ArrayList}/{@link
 * HashMap}/{@link LinkedHashMap}/{@link java.util.TreeMap}/etc have `writeObject`/`readObject`
 * defined, which will use JDK compatible serializers, thus inefficient. Serializers will optimize
 * the serialization for those cases by serializing super classes part separately using existing
 * JIT/interpreter-mode serializers.
 *
 * @author chaokunyang
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class ChildContainerSerializers {

  public static Class<? extends Serializer> getCollectionSerializerClass(Class<?> cls) {
    if (ChildCollectionSerializer.superClasses.contains(cls)) {
      return null;
    }
    if (ClassResolver.useReplaceResolveSerializer(cls)) {
      return null;
    }
    if (!ReflectionUtils.hasPublicNoArgConstructor(cls)) {
      return null;
    }
    while (cls != Object.class) {
      if (ChildCollectionSerializer.superClasses.contains(cls)) {
        if (cls == ArrayList.class) {
          return ChildArrayListSerializer.class;
        } else {
          return ChildCollectionSerializer.class;
        }
      } else {
        if (JavaSerializer.getReadObjectMethod(cls, false) != null
            || JavaSerializer.getWriteObjectMethod(cls, false) != null) {
          return null;
        }
      }
      cls = cls.getSuperclass();
    }
    return null;
  }

  public static Class<? extends Serializer> getMapSerializerClass(Class<?> cls) {
    if (ChildMapSerializer.superClasses.contains(cls)) {
      return null;
    }
    if (ClassResolver.useReplaceResolveSerializer(cls)) {
      return null;
    }
    if (!ReflectionUtils.hasPublicNoArgConstructor(cls)) {
      return null;
    }
    if (!ReflectionUtils.hasNoArgConstructor(cls)) {
      return null;
    }
    while (cls != Object.class) {
      if (ChildMapSerializer.superClasses.contains(cls)) {
        return ChildMapSerializer.class;
      } else {
        if (JavaSerializer.getReadObjectMethod(cls, false) != null
            || JavaSerializer.getWriteObjectMethod(cls, false) != null) {
          return null;
        }
      }
      cls = cls.getSuperclass();
    }
    return null;
  }

  /**
   * Serializer for subclasses of {@link ChildCollectionSerializer#superClasses} if no jdk custom
   * serialization in those classes.
   */
  public static class ChildCollectionSerializer<T extends Collection>
      extends CollectionSerializers.CollectionSerializer<T> {
    public static Set<Class<?>> superClasses =
        ImmutableSet.of(
            ArrayList.class, LinkedList.class, ArrayDeque.class, Vector.class, HashSet.class
            // PriorityQueue/TreeSet/ConcurrentSkipListSet need comparator as constructor argument
            );

    protected final Serializer[] slotsSerializers;

    public ChildCollectionSerializer(Fury fury, Class<T> cls) {
      super(fury, cls);
      slotsSerializers = buildSlotsSerializers(fury, superClasses, cls);
    }

    @Override
    public void writeHeader(MemoryBuffer buffer, T value) {
      for (Serializer slotsSerializer : slotsSerializers) {
        slotsSerializer.write(buffer, value);
      }
    }

    public Collection newCollection(MemoryBuffer buffer, int numElements) {
      Collection collection = super.newCollection(buffer, numElements);
      readAndSetFields(buffer, collection, slotsSerializers);
      return collection;
    }
  }

  public static final class ChildArrayListSerializer<T extends ArrayList>
      extends ChildCollectionSerializer<T> {
    public ChildArrayListSerializer(Fury fury, Class<T> cls) {
      super(fury, cls);
    }

    @Override
    public T newCollection(MemoryBuffer buffer, int numElements) {
      T collection = (T) super.newCollection(buffer, numElements);
      collection.ensureCapacity(numElements);
      return collection;
    }
  }

  /**
   * Serializer for subclasses of {@link ChildMapSerializer#superClasses} if no jdk custom
   * serialization in those classes.
   */
  public static class ChildMapSerializer<T extends Map> extends MapSerializers.MapSerializer<T> {
    public static Set<Class<?>> superClasses =
        ImmutableSet.of(
            HashMap.class, LinkedHashMap.class, ConcurrentHashMap.class
            // TreeMap/ConcurrentSkipListMap need comparator as constructor argument
            );
    private final Serializer[] slotsSerializers;

    public ChildMapSerializer(Fury fury, Class<T> cls) {
      super(fury, cls);
      slotsSerializers = buildSlotsSerializers(fury, superClasses, cls);
    }

    @Override
    public void writeHeader(MemoryBuffer buffer, T value) {
      for (Serializer slotsSerializer : slotsSerializers) {
        slotsSerializer.write(buffer, value);
      }
    }

    @Override
    public Map newMap(MemoryBuffer buffer, int numElements) {
      Map map = super.newMap(buffer, numElements);
      readAndSetFields(buffer, map, slotsSerializers);
      return map;
    }
  }

  private static <T> Serializer[] buildSlotsSerializers(
      Fury fury, Set<Class<?>> superClasses, Class<T> cls) {
    Preconditions.checkArgument(!superClasses.contains(cls));
    List<Serializer> serializers = new ArrayList<>();
    while (!superClasses.contains(cls)) {
      Serializer slotsSerializer;
      if (fury.getConfig().getCompatibleMode() == CompatibleMode.COMPATIBLE) {
        slotsSerializer =
            new CompatibleSerializer(fury, cls, FieldResolver.of(fury, cls, false, false));
      } else {
        slotsSerializer = new ObjectSerializer<>(fury, cls, false);
      }
      serializers.add(slotsSerializer);
      cls = (Class<T>) cls.getSuperclass();
    }
    Collections.reverse(serializers);
    return serializers.toArray(new Serializer[0]);
  }

  private static void readAndSetFields(
      MemoryBuffer buffer, Object collection, Serializer[] slotsSerializers) {
    for (Serializer slotsSerializer : slotsSerializers) {
      if (slotsSerializer.getClass() == CompatibleSerializer.class) {
        ((CompatibleSerializer) slotsSerializer).readAndSetFields(buffer, collection);
      } else {
        ((ObjectSerializer) slotsSerializer).readAndSetFields(buffer, collection);
      }
    }
  }
}
