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

package io.fury.serializer;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.fury.Fury;
import io.fury.memory.MemoryBuffer;
import io.fury.serializer.CollectionSerializers.CollectionSerializer;
import io.fury.serializer.MapSerializers.MapSerializer;
import io.fury.type.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Serializers for common guava types.
 *
 * @author chaokunyang
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class GuavaSerializers {
  // TODO(chaokunyang) support jit optimization for guava types.

  abstract static class GuavaCollectionSerializer<T extends Collection>
      extends CollectionSerializer<T> {
    private ReplaceResolveSerializer serializer;

    public GuavaCollectionSerializer(Fury fury, Class<T> cls) {
      super(fury, cls, false, false);
      fury.getClassResolver().setSerializer(cls, this);
    }

    protected ReplaceResolveSerializer getOrCreateSerializer() {
      // reduce cost of fury creation, ReplaceResolveSerializer will jit-generate serializer.
      ReplaceResolveSerializer serializer = this.serializer;
      if (serializer == null) {
        this.serializer = serializer = new ReplaceResolveSerializer(fury, type);
      }
      return serializer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T read(MemoryBuffer buffer) {
      return (T) getOrCreateSerializer().read(buffer);
    }

    @Override
    public void write(MemoryBuffer buffer, T value) {
      getOrCreateSerializer().write(buffer, value);
    }

    @Override
    public T xread(MemoryBuffer buffer) {
      int size = buffer.readPositiveVarInt();
      List list = new ArrayList<>();
      xreadElements(fury, buffer, list, size);
      T immutableList = xnewInstance(list);
      fury.getRefResolver().reference(immutableList);
      return immutableList;
    }

    protected abstract T xnewInstance(Collection collection);
  }

  public static final class ImmutableListSerializer<T extends ImmutableList>
      extends GuavaCollectionSerializer<T> {
    public ImmutableListSerializer(Fury fury, Class<T> cls) {
      super(fury, cls);
      fury.getClassResolver().setSerializer(cls, this);
    }

    @Override
    public short getXtypeId() {
      return (short) -Type.LIST.getId();
    }

    @Override
    public T xread(MemoryBuffer buffer) {
      int size = buffer.readPositiveVarInt();
      List list = new ArrayList<>();
      xreadElements(fury, buffer, list, size);
      T immutableList = (T) ImmutableList.copyOf(list);
      fury.getRefResolver().reference(immutableList);
      return immutableList;
    }

    public T xnewInstance(Collection collection) {
      return (T) ImmutableList.copyOf(collection);
    }
  }

  public static final class ImmutableSetSerializer<T extends ImmutableSet>
      extends GuavaCollectionSerializer<T> {

    public ImmutableSetSerializer(Fury fury, Class<T> cls) {
      super(fury, cls);
    }

    @Override
    public short getXtypeId() {
      return (short) -Type.FURY_SET.getId();
    }

    @Override
    protected T xnewInstance(Collection collection) {
      return (T) ImmutableSet.copyOf(collection);
    }
  }

  abstract static class GuavaMapSerializer<T extends Map> extends MapSerializer<T> {
    private ReplaceResolveSerializer serializer;

    public GuavaMapSerializer(Fury fury, Class<T> cls) {
      super(fury, cls, false, false);
      fury.getClassResolver().setSerializer(cls, this);
    }

    protected ReplaceResolveSerializer getOrCreateSerializer() {
      // reduce cost of fury creation, ReplaceResolveSerializer will jit-generate serializer.
      ReplaceResolveSerializer serializer = this.serializer;
      if (serializer == null) {
        this.serializer = serializer = new ReplaceResolveSerializer(fury, type);
      }
      return serializer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T read(MemoryBuffer buffer) {
      return (T) getOrCreateSerializer().read(buffer);
    }

    @Override
    public void write(MemoryBuffer buffer, T value) {
      getOrCreateSerializer().write(buffer, value);
    }

    @Override
    public short getXtypeId() {
      return (short) -Type.MAP.getId();
    }

    @Override
    public T xread(MemoryBuffer buffer) {
      int size = buffer.readPositiveVarInt();
      Map map = new HashMap();
      xreadElements(fury, buffer, map, size);
      T immutableMap = xnewInstance(map);
      fury.getRefResolver().reference(immutableMap);
      return immutableMap;
    }

    protected abstract T xnewInstance(Map map);
  }

  public static final class ImmutableMapSerializer<T extends ImmutableMap>
      extends GuavaMapSerializer<T> {

    public ImmutableMapSerializer(Fury fury, Class<T> cls) {
      super(fury, cls);
      fury.getClassResolver().setSerializer(cls, this);
    }

    @Override
    protected T xnewInstance(Map map) {
      return (T) ImmutableMap.copyOf(map);
    }
  }

  public static final class ImmutableBiMapSerializer<T extends ImmutableBiMap>
      extends GuavaMapSerializer<T> {
    public ImmutableBiMapSerializer(Fury fury, Class<T> cls) {
      super(fury, cls);
      fury.getClassResolver().setSerializer(cls, this);
    }

    @Override
    protected T xnewInstance(Map map) {
      return (T) ImmutableBiMap.copyOf(map);
    }
  }

  // TODO guava serializers
  // guava/ArrayListMultimapSerializer - serializer for guava-libraries' ArrayListMultimap
  // guava/ArrayTableSerializer - serializer for guava-libraries' ArrayTable
  // guava/HashBasedTableSerializer - serializer for guava-libraries' HashBasedTable
  // guava/HashMultimapSerializer -- serializer for guava-libraries' HashMultimap
  // guava/ImmutableListSerializer - serializer for guava-libraries' ImmutableList
  // guava/ImmutableSetSerializer - serializer for guava-libraries' ImmutableSet
  // guava/ImmutableMapSerializer - serializer for guava-libraries' ImmutableMap
  // guava/ImmutableMultimapSerializer - serializer for guava-libraries' ImmutableMultimap
  // guava/ImmutableSortedSetSerializer - serializer for guava-libraries' ImmutableSortedSet
  // guava/ImmutableTableSerializer - serializer for guava-libraries' ImmutableTable
  // guava/LinkedHashMultimapSerializer - serializer for guava-libraries' LinkedHashMultimap
  // guava/LinkedListMultimapSerializer - serializer for guava-libraries' LinkedListMultimap
  // guava/ReverseListSerializer - serializer for guava-libraries' Lists.ReverseList / Lists.reverse
  // guava/TreeBasedTableSerializer - serializer for guava-libraries' TreeBasedTable
  // guava/TreeMultimapSerializer - serializer for guava-libraries' TreeMultimap
  // guava/UnmodifiableNavigableSetSerializer - serializer for guava-libraries'
  // UnmodifiableNavigableSet
  public static void registerDefaultSerializers(Fury fury) {
    // Note: Guava common types are not public API, don't register by `ImmutableXXX.of()`,
    // since different guava version may return different type objects, which make class
    // registration
    // inconsistent if peers load different version of guava.
    // For example: guava 20 return ImmutableBiMap for ImmutableMap.of(), but guava 27 return
    // ImmutableMap.
    String pkg = "com.google.common.collect";
    Class<?> cls = loadClass(pkg + ".RegularImmutableBiMap", ImmutableBiMap.of().getClass());
    fury.registerSerializer(cls, new ImmutableBiMapSerializer(fury, cls));
    cls = loadClass(pkg + ".SingletonImmutableBiMap", ImmutableBiMap.of(1, 2).getClass());
    fury.registerSerializer(cls, new ImmutableBiMapSerializer(fury, cls));
    cls = loadClass(pkg + ".RegularImmutableMap", ImmutableMap.of().getClass());
    fury.registerSerializer(cls, new ImmutableMapSerializer(fury, cls));
    cls = loadClass(pkg + ".RegularImmutableList", ImmutableList.of().getClass());
    fury.registerSerializer(cls, new ImmutableListSerializer(fury, cls));
    cls = loadClass(pkg + ".SingletonImmutableList", ImmutableList.of(1).getClass());
    fury.registerSerializer(cls, new ImmutableSetSerializer(fury, cls));
    cls = loadClass(pkg + ".RegularImmutableSet", ImmutableSet.of().getClass());
    fury.registerSerializer(cls, new ImmutableSetSerializer(fury, cls));
    cls = loadClass(pkg + ".SingletonImmutableSet", ImmutableSet.of(1).getClass());
    fury.registerSerializer(cls, new ImmutableSetSerializer(fury, cls));
    // TODO(chaokunyang) add immutable sorted set support.
    //  sorted set doesn't support xlang.
    // register(fury, loadClass(pkg + ".RegularImmutableSortedSet",
    // ImmutableSortedSet.of(1).getClass()));
    // register(fury, loadClass(pkg + ".ImmutableSortedMap", ImmutableSortedMap.of().getClass()));
  }

  static Class<?> loadClass(String className, Class<?> cache) {
    if (cache.getName().equals(className)) {
      return cache;
    } else {
      try {
        return Class.forName(className);
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
