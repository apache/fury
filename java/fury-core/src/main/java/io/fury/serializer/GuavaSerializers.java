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
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.UnmodifiableIterator;
import io.fury.Fury;
import io.fury.memory.MemoryBuffer;
import io.fury.resolver.ClassInfoCache;
import io.fury.serializer.CollectionSerializers.CollectionSerializer;
import io.fury.serializer.MapSerializers.MapSerializer;
import io.fury.type.Type;
import io.fury.util.Platform;
import io.fury.util.ReflectionUtils;
import io.fury.util.unsafe._JDKAccess;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

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
    public void write(MemoryBuffer buffer, T value) {
      buffer.writePositiveVarInt(value.size());
      for (Object o : value) {
        fury.writeRef(buffer, o, elementClassInfoWriteCache);
      }
    }

    @Override
    public T read(MemoryBuffer buffer) {
      int size = buffer.readPositiveVarInt();
      Object[] array = new Object[size];
      for (int i = 0; i < size; i++) {
        array[i] = fury.readRef(buffer, elementClassInfoReadCache);
      }
      return (T) ImmutableList.copyOf(array);
    }

    @Override
    public short getXtypeId() {
      return (short) -Type.LIST.getId();
    }

    public T xnewInstance(Collection collection) {
      return (T) ImmutableList.copyOf(collection);
    }
  }

  private static final String pkg = "com.google.common.collect";
  private static volatile Object[] regularImmutableListInvokeCache;

  private static Object[] regularImmutableListInvoke() {
    Object[] regularImmutableListInvoke = regularImmutableListInvokeCache;
    if (regularImmutableListInvoke == null) {
      regularImmutableListInvoke = new Object[3];
      Class<?> cls = loadClass(pkg + ".RegularImmutableList", ImmutableList.of(1, 2).getClass());
      regularImmutableListInvoke[0] = cls;
      regularImmutableListInvoke[1] = ReflectionUtils.getFieldOffset(cls, "array");
      MethodHandles.Lookup lookup = _JDKAccess._trustedLookup(cls);
      try {
        MethodHandle ctr =
            lookup.findConstructor(cls, MethodType.methodType(void.class, Object[].class));
        Function func = _JDKAccess.makeJDKFunction(lookup, ctr);
        regularImmutableListInvoke[2] = func;
      } catch (NoSuchMethodException | IllegalAccessException e) {
        Platform.throwException(e);
      }
      regularImmutableListInvokeCache = regularImmutableListInvoke;
    }
    return regularImmutableListInvoke;
  }

  public static final class RegularImmutableListSerializer<T extends ImmutableList>
      extends GuavaCollectionSerializer<T> {
    private final long offset;
    private final Function<Object[], ImmutableList> function;
    private final ClassInfoCache classInfoCache;

    public RegularImmutableListSerializer(Fury fury, Class<T> cls) {
      super(fury, cls);
      Object[] cache = regularImmutableListInvoke();
      offset = (long) cache[1];
      function = (Function<Object[], ImmutableList>) cache[2];
      classInfoCache = fury.getClassResolver().nilClassInfoCache();
    }

    @Override
    public void write(MemoryBuffer buffer, T value) {
      Object[] array = (Object[]) Platform.getObject(value, offset);
      buffer.writePositiveVarInt(array.length);
      for (Object o : array) {
        fury.writeRef(buffer, o, classInfoCache);
      }
    }

    @Override
    public T read(MemoryBuffer buffer) {
      int len = buffer.readPositiveVarInt();
      Object[] array = new Object[len];
      for (int i = 0; i < len; i++) {
        array[i] = fury.readRef(buffer, classInfoCache);
      }
      return (T) function.apply(array);
    }

    @Override
    public short getXtypeId() {
      return (short) -Type.LIST.getId();
    }

    @Override
    protected T xnewInstance(Collection collection) {
      return (T) ImmutableList.copyOf(collection);
    }
  }

  public static final class ImmutableSetSerializer<T extends ImmutableSet>
      extends GuavaCollectionSerializer<T> {

    public ImmutableSetSerializer(Fury fury, Class<T> cls) {
      super(fury, cls);
    }

    @Override
    public void write(MemoryBuffer buffer, T value) {
      buffer.writePositiveVarInt(value.size());
      for (Object o : value) {
        fury.writeRef(buffer, o, elementClassInfoWriteCache);
      }
    }

    @Override
    public T read(MemoryBuffer buffer) {
      int size = buffer.readPositiveVarInt();
      Object[] array = new Object[size];
      for (int i = 0; i < size; i++) {
        array[i] = fury.readRef(buffer, elementClassInfoReadCache);
      }
      return (T) ImmutableSet.copyOf(array);
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

  public static final class ImmutableSortedSetSerializer<T extends ImmutableSortedSet>
      extends CollectionSerializer<T> {

    public ImmutableSortedSetSerializer(Fury fury, Class<T> cls) {
      super(fury, cls, false, false);
      fury.getClassResolver().setSerializer(cls, this);
    }

    @Override
    public void write(MemoryBuffer buffer, T value) {
      fury.writeRef(buffer, value.comparator());
      buffer.writePositiveVarInt(value.size());
      for (Object o : value) {
        fury.writeRef(buffer, o, elementClassInfoWriteCache);
      }
    }

    @Override
    public T read(MemoryBuffer buffer) {
      Comparator comparator = (Comparator) fury.readRef(buffer);
      int size = buffer.readPositiveVarInt();
      Object[] array = new Object[size];
      for (int i = 0; i < size; i++) {
        array[i] = fury.readRef(buffer, elementClassInfoReadCache);
      }
      return (T) new ImmutableSortedSet.Builder<>(comparator).add(array).build();
    }
  }

  abstract static class GuavaMapSerializer<T extends Map> extends MapSerializer<T> {

    public GuavaMapSerializer(Fury fury, Class<T> cls) {
      super(fury, cls, false, false);
      fury.getClassResolver().setSerializer(cls, this);
    }

    @Override
    public void write(MemoryBuffer buffer, T value) {
      // reuse classInfoCache for key/value
      buffer.writePositiveVarInt(value.size());
      Fury fury = this.fury;
      ClassInfoCache keyClassInfoWriteCache = this.keyClassInfoWriteCache;
      ClassInfoCache valueClassInfoWriteCache = this.valueClassInfoWriteCache;
      for (Object o : value.entrySet()) {
        Map.Entry entry = (Map.Entry) o;
        fury.writeRef(buffer, entry.getKey(), keyClassInfoWriteCache);
        fury.writeRef(buffer, entry.getValue(), valueClassInfoWriteCache);
      }
    }

    protected abstract ImmutableMap.Builder makeBuilder(int size);

    @Override
    public T read(MemoryBuffer buffer) {
      int size = buffer.readPositiveVarInt();
      ImmutableMap.Builder builder = makeBuilder(size);
      Fury fury = this.fury;
      ClassInfoCache keyClassInfoReadCache = this.keyClassInfoReadCache;
      ClassInfoCache valueClassInfoReadCache = this.valueClassInfoReadCache;
      for (int i = 0; i < size; i++) {
        // reuse classInfoCache for key/value
        Object key = fury.readRef(buffer, keyClassInfoReadCache);
        Object value = fury.readRef(buffer, valueClassInfoReadCache);
        builder.put(key, value);
      }
      return (T) builder.buildOrThrow();
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

    protected static Function builderCtr(Class<?> builderClass) {
      MethodHandles.Lookup lookup = _JDKAccess._trustedLookup(builderClass);
      MethodHandle ctr = null;
      try {
        ctr = lookup.findConstructor(builderClass, MethodType.methodType(void.class, int.class));
      } catch (NoSuchMethodException | IllegalAccessException e) {
        Platform.throwException(e);
      }
      return _JDKAccess.makeJDKFunction(lookup, ctr);
    }
  }

  public static final class ImmutableMapSerializer<T extends ImmutableMap>
      extends GuavaMapSerializer<T> {

    private final Function<Integer, ImmutableMap.Builder> builderCtr;

    public ImmutableMapSerializer(Fury fury, Class<T> cls) {
      super(fury, cls);
      builderCtr = builderCtr(ImmutableMap.Builder.class);
      fury.getClassResolver().setSerializer(cls, this);
    }

    @Override
    protected ImmutableMap.Builder makeBuilder(int size) {
      return builderCtr.apply(size);
    }

    @Override
    protected T xnewInstance(Map map) {
      return (T) ImmutableMap.copyOf(map);
    }
  }

  public static final class ImmutableBiMapSerializer<T extends ImmutableBiMap>
      extends GuavaMapSerializer<T> {
    private final Function<Integer, ImmutableBiMap.Builder> builderCtr;

    public ImmutableBiMapSerializer(Fury fury, Class<T> cls) {
      super(fury, cls);
      builderCtr = builderCtr(ImmutableBiMap.Builder.class);
      fury.getClassResolver().setSerializer(cls, this);
    }

    @Override
    protected ImmutableMap.Builder makeBuilder(int size) {
      return builderCtr.apply(size);
    }

    @Override
    protected T xnewInstance(Map map) {
      return (T) ImmutableBiMap.copyOf(map);
    }
  }

  public static final class ImmutableSortedMapSerializer<T extends ImmutableSortedMap>
      extends MapSerializer<T> {

    public ImmutableSortedMapSerializer(Fury fury, Class<T> cls) {
      super(fury, cls, false, false);
      fury.getClassResolver().setSerializer(cls, this);
    }

    @Override
    public void write(MemoryBuffer buffer, T value) {
      fury.writeRef(buffer, value.comparator());
      fury.writeRef(buffer, value.keySet(), keyClassInfoWriteCache);
      fury.writeRef(buffer, value.values(), valueClassInfoWriteCache);
    }

    @Override
    public T read(MemoryBuffer buffer) {
      Comparator comparator = (Comparator) fury.readRef(buffer);
      // reuse classInfoCache for key/value
      ImmutableSet keySet = (ImmutableSet) fury.readRef(buffer, keyClassInfoReadCache);
      ImmutableCollection values =
          (ImmutableCollection) fury.readRef(buffer, valueClassInfoReadCache);
      ImmutableMap.Builder builder = new ImmutableSortedMap.Builder(comparator);
      UnmodifiableIterator keyIter = keySet.iterator();
      UnmodifiableIterator valueIter = values.iterator();
      while (keyIter.hasNext()) {
        builder.put(keyIter.next(), valueIter.next());
      }
      return (T) builder.buildOrThrow();
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
    Class cls = loadClass(pkg + ".RegularImmutableBiMap", ImmutableBiMap.of().getClass());
    fury.registerSerializer(cls, new ImmutableBiMapSerializer(fury, cls));
    cls = loadClass(pkg + ".SingletonImmutableBiMap", ImmutableBiMap.of(1, 2).getClass());
    fury.registerSerializer(cls, new ImmutableBiMapSerializer(fury, cls));
    cls = loadClass(pkg + ".RegularImmutableMap", ImmutableMap.of().getClass());
    fury.registerSerializer(cls, new ImmutableMapSerializer(fury, cls));
    cls = loadClass(pkg + ".RegularImmutableList", ImmutableList.of().getClass());
    fury.registerSerializer(cls, new RegularImmutableListSerializer(fury, cls));
    cls = loadClass(pkg + ".SingletonImmutableList", ImmutableList.of(1).getClass());
    fury.registerSerializer(cls, new ImmutableListSerializer(fury, cls));
    cls = loadClass(pkg + ".RegularImmutableSet", ImmutableSet.of().getClass());
    fury.registerSerializer(cls, new ImmutableSetSerializer(fury, cls));
    cls = loadClass(pkg + ".SingletonImmutableSet", ImmutableSet.of(1).getClass());
    fury.registerSerializer(cls, new ImmutableSetSerializer(fury, cls));
    // sorted set/map doesn't support xlang.
    cls = loadClass(pkg + ".RegularImmutableSortedSet", ImmutableSortedSet.of(1, 2).getClass());
    fury.registerSerializer(cls, new ImmutableSortedSetSerializer<>(fury, cls));
    cls = loadClass(pkg + ".ImmutableSortedMap", ImmutableSortedMap.of(1, 2).getClass());
    fury.registerSerializer(cls, new ImmutableSortedMapSerializer<>(fury, cls));
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
