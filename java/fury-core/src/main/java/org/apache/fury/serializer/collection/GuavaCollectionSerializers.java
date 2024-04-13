/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fury.serializer.collection;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
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
import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.type.Type;
import org.apache.fury.util.Platform;
import org.apache.fury.util.unsafe._JDKAccess;

/** Serializers for common guava types. */
@SuppressWarnings({"unchecked", "rawtypes"})
public class GuavaCollectionSerializers {
  abstract static class GuavaCollectionSerializer<T extends Collection>
      extends CollectionSerializer<T> {
    public GuavaCollectionSerializer(Fury fury, Class<T> cls) {
      super(fury, cls, true);
      fury.getClassResolver().setSerializer(cls, this);
    }

    @Override
    public T xread(MemoryBuffer buffer) {
      int size = buffer.readVarUint32Small7();
      List list = new ArrayList<>();
      xreadElements(fury, buffer, list, size);
      return xnewInstance(list);
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
    public Collection newCollection(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      return new CollectionContainer<>(numElements);
    }

    @Override
    public T onCollectionRead(Collection collection) {
      Object[] elements = ((CollectionContainer) collection).elements;
      ImmutableList list = ImmutableList.copyOf(elements);
      return (T) list;
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
  private static Function regularImmutableListInvokeCache;

  private static synchronized Function regularImmutableListInvoke() {
    if (regularImmutableListInvokeCache == null) {
      Class<?> cls = loadClass(pkg + ".RegularImmutableList", ImmutableList.of(1, 2).getClass());
      MethodHandles.Lookup lookup = _JDKAccess._trustedLookup(cls);
      MethodHandle ctr = null;
      try {
        ctr = lookup.findConstructor(cls, MethodType.methodType(void.class, Object[].class));
      } catch (NoSuchMethodException | IllegalAccessException e) {
        Platform.throwException(e);
      }
      regularImmutableListInvokeCache = _JDKAccess.makeJDKFunction(lookup, ctr);
    }
    return regularImmutableListInvokeCache;
  }

  public static final class RegularImmutableListSerializer<T extends ImmutableList>
      extends GuavaCollectionSerializer<T> {
    private final Function<Object[], ImmutableList> function;

    public RegularImmutableListSerializer(Fury fury, Class<T> cls) {
      super(fury, cls);
      function = (Function<Object[], ImmutableList>) regularImmutableListInvoke();
    }

    @Override
    public Collection newCollection(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      return new CollectionContainer(numElements);
    }

    @Override
    public T onCollectionRead(Collection collection) {
      Object[] elements = ((CollectionContainer) collection).elements;
      return (T) function.apply(elements);
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
    public Collection newCollection(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      return new CollectionContainer<>(numElements);
    }

    @Override
    public T onCollectionRead(Collection collection) {
      Object[] elements = ((CollectionContainer) collection).elements;
      return (T) ImmutableSet.copyOf(elements);
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
      super(fury, cls, false);
      fury.getClassResolver().setSerializer(cls, this);
    }

    @Override
    public Collection onCollectionWrite(MemoryBuffer buffer, T value) {
      buffer.writeVarUint32Small7(value.size());
      fury.writeRef(buffer, value.comparator());
      return value;
    }

    @Override
    public Collection newCollection(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      Comparator comparator = (Comparator) fury.readRef(buffer);
      return new SortedCollectionContainer(comparator, numElements);
    }

    @Override
    public T onCollectionRead(Collection collection) {
      SortedCollectionContainer data = (SortedCollectionContainer) collection;
      Object[] elements = data.elements;
      return (T) new ImmutableSortedSet.Builder<>(data.comparator).add(elements).build();
    }
  }

  abstract static class GuavaMapSerializer<T extends Map> extends MapSerializer<T> {

    public GuavaMapSerializer(Fury fury, Class<T> cls) {
      super(fury, cls, true);
      fury.getClassResolver().setSerializer(cls, this);
    }

    protected abstract ImmutableMap.Builder makeBuilder(int size);

    @Override
    public Map newMap(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      return new MapContainer(numElements);
    }

    @Override
    public T onMapRead(Map map) {
      MapContainer container = (MapContainer) map;
      int size = container.size;
      ImmutableMap.Builder builder = makeBuilder(size);
      Object[] keyArray = container.keyArray;
      Object[] valueArray = container.valueArray;
      for (int i = 0; i < size; i++) {
        builder.put(keyArray[i], valueArray[i]);
      }
      return (T) builder.build();
    }

    @Override
    public short getXtypeId() {
      return (short) -Type.MAP.getId();
    }

    @Override
    public T xread(MemoryBuffer buffer) {
      int size = buffer.readVarUint32Small7();
      Map map = new HashMap();
      xreadElements(fury, buffer, map, size);
      return xnewInstance(map);
    }

    protected abstract T xnewInstance(Map map);
  }

  private static final ClassValue<Function> builderCtrCache =
      new ClassValue<Function>() {
        @Override
        protected Function computeValue(Class<?> builderClass) {
          MethodHandles.Lookup lookup = _JDKAccess._trustedLookup(builderClass);
          MethodHandle ctr = null;
          try {
            ctr =
                lookup.findConstructor(builderClass, MethodType.methodType(void.class, int.class));
          } catch (NoSuchMethodException | IllegalAccessException e) {
            Platform.throwException(e);
          }
          return _JDKAccess.makeJDKFunction(lookup, ctr);
        }
      };

  public static final class ImmutableMapSerializer<T extends ImmutableMap>
      extends GuavaMapSerializer<T> {

    private final Function<Integer, ImmutableMap.Builder> builderCtr;

    public ImmutableMapSerializer(Fury fury, Class<T> cls) {
      super(fury, cls);
      builderCtr = builderCtrCache.get(ImmutableMap.Builder.class);
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
      builderCtr = builderCtrCache.get(ImmutableBiMap.Builder.class);
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
      super(fury, cls);
      fury.getClassResolver().setSerializer(cls, this);
    }

    @Override
    public Map onMapWrite(MemoryBuffer buffer, T value) {
      buffer.writeVarUint32Small7(value.size());
      fury.writeRef(buffer, value.comparator());
      return value;
    }

    @Override
    public Map newMap(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      Comparator comparator = (Comparator) fury.readRef(buffer);
      return new SortedMapContainer<>(comparator, numElements);
    }

    @Override
    public T onMapRead(Map map) {
      SortedMapContainer mapContainer = (SortedMapContainer) map;
      ImmutableMap.Builder builder = new ImmutableSortedMap.Builder(mapContainer.comparator);
      int size = mapContainer.size;
      Object[] keyArray = mapContainer.keyArray;
      Object[] valueArray = mapContainer.valueArray;
      for (int i = 0; i < size; i++) {
        builder.put(keyArray[i], valueArray[i]);
      }
      return (T) builder.build();
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
