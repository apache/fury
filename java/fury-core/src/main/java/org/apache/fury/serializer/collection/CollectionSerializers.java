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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.fury.Fury;
import org.apache.fury.config.Language;
import org.apache.fury.exception.FuryException;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.Platform;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.resolver.ClassInfo;
import org.apache.fury.resolver.ClassInfoHolder;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.RefResolver;
import org.apache.fury.serializer.ReplaceResolveSerializer;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.serializer.Serializers;
import org.apache.fury.util.Preconditions;
import org.apache.fury.util.unsafe._JDKAccess;

/**
 * Serializers for classes implements {@link Collection}. All collection serializers should extend
 * {@link CollectionSerializer}.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class CollectionSerializers {

  public static final class ArrayListSerializer extends CollectionSerializer<ArrayList> {
    public ArrayListSerializer(Fury fury) {
      super(fury, ArrayList.class, true);
    }

    @Override
    public ArrayList newCollection(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      ArrayList arrayList = new ArrayList(numElements);
      fury.getRefResolver().reference(arrayList);
      return arrayList;
    }
  }

  public static final class ArraysAsListSerializer extends CollectionSerializer<List<?>> {
    // Make offset compatible with graalvm native image.
    private static final long arrayFieldOffset;

    static {
      try {
        Field arrayField = Class.forName("java.util.Arrays$ArrayList").getDeclaredField("a");
        arrayFieldOffset = Platform.objectFieldOffset(arrayField);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    public ArraysAsListSerializer(Fury fury, Class<List<?>> cls) {
      super(fury, cls, fury.isCrossLanguage());
    }

    @Override
    public List<?> copy(List<?> originCollection) {
      Object[] elements = new Object[originCollection.size()];
      List<?> newCollection = Arrays.asList(elements);
      if (needToCopyRef) {
        fury.reference(originCollection, newCollection);
      }
      copyElements(originCollection, elements);
      return newCollection;
    }

    @Override
    public void write(MemoryBuffer buffer, List<?> value) {
      try {
        final Object[] array = (Object[]) Platform.getObject(value, arrayFieldOffset);
        fury.writeRef(buffer, array);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public List<?> read(MemoryBuffer buffer) {
      final Object[] array = (Object[]) fury.readRef(buffer);
      Preconditions.checkNotNull(array);
      return Arrays.asList(array);
    }

    @Override
    public void xwrite(MemoryBuffer buffer, List<?> value) {
      super.write(buffer, value);
    }

    @Override
    public List<?> xread(MemoryBuffer buffer) {
      return super.read(buffer);
    }

    @Override
    public ArrayList newCollection(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      ArrayList arrayList = new ArrayList(numElements);
      fury.getRefResolver().reference(arrayList);
      return arrayList;
    }
  }

  public static final class HashSetSerializer extends CollectionSerializer<HashSet> {
    public HashSetSerializer(Fury fury) {
      super(fury, HashSet.class, true);
    }

    @Override
    public HashSet newCollection(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      HashSet hashSet = new HashSet(numElements);
      fury.getRefResolver().reference(hashSet);
      return hashSet;
    }
  }

  public static final class LinkedHashSetSerializer extends CollectionSerializer<LinkedHashSet> {
    public LinkedHashSetSerializer(Fury fury) {
      super(fury, LinkedHashSet.class, true);
    }

    @Override
    public LinkedHashSet newCollection(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      LinkedHashSet hashSet = new LinkedHashSet(numElements);
      fury.getRefResolver().reference(hashSet);
      return hashSet;
    }
  }

  public static class SortedSetSerializer<T extends SortedSet> extends CollectionSerializer<T> {
    private MethodHandle constructor;

    public SortedSetSerializer(Fury fury, Class<T> cls) {
      super(fury, cls, true);
      if (cls != TreeSet.class) {
        try {
          constructor = ReflectionUtils.getCtrHandle(cls, Comparator.class);
        } catch (Exception e) {
          throw new UnsupportedOperationException(e);
        }
      }
    }

    @Override
    public Collection onCollectionWrite(MemoryBuffer buffer, T value) {
      buffer.writeVarUint32Small7(value.size());
      if (!fury.isCrossLanguage()) {
        fury.writeRef(buffer, value.comparator());
      }
      return value;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T newCollection(MemoryBuffer buffer) {
      assert !fury.isCrossLanguage();
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      T collection;
      Comparator comparator = (Comparator) fury.readRef(buffer);
      if (type == TreeSet.class) {
        collection = (T) new TreeSet(comparator);
      } else {
        try {
          collection = (T) constructor.invoke(comparator);
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }
      fury.getRefResolver().reference(collection);
      return collection;
    }

    @Override
    public Collection newCollection(Collection originCollection) {
      Collection collection;
      Comparator comparator = fury.copyObject(((SortedSet) originCollection).comparator());
      if (Objects.equals(type, TreeSet.class)) {
        collection = new TreeSet(comparator);
      } else {
        try {
          collection = (T) constructor.invoke(comparator);
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }
      return collection;
    }
  }

  // ------------------------------ collections serializers ------------------------------ //
  // For cross-language serialization, if the data is passed from python, the data will be
  // deserialized by `MapSerializers` and `CollectionSerializers`.
  // But if the data is serialized by following collections serializers, we need to ensure the real
  // type of `xread` is the same as the type when serializing.
  public static final class EmptyListSerializer extends CollectionSerializer<List<?>> {

    public EmptyListSerializer(Fury fury, Class<List<?>> cls) {
      super(fury, cls, false, true);
    }

    @Override
    public void write(MemoryBuffer buffer, List<?> value) {}

    @Override
    public void xwrite(MemoryBuffer buffer, List<?> value) {
      // write length
      buffer.writeVarUint32Small7(0);
    }

    @Override
    public List<?> read(MemoryBuffer buffer) {
      return Collections.EMPTY_LIST;
    }

    @Override
    public List<?> xread(MemoryBuffer buffer) {
      buffer.readVarUint32Small7();
      return Collections.EMPTY_LIST;
    }
  }

  public static class CopyOnWriteArrayListSerializer
      extends CollectionSerializer<CopyOnWriteArrayList> {

    public CopyOnWriteArrayListSerializer(Fury fury, Class<CopyOnWriteArrayList> type) {
      super(fury, type);
    }

    @Override
    public Collection newCollection(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      return new CollectionContainer<>(numElements);
    }

    @Override
    public CopyOnWriteArrayList onCollectionRead(Collection collection) {
      Object[] elements = ((CollectionContainer) collection).elements;
      return new CopyOnWriteArrayList(elements);
    }
  }

  public static final class EmptySetSerializer extends CollectionSerializer<Set<?>> {

    public EmptySetSerializer(Fury fury, Class<Set<?>> cls) {
      super(fury, cls, fury.isCrossLanguage(), true);
    }

    @Override
    public void write(MemoryBuffer buffer, Set<?> value) {}

    @Override
    public void xwrite(MemoryBuffer buffer, Set<?> value) {
      super.write(buffer, value);
    }

    @Override
    public Set<?> read(MemoryBuffer buffer) {
      return Collections.EMPTY_SET;
    }

    @Override
    public Set<?> xread(MemoryBuffer buffer) {
      throw new UnsupportedOperationException();
    }
  }

  public static final class EmptySortedSetSerializer extends CollectionSerializer<SortedSet<?>> {

    public EmptySortedSetSerializer(Fury fury, Class<SortedSet<?>> cls) {
      super(fury, cls, fury.isCrossLanguage(), true);
    }

    @Override
    public void write(MemoryBuffer buffer, SortedSet<?> value) {}

    @Override
    public SortedSet<?> read(MemoryBuffer buffer) {
      return Collections.emptySortedSet();
    }

    @Override
    public void xwrite(MemoryBuffer buffer, SortedSet<?> value) {
      super.write(buffer, value);
    }

    @Override
    public SortedSet<?> xread(MemoryBuffer buffer) {
      throw new UnsupportedOperationException();
    }
  }

  public static final class CollectionsSingletonListSerializer
      extends CollectionSerializer<List<?>> {

    public CollectionsSingletonListSerializer(Fury fury, Class<List<?>> cls) {
      super(fury, cls, fury.isCrossLanguage());
    }

    @Override
    public List<?> copy(List<?> originCollection) {
      return Collections.singletonList(fury.copyObject(originCollection.get(0)));
    }

    @Override
    public void write(MemoryBuffer buffer, List<?> value) {
      fury.writeRef(buffer, value.get(0));
    }

    @Override
    public void xwrite(MemoryBuffer buffer, List<?> value) {
      super.write(buffer, value);
    }

    @Override
    public List<?> read(MemoryBuffer buffer) {
      return Collections.singletonList(fury.readRef(buffer));
    }

    @Override
    public List<?> xread(MemoryBuffer buffer) {
      throw new UnsupportedOperationException();
    }
  }

  public static final class CollectionsSingletonSetSerializer extends CollectionSerializer<Set<?>> {

    public CollectionsSingletonSetSerializer(Fury fury, Class<Set<?>> cls) {
      super(fury, cls, fury.isCrossLanguage());
    }

    @Override
    public Set<?> copy(Set<?> originCollection) {
      return Collections.singleton(fury.copyObject(originCollection.iterator().next()));
    }

    @Override
    public void write(MemoryBuffer buffer, Set<?> value) {
      fury.writeRef(buffer, value.iterator().next());
    }

    @Override
    public void xwrite(MemoryBuffer buffer, Set<?> value) {
      super.write(buffer, value);
    }

    @Override
    public Set<?> read(MemoryBuffer buffer) {
      return Collections.singleton(fury.readRef(buffer));
    }

    @Override
    public Set<?> xread(MemoryBuffer buffer) {
      throw new UnsupportedOperationException();
    }
  }

  public static final class ConcurrentSkipListSetSerializer
      extends SortedSetSerializer<ConcurrentSkipListSet> {

    public ConcurrentSkipListSetSerializer(Fury fury, Class<ConcurrentSkipListSet> cls) {
      super(fury, cls);
    }

    @Override
    public ConcurrentSkipListSet newCollection(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      assert !fury.isCrossLanguage();
      RefResolver refResolver = fury.getRefResolver();
      int refId = refResolver.lastPreservedRefId();
      // It's possible that comparator/elements has circular ref to set.
      Comparator comparator = (Comparator) fury.readRef(buffer);
      ConcurrentSkipListSet skipListSet = new ConcurrentSkipListSet(comparator);
      refResolver.setReadObject(refId, skipListSet);
      return skipListSet;
    }
  }

  public static final class SetFromMapSerializer extends CollectionSerializer<Set<?>> {

    private static final long MAP_FIELD_OFFSET;
    private static final List EMPTY_COLLECTION_STUB = new ArrayList<>();

    private static final MethodHandle m;

    private static final MethodHandle s;

    static {
      try {
        Class<?> type = Class.forName("java.util.Collections$SetFromMap");
        Field mapField = type.getDeclaredField("m");
        MAP_FIELD_OFFSET = Platform.objectFieldOffset(mapField);
        MethodHandles.Lookup lookup = _JDKAccess._trustedLookup(type);
        m = lookup.findSetter(type, "m", Map.class);
        s = lookup.findSetter(type, "s", Set.class);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    public SetFromMapSerializer(Fury fury, Class<Set<?>> type) {
      super(fury, type, true);
    }

    @Override
    public Collection newCollection(MemoryBuffer buffer) {
      final ClassInfo mapClassInfo = fury.getClassResolver().readClassInfo(buffer);
      final AbstractMapSerializer mapSerializer =
          (AbstractMapSerializer) mapClassInfo.getSerializer();
      RefResolver refResolver = fury.getRefResolver();
      // It's possible that elements or nested fields has circular ref to set.
      int refId = refResolver.lastPreservedRefId();
      Set set;
      if (buffer.readBoolean()) {
        refResolver.preserveRefId();
        set = Collections.newSetFromMap(mapSerializer.newMap(buffer));
        setNumElements(mapSerializer.getAndClearNumElements());
      } else {
        Map map = (Map) mapSerializer.read(buffer);
        try {
          set = Platform.newInstance(type);
          m.invoke(set, map);
          s.invoke(set, map.keySet());
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
        setNumElements(0);
      }
      refResolver.setReadObject(refId, set);
      return set;
    }

    @Override
    public Collection newCollection(Collection originCollection) {
      assert !fury.isCrossLanguage();
      Map<?, Boolean> map =
          (Map<?, Boolean>) Platform.getObject(originCollection, MAP_FIELD_OFFSET);
      AbstractMapSerializer mapSerializer =
          (AbstractMapSerializer) fury.getClassResolver().getSerializer(map.getClass());
      Map newMap = mapSerializer.newMap(map);
      return Collections.newSetFromMap(newMap);
    }

    @Override
    public Collection onCollectionWrite(MemoryBuffer buffer, Set<?> value) {
      final Map<?, Boolean> map = (Map<?, Boolean>) Platform.getObject(value, MAP_FIELD_OFFSET);
      final ClassInfo classInfo = fury.getClassResolver().getClassInfo(map.getClass());
      AbstractMapSerializer mapSerializer = (AbstractMapSerializer) classInfo.getSerializer();
      fury.getClassResolver().writeClassInfo(buffer, classInfo);
      if (mapSerializer.supportCodegenHook) {
        buffer.writeBoolean(true);
        mapSerializer.onMapWrite(buffer, map);
        return value;
      } else {
        buffer.writeBoolean(false);
        mapSerializer.write(buffer, map);
        return EMPTY_COLLECTION_STUB;
      }
    }
  }

  public static final class ConcurrentHashMapKeySetViewSerializer
      extends CollectionSerializer<ConcurrentHashMap.KeySetView> {
    private final ClassInfoHolder mapClassInfoHolder;
    private final ClassInfoHolder valueClassInfoHolder;

    public ConcurrentHashMapKeySetViewSerializer(
        Fury fury, Class<ConcurrentHashMap.KeySetView> type) {
      super(fury, type, false);
      mapClassInfoHolder = fury.getClassResolver().nilClassInfoHolder();
      valueClassInfoHolder = fury.getClassResolver().nilClassInfoHolder();
    }

    @Override
    public void write(MemoryBuffer buffer, ConcurrentHashMap.KeySetView value) {
      fury.writeRef(buffer, value.getMap(), mapClassInfoHolder);
      fury.writeRef(buffer, value.getMappedValue(), valueClassInfoHolder);
    }

    @Override
    public ConcurrentHashMap.KeySetView read(MemoryBuffer buffer) {
      ConcurrentHashMap map = (ConcurrentHashMap) fury.readRef(buffer, mapClassInfoHolder);
      Object value = fury.readRef(buffer, valueClassInfoHolder);
      return map.keySet(value);
    }

    @Override
    public ConcurrentHashMap.KeySetView copy(ConcurrentHashMap.KeySetView value) {
      ConcurrentHashMap newMap = fury.copyObject(value.getMap());
      return newMap.keySet(fury.copyObject(value.getMappedValue()));
    }

    @Override
    public Collection newCollection(MemoryBuffer buffer) {
      throw new IllegalStateException(
          "Should not be invoked since we set supportCodegenHook to false");
    }
  }

  public static final class VectorSerializer extends CollectionSerializer<Vector> {

    public VectorSerializer(Fury fury, Class<Vector> cls) {
      super(fury, cls, true);
    }

    @Override
    public Vector newCollection(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      Vector<Object> vector = new Vector<>(numElements);
      fury.getRefResolver().reference(vector);
      return vector;
    }
  }

  public static final class ArrayDequeSerializer extends CollectionSerializer<ArrayDeque> {

    public ArrayDequeSerializer(Fury fury, Class<ArrayDeque> cls) {
      super(fury, cls, true);
    }

    @Override
    public ArrayDeque newCollection(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      ArrayDeque deque = new ArrayDeque(numElements);
      fury.getRefResolver().reference(deque);
      return deque;
    }
  }

  public static class EnumSetSerializer extends CollectionSerializer<EnumSet> {
    public EnumSetSerializer(Fury fury, Class<EnumSet> type) {
      // getElementType(EnumSet.class) will be `E` without Enum as bound.
      // so no need to infer generics in init.
      super(fury, type, false);
    }

    @Override
    public void write(MemoryBuffer buffer, EnumSet object) {
      Class<?> elemClass;
      if (object.isEmpty()) {
        EnumSet tmp = EnumSet.complementOf(object);
        if (tmp.isEmpty()) {
          throw new FuryException("An EnumSet must have a defined Enum to be serialized.");
        }
        elemClass = tmp.iterator().next().getClass();
      } else {
        elemClass = object.iterator().next().getClass();
      }
      fury.getClassResolver().writeClassAndUpdateCache(buffer, elemClass);
      Serializer serializer = fury.getClassResolver().getSerializer(elemClass);
      buffer.writeVarUint32Small7(object.size());
      for (Object element : object) {
        serializer.write(buffer, element);
      }
    }

    @Override
    public EnumSet read(MemoryBuffer buffer) {
      Class elemClass = fury.getClassResolver().readClassInfo(buffer).getCls();
      EnumSet object = EnumSet.noneOf(elemClass);
      Serializer elemSerializer = fury.getClassResolver().getSerializer(elemClass);
      int length = buffer.readVarUint32Small7();
      for (int i = 0; i < length; i++) {
        object.add(elemSerializer.read(buffer));
      }
      return object;
    }

    @Override
    public EnumSet copy(EnumSet originCollection) {
      return EnumSet.copyOf(originCollection);
    }
  }

  public static class BitSetSerializer extends Serializer<BitSet> {
    public BitSetSerializer(Fury fury, Class<BitSet> type) {
      super(fury, type);
    }

    @Override
    public void write(MemoryBuffer buffer, BitSet set) {
      long[] values = set.toLongArray();
      buffer.writePrimitiveArrayWithSize(
          values, Platform.LONG_ARRAY_OFFSET, Math.multiplyExact(values.length, 8));
    }

    @Override
    public BitSet copy(BitSet originCollection) {
      return BitSet.valueOf(originCollection.toLongArray());
    }

    @Override
    public BitSet read(MemoryBuffer buffer) {
      long[] values = buffer.readLongs(buffer.readVarUint32Small7());
      return BitSet.valueOf(values);
    }
  }

  public static class PriorityQueueSerializer extends CollectionSerializer<PriorityQueue> {
    public PriorityQueueSerializer(Fury fury, Class<PriorityQueue> cls) {
      super(fury, cls, true);
    }

    public Collection onCollectionWrite(MemoryBuffer buffer, PriorityQueue value) {
      buffer.writeVarUint32Small7(value.size());
      if (!fury.isCrossLanguage()) {
        fury.writeRef(buffer, value.comparator());
      }
      return value;
    }

    @Override
    public Collection newCollection(Collection collection) {
      return new PriorityQueue(
          collection.size(), fury.copyObject(((PriorityQueue) collection).comparator()));
    }

    @Override
    public PriorityQueue newCollection(MemoryBuffer buffer) {
      assert !fury.isCrossLanguage();
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      Comparator comparator = (Comparator) fury.readRef(buffer);
      PriorityQueue queue = new PriorityQueue(comparator);
      fury.getRefResolver().reference(queue);
      return queue;
    }
  }

  /**
   * Java serializer to serialize all fields of a collection implementation. Note that this
   * serializer won't use element generics and doesn't support JIT, performance won't be the best,
   * but the correctness can be ensured.
   */
  public static final class DefaultJavaCollectionSerializer<T>
      extends AbstractCollectionSerializer<T> {
    private Serializer<T> dataSerializer;

    public DefaultJavaCollectionSerializer(Fury fury, Class<T> cls) {
      super(fury, cls, false);
      Preconditions.checkArgument(
          fury.getLanguage() == Language.JAVA,
          "Python default collection serializer should use " + CollectionSerializer.class);
      fury.getClassResolver().setSerializer(cls, this);
      Class<? extends Serializer> serializerClass =
          fury.getClassResolver()
              .getObjectSerializerClass(
                  cls, sc -> dataSerializer = Serializers.newSerializer(fury, cls, sc));
      dataSerializer = Serializers.newSerializer(fury, cls, serializerClass);
      // No need to set object serializer to this, it will be set in class resolver later.
      // fury.getClassResolver().setSerializer(cls, this);
    }

    @Override
    public Collection onCollectionWrite(MemoryBuffer buffer, T value) {
      throw new IllegalStateException();
    }

    @Override
    public T onCollectionRead(Collection collection) {
      throw new IllegalStateException();
    }

    @Override
    public void write(MemoryBuffer buffer, T value) {
      dataSerializer.write(buffer, value);
    }

    @Override
    public T copy(T originCollection) {
      return fury.copyObject(originCollection, dataSerializer);
    }

    @Override
    public T read(MemoryBuffer buffer) {
      return dataSerializer.read(buffer);
    }
  }

  /** Collection serializer for class with JDK custom serialization methods defined. */
  public static final class JDKCompatibleCollectionSerializer<T>
      extends AbstractCollectionSerializer<T> {
    private final Serializer serializer;

    public JDKCompatibleCollectionSerializer(Fury fury, Class<T> cls) {
      super(fury, cls, false);
      // Collection which defined `writeReplace` may use this serializer, so check replace/resolve
      // is necessary.
      Class<? extends Serializer> serializerType =
          ClassResolver.useReplaceResolveSerializer(cls)
              ? ReplaceResolveSerializer.class
              : fury.getDefaultJDKStreamSerializerType();
      serializer = Serializers.newSerializer(fury, cls, serializerType);
    }

    @Override
    public Collection onCollectionWrite(MemoryBuffer buffer, T value) {
      throw new IllegalStateException();
    }

    @SuppressWarnings("unchecked")
    @Override
    public T read(MemoryBuffer buffer) {
      return (T) serializer.read(buffer);
    }

    @Override
    public T onCollectionRead(Collection collection) {
      throw new IllegalStateException();
    }

    @Override
    public void write(MemoryBuffer buffer, T value) {
      serializer.write(buffer, value);
    }

    @Override
    public T copy(T value) {
      return fury.copyObject(value, (Serializer<T>) serializer);
    }
  }

  // TODO add JDK11:JdkImmutableListSerializer,JdkImmutableMapSerializer,JdkImmutableSetSerializer
  //  by jit codegen those constructor for compiling in jdk8.
  // TODO Support ArraySubListSerializer, SubListSerializer

  public static void registerDefaultSerializers(Fury fury) {
    ClassResolver resolver = fury.getClassResolver();
    resolver.registerSerializer(ArrayList.class, new ArrayListSerializer(fury));
    Class arrayAsListClass = Arrays.asList(1, 2).getClass();
    resolver.registerSerializer(
        arrayAsListClass, new ArraysAsListSerializer(fury, arrayAsListClass));
    resolver.registerSerializer(
        LinkedList.class, new CollectionSerializer(fury, LinkedList.class, true));
    resolver.registerSerializer(HashSet.class, new HashSetSerializer(fury));
    resolver.registerSerializer(LinkedHashSet.class, new LinkedHashSetSerializer(fury));
    resolver.registerSerializer(TreeSet.class, new SortedSetSerializer<>(fury, TreeSet.class));
    resolver.registerSerializer(
        Collections.EMPTY_LIST.getClass(),
        new EmptyListSerializer(fury, (Class<List<?>>) Collections.EMPTY_LIST.getClass()));
    resolver.registerSerializer(
        Collections.emptySortedSet().getClass(),
        new EmptySortedSetSerializer(
            fury, (Class<SortedSet<?>>) Collections.emptySortedSet().getClass()));
    resolver.registerSerializer(
        Collections.EMPTY_SET.getClass(),
        new EmptySetSerializer(fury, (Class<Set<?>>) Collections.EMPTY_SET.getClass()));
    resolver.registerSerializer(
        Collections.singletonList(null).getClass(),
        new CollectionsSingletonListSerializer(
            fury, (Class<List<?>>) Collections.singletonList(null).getClass()));
    resolver.registerSerializer(
        Collections.singleton(null).getClass(),
        new CollectionsSingletonSetSerializer(
            fury, (Class<Set<?>>) Collections.singleton(null).getClass()));
    resolver.registerSerializer(
        ConcurrentSkipListSet.class,
        new ConcurrentSkipListSetSerializer(fury, ConcurrentSkipListSet.class));
    resolver.registerSerializer(Vector.class, new VectorSerializer(fury, Vector.class));
    resolver.registerSerializer(ArrayDeque.class, new ArrayDequeSerializer(fury, ArrayDeque.class));
    resolver.registerSerializer(BitSet.class, new BitSetSerializer(fury, BitSet.class));
    resolver.registerSerializer(
        PriorityQueue.class, new PriorityQueueSerializer(fury, PriorityQueue.class));
    resolver.registerSerializer(
        CopyOnWriteArrayList.class,
        new CopyOnWriteArrayListSerializer(fury, CopyOnWriteArrayList.class));
    final Class setFromMapClass = Collections.newSetFromMap(new HashMap<>()).getClass();
    resolver.registerSerializer(setFromMapClass, new SetFromMapSerializer(fury, setFromMapClass));
    resolver.registerSerializer(
        ConcurrentHashMap.KeySetView.class,
        new ConcurrentHashMapKeySetViewSerializer(fury, ConcurrentHashMap.KeySetView.class));
  }
}
