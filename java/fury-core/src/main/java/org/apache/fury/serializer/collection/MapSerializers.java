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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.fury.Fury;
import org.apache.fury.collection.LazyMap;
import org.apache.fury.config.Language;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.Platform;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.resolver.ClassInfo;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.serializer.ReplaceResolveSerializer;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.serializer.Serializers;
import org.apache.fury.serializer.StringSerializer;
import org.apache.fury.util.Preconditions;

/**
 * Serializers for classes implements {@link Collection}. All map serializers must extends {@link
 * MapSerializer}.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class MapSerializers {

  public static final class HashMapSerializer extends MapSerializer<HashMap> {
    public HashMapSerializer(Fury fury) {
      super(fury, HashMap.class, true);
    }

    @Override
    public HashMap newMap(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      HashMap hashMap = new HashMap(numElements);
      fury.getRefResolver().reference(hashMap);
      return hashMap;
    }

    @Override
    public Map newMap(Map map) {
      return new HashMap(map.size());
    }
  }

  public static final class LinkedHashMapSerializer extends MapSerializer<LinkedHashMap> {
    public LinkedHashMapSerializer(Fury fury) {
      super(fury, LinkedHashMap.class, true);
    }

    @Override
    public LinkedHashMap newMap(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      LinkedHashMap hashMap = new LinkedHashMap(numElements);
      fury.getRefResolver().reference(hashMap);
      return hashMap;
    }

    @Override
    public Map newMap(Map map) {
      return new LinkedHashMap(map.size());
    }
  }

  public static final class LazyMapSerializer extends MapSerializer<LazyMap> {
    public LazyMapSerializer(Fury fury) {
      super(fury, LazyMap.class, true);
    }

    @Override
    public LazyMap newMap(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      LazyMap map = new LazyMap(numElements);
      fury.getRefResolver().reference(map);
      return map;
    }

    @Override
    public Map newMap(Map map) {
      return new LazyMap(map.size());
    }
  }

  public static class SortedMapSerializer<T extends SortedMap> extends MapSerializer<T> {

    public SortedMapSerializer(Fury fury, Class<T> cls) {
      super(fury, cls, true);
      if (cls != TreeMap.class) {
        this.constructor = ReflectionUtils.getCtrHandle(cls, Comparator.class);
      }
    }

    @Override
    public Map onMapWrite(MemoryBuffer buffer, T value) {
      buffer.writeVarUint32Small7(value.size());
      if (!fury.isCrossLanguage()) {
        fury.writeRef(buffer, value.comparator());
      }
      return value;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Map newMap(MemoryBuffer buffer) {
      assert !fury.isCrossLanguage();
      setNumElements(buffer.readVarUint32Small7());
      T map;
      Comparator comparator = (Comparator) fury.readRef(buffer);
      if (type == TreeMap.class) {
        map = (T) new TreeMap(comparator);
      } else {
        try {
          map = (T) constructor.invoke(comparator);
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }
      fury.getRefResolver().reference(map);
      return map;
    }

    @Override
    public Map newMap(Map originMap) {
      Comparator comparator = fury.copyObject(((SortedMap) originMap).comparator());
      Map map;
      if (type == TreeMap.class) {
        map = new TreeMap(comparator);
      } else {
        try {
          map = (Map) constructor.invoke(comparator);
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }
      return map;
    }
  }

  public static final class EmptyMapSerializer extends MapSerializer<Map<?, ?>> {

    public EmptyMapSerializer(Fury fury, Class<Map<?, ?>> cls) {
      super(fury, cls, fury.isCrossLanguage(), true);
    }

    @Override
    public void write(MemoryBuffer buffer, Map<?, ?> value) {}

    @Override
    public void xwrite(MemoryBuffer buffer, Map<?, ?> value) {
      super.write(buffer, value);
    }

    @Override
    public Map<?, ?> read(MemoryBuffer buffer) {
      return Collections.EMPTY_MAP;
    }

    @Override
    public Map<?, ?> xread(MemoryBuffer buffer) {
      throw new IllegalStateException();
    }
  }

  public static final class EmptySortedMapSerializer extends MapSerializer<SortedMap<?, ?>> {
    public EmptySortedMapSerializer(Fury fury, Class<SortedMap<?, ?>> cls) {
      super(fury, cls, fury.isCrossLanguage(), true);
    }

    @Override
    public void write(MemoryBuffer buffer, SortedMap<?, ?> value) {}

    @Override
    public SortedMap<?, ?> read(MemoryBuffer buffer) {
      return Collections.emptySortedMap();
    }
  }

  public static final class SingletonMapSerializer extends MapSerializer<Map<?, ?>> {

    public SingletonMapSerializer(Fury fury, Class<Map<?, ?>> cls) {
      super(fury, cls, fury.isCrossLanguage());
    }

    @Override
    public Map<?, ?> copy(Map<?, ?> originMap) {
      Entry<?, ?> entry = originMap.entrySet().iterator().next();
      return Collections.singletonMap(
          fury.copyObject(entry.getKey()), fury.copyObject(entry.getValue()));
    }

    @Override
    public void write(MemoryBuffer buffer, Map<?, ?> value) {
      Map.Entry entry = value.entrySet().iterator().next();
      fury.writeRef(buffer, entry.getKey());
      fury.writeRef(buffer, entry.getValue());
    }

    @Override
    public Map<?, ?> read(MemoryBuffer buffer) {
      Object key = fury.readRef(buffer);
      Object value = fury.readRef(buffer);
      return Collections.singletonMap(key, value);
    }

    @Override
    public void xwrite(MemoryBuffer buffer, Map<?, ?> value) {
      super.write(buffer, value);
    }

    @Override
    public Map<?, ?> xread(MemoryBuffer buffer) {
      throw new UnsupportedOperationException();
    }
  }

  public static final class ConcurrentHashMapSerializer extends MapSerializer<ConcurrentHashMap> {

    public ConcurrentHashMapSerializer(Fury fury, Class<ConcurrentHashMap> type) {
      super(fury, type, true);
    }

    @Override
    public ConcurrentHashMap newMap(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      ConcurrentHashMap map = new ConcurrentHashMap(numElements);
      fury.getRefResolver().reference(map);
      return map;
    }

    @Override
    public Map newMap(Map map) {
      return new ConcurrentHashMap(map.size());
    }
  }

  public static final class ConcurrentSkipListMapSerializer
      extends SortedMapSerializer<ConcurrentSkipListMap> {

    public ConcurrentSkipListMapSerializer(Fury fury, Class<ConcurrentSkipListMap> cls) {
      super(fury, cls);
    }

    @Override
    public ConcurrentSkipListMap newMap(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      setNumElements(numElements);
      Comparator comparator = (Comparator) fury.readRef(buffer);
      ConcurrentSkipListMap map = new ConcurrentSkipListMap(comparator);
      fury.getRefResolver().reference(map);
      return map;
    }

    @Override
    public Map newMap(Map originMap) {
      Comparator comparator = fury.copyObject(((ConcurrentSkipListMap) originMap).comparator());
      return new ConcurrentSkipListMap(comparator);
    }
  }

  public static class EnumMapSerializer extends MapSerializer<EnumMap> {
    // Make offset compatible with graalvm native image.
    private static final long keyTypeFieldOffset;

    static {
      try {
        keyTypeFieldOffset = Platform.objectFieldOffset(EnumMap.class.getDeclaredField("keyType"));
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    public EnumMapSerializer(Fury fury) {
      // getMapKeyValueType(EnumMap.class) will be `K, V` without Enum as key bound.
      // so no need to infer key generics in init.
      super(fury, EnumMap.class, true);
    }

    @Override
    public Map onMapWrite(MemoryBuffer buffer, EnumMap value) {
      buffer.writeVarUint32Small7(value.size());
      Class keyType = (Class) Platform.getObject(value, keyTypeFieldOffset);
      fury.getClassResolver().writeClassAndUpdateCache(buffer, keyType);
      return value;
    }

    @Override
    public EnumMap newMap(MemoryBuffer buffer) {
      setNumElements(buffer.readVarUint32Small7());
      Class<?> keyType = fury.getClassResolver().readClassInfo(buffer).getCls();
      return new EnumMap(keyType);
    }

    @Override
    public EnumMap copy(EnumMap originMap) {
      return new EnumMap(originMap);
    }
  }

  public static class StringKeyMapSerializer<T> extends MapSerializer<Map<String, T>> {

    public StringKeyMapSerializer(Fury fury, Class<Map<String, T>> cls) {
      super(fury, cls, false);
      setKeySerializer(new StringSerializer(fury));
    }

    @Override
    public void write(MemoryBuffer buffer, Map<String, T> value) {
      buffer.writeVarUint32Small7(value.size());
      for (Map.Entry<String, T> e : value.entrySet()) {
        fury.writeJavaStringRef(buffer, e.getKey());
        // If value is a collection, the `newCollection` method will record itself to
        // reference map, which may get wrong index if this value is written without index.
        fury.writeRef(buffer, e.getValue());
      }
    }

    @Override
    public Map<String, T> read(MemoryBuffer buffer) {
      Map map = newMap(buffer);
      int numElements = getAndClearNumElements();
      for (int i = 0; i < numElements; i++) {
        map.put(fury.readJavaStringRef(buffer), fury.readRef(buffer));
      }
      return (Map<String, T>) map;
    }

    @Override
    protected <K, V> void copyEntry(Map<K, V> originMap, Map<K, V> newMap) {
      ClassResolver classResolver = fury.getClassResolver();
      for (Entry<K, V> entry : originMap.entrySet()) {
        V value = entry.getValue();
        if (value != null) {
          ClassInfo classInfo =
              classResolver.getClassInfo(value.getClass(), valueClassInfoWriteCache);
          if (!classInfo.getSerializer().isImmutable()) {
            value = fury.copyObject(value, classInfo.getClassId());
          }
        }
        newMap.put(entry.getKey(), value);
      }
    }
  }

  /**
   * Java serializer to serialize all fields of a map implementation. Note that this serializer
   * won't use element generics and doesn't support JIT, performance won't be the best, but the
   * correctness can be ensured.
   */
  public static final class DefaultJavaMapSerializer<T> extends AbstractMapSerializer<T> {
    private Serializer<T> dataSerializer;

    public DefaultJavaMapSerializer(Fury fury, Class<T> cls) {
      super(fury, cls, false);
      Preconditions.checkArgument(
          fury.getLanguage() == Language.JAVA,
          "Python default map serializer should use " + MapSerializer.class);
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
    public Map onMapWrite(MemoryBuffer buffer, T value) {
      throw new IllegalStateException();
    }

    @Override
    public T onMapCopy(Map map) {
      throw new IllegalStateException();
    }

    @Override
    public T onMapRead(Map map) {
      throw new IllegalStateException();
    }

    @Override
    public void write(MemoryBuffer buffer, T value) {
      dataSerializer.write(buffer, value);
    }

    @Override
    public T copy(T value) {
      return fury.copyObject(value, dataSerializer);
    }

    @Override
    public T read(MemoryBuffer buffer) {
      return dataSerializer.read(buffer);
    }
  }

  /** Map serializer for class with JDK custom serialization methods defined. */
  public static class JDKCompatibleMapSerializer<T> extends AbstractMapSerializer<T> {
    private final Serializer serializer;

    public JDKCompatibleMapSerializer(Fury fury, Class<T> cls) {
      super(fury, cls, false);
      // Map which defined `writeReplace` may use this serializer, so check replace/resolve
      // is necessary.
      Class<? extends Serializer> serializerType =
          ClassResolver.useReplaceResolveSerializer(cls)
              ? ReplaceResolveSerializer.class
              : fury.getDefaultJDKStreamSerializerType();
      serializer = Serializers.newSerializer(fury, cls, serializerType);
    }

    @Override
    public Map onMapWrite(MemoryBuffer buffer, T value) {
      throw new IllegalStateException();
    }

    @Override
    public T onMapCopy(Map map) {
      throw new IllegalStateException();
    }

    @Override
    public T onMapRead(Map map) {
      throw new IllegalStateException();
    }

    @SuppressWarnings("unchecked")
    @Override
    public T read(MemoryBuffer buffer) {
      return (T) serializer.read(buffer);
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

  // TODO(chaokunyang) support ConcurrentSkipListMap.SubMap mo efficiently.
  public static void registerDefaultSerializers(Fury fury) {
    ClassResolver resolver = fury.getClassResolver();
    resolver.registerSerializer(HashMap.class, new HashMapSerializer(fury));
    fury.getClassResolver()
        .registerSerializer(LinkedHashMap.class, new LinkedHashMapSerializer(fury));
    resolver.registerSerializer(TreeMap.class, new SortedMapSerializer<>(fury, TreeMap.class));
    resolver.registerSerializer(
        Collections.EMPTY_MAP.getClass(),
        new EmptyMapSerializer(fury, (Class<Map<?, ?>>) Collections.EMPTY_MAP.getClass()));
    resolver.registerSerializer(
        Collections.emptySortedMap().getClass(),
        new EmptySortedMapSerializer(
            fury, (Class<SortedMap<?, ?>>) Collections.emptySortedMap().getClass()));
    resolver.registerSerializer(
        Collections.singletonMap(null, null).getClass(),
        new SingletonMapSerializer(
            fury, (Class<Map<?, ?>>) Collections.singletonMap(null, null).getClass()));
    resolver.registerSerializer(
        ConcurrentHashMap.class, new ConcurrentHashMapSerializer(fury, ConcurrentHashMap.class));
    resolver.registerSerializer(
        ConcurrentSkipListMap.class,
        new ConcurrentSkipListMapSerializer(fury, ConcurrentSkipListMap.class));
    resolver.registerSerializer(EnumMap.class, new EnumMapSerializer(fury));
    resolver.registerSerializer(LazyMap.class, new LazyMapSerializer(fury));
  }
}
