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

import static io.fury.type.TypeUtils.getRawType;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import io.fury.Fury;
import io.fury.memory.MemoryBuffer;
import io.fury.resolver.ClassInfoCache;
import io.fury.resolver.ClassResolver;
import io.fury.resolver.ReferenceResolver;
import io.fury.type.GenericType;
import io.fury.type.Type;
import io.fury.type.TypeUtils;
import io.fury.util.Platform;
import io.fury.util.ReflectionUtils;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Serializers for classes implements {@link Collection}. All collection serializers should extend
 * {@link CollectionSerializer}.
 *
 * @author chaokunyang
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class CollectionSerializers {
  public static class CollectionSerializer<T extends Collection> extends Serializer<T> {
    private Constructor<?> constructor;
    private final boolean supportCodegenHook;
    // TODO remove elemSerializer, support generics in CompatibleSerializer.
    private Serializer<?> elemSerializer;
    private final ClassInfoCache elementClassInfoWriteCache;
    private final ClassInfoCache elementClassInfoReadCache;
    // support subclass whose element type are instantiated already, such as
    // `Subclass extends ArrayList<String>`.
    // nested generics such as `Subclass extends ArrayList<List<Integer>>` can only be passed by
    // `pushGenerics` instead of set element serializers.
    private final GenericType collectionGenericType;

    public CollectionSerializer(Fury fury, Class<T> cls) {
      this(fury, cls, !ReflectionUtils.isDynamicGeneratedCLass(cls), true);
    }

    public CollectionSerializer(
        Fury fury, Class<T> cls, boolean supportCodegenHook, boolean inferGenerics) {
      super(fury, cls);
      this.supportCodegenHook = supportCodegenHook;
      elementClassInfoWriteCache = fury.getClassResolver().nilClassInfoCache();
      elementClassInfoReadCache = fury.getClassResolver().nilClassInfoCache();
      if (inferGenerics) {
        TypeToken<?> elementType = TypeUtils.getElementType(TypeToken.of(cls));
        if (getRawType(elementType) != Object.class) {
          collectionGenericType =
              fury.getClassResolver()
                  .buildGenericType(TypeUtils.collectionOf(elementType).getType());
        } else {
          collectionGenericType = null;
        }
      } else {
        collectionGenericType = null;
      }
    }

    /**
     * Set element serializer for next serialization, the <code>serializer</code> will be cleared
     * when next serialization finished.
     */
    public void setElementSerializer(Serializer serializer) {
      elemSerializer = serializer;
    }

    @Override
    public void write(MemoryBuffer buffer, T value) {
      int len = value.size();
      buffer.writePositiveVarInt(len);
      writeHeader(buffer, value);
      writeElements(fury, buffer, value);
    }

    protected final void writeElements(Fury fury, MemoryBuffer buffer, T value) {
      Serializer elemSerializer = this.elemSerializer;
      // clear the elemSerializer to avoid conflict if the nested
      // serialization has collection field.
      // TODO use generics for compatible serializer.
      this.elemSerializer = null;
      ReferenceResolver refResolver = fury.getReferenceResolver();
      if (elemSerializer == null) {
        GenericType genericType = fury.getGenerics().nextGenericType();
        if (genericType == null || genericType.getTypeParametersCount() < 1) {
          genericType = collectionGenericType;
        }
        GenericType elemGenericType = null;
        if (genericType != null) {
          elemGenericType = genericType.getTypeParameter0();
        }
        if (elemGenericType != null) {
          javaWriteWithGenerics(fury, buffer, value, refResolver, elemGenericType);
        } else {
          for (Object elem : value) {
            fury.writeReferencableToJava(buffer, elem);
          }
        }
      } else {
        for (Object elem : value) {
          fury.writeReferencableToJava(buffer, elem, elemSerializer);
        }
      }
    }

    private void javaWriteWithGenerics(
        Fury fury,
        MemoryBuffer buffer,
        T value,
        ReferenceResolver refResolver,
        GenericType elemGenericType) {
      ClassResolver classResolver = fury.getClassResolver();
      Serializer elemSerializer;
      boolean hasGenericParameters = elemGenericType.hasGenericParameters();
      if (hasGenericParameters) {
        fury.getGenerics().pushGenericType(elemGenericType);
      }
      // Note: ObjectSerializer should mark `FinalElemType` in `Collection<FinalElemType>` as
      // non-final to
      // write class def when meta share is enabled.
      if (elemGenericType.isFinal()) {
        elemSerializer = elemGenericType.getSerializer(classResolver);
        for (Object elem : value) {
          fury.writeReferencableToJava(buffer, elem, elemSerializer);
        }
      } else {
        // whether ignore all subclass ref tracking.
        if (fury.getClassResolver().needToWriteReference(elemGenericType.getCls())) {
          for (Object elem : value) {
            fury.writeReferencableToJava(buffer, elem);
          }
        } else {
          for (Object elem : value) {
            if (!refResolver.writeNullFlag(buffer, elem)) {
              fury.writeReferencableToJava(
                  buffer,
                  elem,
                  classResolver.getClassInfo(elem.getClass(), elementClassInfoWriteCache));
            }
          }
        }
      }
      if (hasGenericParameters) {
        fury.getGenerics().popGenericType();
      }
    }

    @Override
    public void crossLanguageWrite(MemoryBuffer buffer, T value) {
      int len = value.size();
      buffer.writePositiveVarInt(len);
      crossLanguageWriteElements(fury, buffer, value);
    }

    public static void crossLanguageWriteElements(
        Fury fury, MemoryBuffer buffer, Collection value) {
      GenericType genericType = fury.getGenerics().nextGenericType();
      GenericType elemGenericType = null;
      if (genericType != null) {
        elemGenericType = genericType.getTypeParameter0();
      }
      if (elemGenericType != null) {
        boolean hasGenericParameters = elemGenericType.hasGenericParameters();
        if (hasGenericParameters) {
          fury.getGenerics().pushGenericType(elemGenericType);
        }
        if (elemGenericType.isFinal()) {
          Serializer elemSerializer = elemGenericType.getSerializer(fury.getClassResolver());
          for (Object elem : value) {
            fury.crossLanguageWriteReferencable(buffer, elem, elemSerializer);
          }
        } else {
          for (Object elem : value) {
            fury.crossLanguageWriteReferencable(buffer, elem);
          }
        }
        if (hasGenericParameters) {
          fury.getGenerics().popGenericType();
        }
      } else {
        for (Object elem : value) {
          fury.crossLanguageWriteReferencable(buffer, elem);
        }
      }
    }

    @Override
    public T read(MemoryBuffer buffer) {
      int numElements = buffer.readPositiveVarInt();
      T collection = newCollection(buffer, numElements);
      readElements(fury, buffer, collection, numElements);
      return collection;
    }

    protected final void readElements(
        Fury fury, MemoryBuffer buffer, T collection, int numElements) {
      Serializer elemSerializer = this.elemSerializer;
      // clear the elemSerializer to avoid conflict if the nested
      // serialization has collection field.
      // TODO use generics for compatible serializer.
      this.elemSerializer = null;
      if (elemSerializer == null) {
        GenericType genericType = fury.getGenerics().nextGenericType();
        if (genericType == null || genericType.getTypeParametersCount() < 1) {
          genericType = collectionGenericType;
        }
        GenericType elemGenericType = null;
        if (genericType != null) {
          elemGenericType = genericType.getTypeParameter0();
        }
        if (elemGenericType != null) {
          javaReadWithGenerics(fury, buffer, collection, numElements, elemGenericType);
        } else {
          for (int i = 0; i < numElements; i++) {
            Object elem = fury.readReferencableFromJava(buffer, elementClassInfoReadCache);
            collection.add(elem);
          }
        }
      } else {
        for (int i = 0; i < numElements; i++) {
          collection.add(fury.readReferencableFromJava(buffer, elemSerializer));
        }
      }
    }

    private void javaReadWithGenerics(
        Fury fury,
        MemoryBuffer buffer,
        T collection,
        int numElements,
        GenericType elemGenericType) {
      Serializer elemSerializer;
      boolean hasGenericParameters = elemGenericType.hasGenericParameters();
      if (hasGenericParameters) {
        fury.getGenerics().pushGenericType(elemGenericType);
      }
      if (elemGenericType.isFinal()) {
        elemSerializer = elemGenericType.getSerializer(fury.getClassResolver());
        for (int i = 0; i < numElements; i++) {
          Object elem = fury.readReferencableFromJava(buffer, elemSerializer);
          collection.add(elem);
        }
      } else {
        if (fury.getClassResolver().needToWriteReference(elemGenericType.getCls())) {
          for (int i = 0; i < numElements; i++) {
            Object elem = fury.readReferencableFromJava(buffer);
            collection.add(elem);
          }
        } else {
          for (int i = 0; i < numElements; i++) {
            if (buffer.readByte() == Fury.NULL_FLAG) {
              collection.add(null);
            } else {
              Object elem = fury.readNonReferenceFromJava(buffer, elementClassInfoReadCache);
              collection.add(elem);
            }
          }
        }
      }
      if (hasGenericParameters) {
        fury.getGenerics().popGenericType();
      }
    }

    @Override
    public T crossLanguageRead(MemoryBuffer buffer) {
      int numElements = buffer.readPositiveVarInt();
      T collection = newCollection(buffer, numElements);
      crossLanguageReadElements(fury, buffer, collection, numElements);
      return collection;
    }

    public static void crossLanguageReadElements(
        Fury fury, MemoryBuffer buffer, Collection collection, int numElements) {
      GenericType genericType = fury.getGenerics().nextGenericType();
      GenericType elemGenericType = null;
      if (genericType != null) {
        elemGenericType = genericType.getTypeParameter0();
      }
      if (elemGenericType != null) {
        boolean hasGenericParameters = elemGenericType.hasGenericParameters();
        if (hasGenericParameters) {
          fury.getGenerics().pushGenericType(elemGenericType);
        }
        if (elemGenericType.isFinal()) {
          Serializer elemSerializer = elemGenericType.getSerializer(fury.getClassResolver());
          for (int i = 0; i < numElements; i++) {
            Object elem =
                fury.crossLanguageReadReferencableByNullableSerializer(buffer, elemSerializer);
            collection.add(elem);
          }
        } else {
          for (int i = 0; i < numElements; i++) {
            Object elem = fury.crossLanguageReadReferencable(buffer);
            collection.add(elem);
          }
        }
        if (hasGenericParameters) {
          fury.getGenerics().popGenericType();
        }
      } else {
        for (int i = 0; i < numElements; i++) {
          Object elem = fury.crossLanguageReadReferencable(buffer);
          collection.add(elem);
        }
      }
    }

    /**
     * Hook for java serialization codegen, read/write elements will call collection.get/add
     * methods.
     *
     * <p>For key/value type which is final, using codegen may get a big performance gain
     *
     * @return true if read/write elements support calling collection.get/add methods
     */
    public final boolean supportCodegenHook() {
      return supportCodegenHook;
    }

    /**
     * Write data except size and elements.
     *
     * <ol>
     *   In codegen, follows is call order:
     *   <li>write collection class if not final
     *   <li>write collection size
     *   <li>writeHeader
     *   <li>write elements
     * </ol>
     */
    public void writeHeader(MemoryBuffer buffer, T value) {}

    /**
     * Read data except size and elements, return empty collection to be filled.
     *
     * <ol>
     *   In codegen, follows is call order:
     *   <li>read collection class if not final
     *   <li>read collection size
     *   <li>newCollection
     *   <li>read elements
     * </ol>
     */
    public T newCollection(MemoryBuffer buffer, int numElements) {
      if (constructor == null) {
        constructor = ReflectionUtils.newAccessibleNoArgConstructor(type);
      }
      try {
        T instance = (T) constructor.newInstance();
        fury.getReferenceResolver().reference(instance);
        return instance;
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalArgumentException(
            "Please provide public no arguments constructor for class " + type, e);
      }
    }
  }

  public static final class ArrayListSerializer extends CollectionSerializer<ArrayList> {
    public ArrayListSerializer(Fury fury) {
      super(fury, ArrayList.class, true, false);
    }

    @Override
    public short getCrossLanguageTypeId() {
      return Type.LIST.getId();
    }

    @Override
    public ArrayList newCollection(MemoryBuffer buffer, int numElements) {
      ArrayList arrayList = new ArrayList(numElements);
      fury.getReferenceResolver().reference(arrayList);
      return arrayList;
    }
  }

  public static final class ArraysAsListSerializer extends CollectionSerializer<List<?>> {
    private final long arrayFieldOffset;

    public ArraysAsListSerializer(Fury fury, Class<List<?>> cls) {
      super(fury, cls, false, false);
      try {
        Field arrayField = Class.forName("java.util.Arrays$ArrayList").getDeclaredField("a");
        arrayFieldOffset = ReflectionUtils.getFieldOffset(arrayField);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public short getCrossLanguageTypeId() {
      return (short) -Type.LIST.getId();
    }

    @Override
    public void write(MemoryBuffer buffer, List<?> value) {
      try {
        final Object[] array = (Object[]) Platform.getObject(value, arrayFieldOffset);
        fury.writeReferencableToJava(buffer, array);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void crossLanguageWrite(MemoryBuffer buffer, List<?> value) {
      // FIXME this may cause array data got duplicated when reference tracking enabled.
      super.crossLanguageWrite(buffer, value);
    }

    @Override
    public List<?> read(MemoryBuffer buffer) {
      final Object[] array = (Object[]) fury.readReferencableFromJava(buffer);
      Preconditions.checkNotNull(array);
      return Arrays.asList(array);
    }

    @Override
    public List<?> crossLanguageRead(MemoryBuffer buffer) {
      int numElements = buffer.readPositiveVarInt();
      Object[] arr = new Object[numElements];
      for (int i = 0; i < numElements; i++) {
        Object elem = fury.crossLanguageReadReferencable(buffer);
        arr[i] = elem;
      }
      return Arrays.asList(arr);
    }
  }

  public static final class HashSetSerializer extends CollectionSerializer<HashSet> {
    public HashSetSerializer(Fury fury) {
      super(fury, HashSet.class, true, false);
    }

    @Override
    public short getCrossLanguageTypeId() {
      return Type.FURY_SET.getId();
    }

    @Override
    public HashSet newCollection(MemoryBuffer buffer, int numElements) {
      HashSet hashSet = new HashSet(numElements);
      fury.getReferenceResolver().reference(hashSet);
      return hashSet;
    }
  }

  public static final class LinkedHashSetSerializer extends CollectionSerializer<LinkedHashSet> {
    public LinkedHashSetSerializer(Fury fury) {
      super(fury, LinkedHashSet.class, true, false);
    }

    @Override
    public short getCrossLanguageTypeId() {
      return Type.FURY_SET.getId();
    }

    @Override
    public LinkedHashSet newCollection(MemoryBuffer buffer, int numElements) {
      LinkedHashSet hashSet = new LinkedHashSet(numElements);
      fury.getReferenceResolver().reference(hashSet);
      return hashSet;
    }
  }

  public static class SortedSetSerializer<T extends SortedSet> extends CollectionSerializer<T> {
    private Constructor<?> constructor;

    public SortedSetSerializer(Fury fury, Class<T> cls) {
      super(fury, cls, true, false);
      if (cls != TreeSet.class) {
        try {
          this.constructor = cls.getConstructor(Comparator.class);
          if (!constructor.isAccessible()) {
            constructor.setAccessible(true);
          }
        } catch (Exception e) {
          throw new UnsupportedOperationException(e);
        }
      }
    }

    @Override
    public void writeHeader(MemoryBuffer buffer, T value) {
      fury.writeReferencableToJava(buffer, value.comparator());
    }

    @SuppressWarnings("unchecked")
    @Override
    public T newCollection(MemoryBuffer buffer, int numElements) {
      T collection;
      Comparator comparator = (Comparator) fury.readReferencableFromJava(buffer);
      if (type == TreeSet.class) {
        collection = (T) new TreeSet(comparator);
      } else {
        try {
          collection = (T) constructor.newInstance(comparator);
        } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
          throw new RuntimeException(e);
        }
      }
      fury.getReferenceResolver().reference(collection);
      return collection;
    }
  }

  // ------------------------------ collections serializers ------------------------------ //
  // For cross-language serialization, if the data is passed from python, the data will be
  // deserialized by `MapSerializers` and `CollectionSerializers`.
  // But if the data is serialized by following collections serializers, we need to ensure the real
  // type of `crossLanguageRead` is the same as the type when serializing.
  public static final class EmptyListSerializer extends CollectionSerializer<List<?>> {

    public EmptyListSerializer(Fury fury, Class<List<?>> cls) {
      super(fury, cls, false, false);
    }

    @Override
    public void write(MemoryBuffer buffer, List<?> value) {}

    @Override
    public short getCrossLanguageTypeId() {
      return (short) -Type.LIST.getId();
    }

    @Override
    public void crossLanguageWrite(MemoryBuffer buffer, List<?> value) {
      // write length
      buffer.writePositiveVarInt(0);
    }

    @Override
    public List<?> read(MemoryBuffer buffer) {
      return Collections.EMPTY_LIST;
    }

    @Override
    public List<?> crossLanguageRead(MemoryBuffer buffer) {
      buffer.readPositiveVarInt();
      return Collections.EMPTY_LIST;
    }
  }

  public static final class EmptySetSerializer extends CollectionSerializer<Set<?>> {

    public EmptySetSerializer(Fury fury, Class<Set<?>> cls) {
      super(fury, cls, false, false);
    }

    @Override
    public void write(MemoryBuffer buffer, Set<?> value) {}

    @Override
    public short getCrossLanguageTypeId() {
      return (short) -Type.FURY_SET.getId();
    }

    @Override
    public void crossLanguageWrite(MemoryBuffer buffer, Set<?> value) {
      // write length
      buffer.writePositiveVarInt(0);
    }

    @Override
    public Set<?> read(MemoryBuffer buffer) {
      return Collections.EMPTY_SET;
    }

    @Override
    public Set<?> crossLanguageRead(MemoryBuffer buffer) {
      buffer.readPositiveVarInt();
      return Collections.EMPTY_SET;
    }
  }

  public static final class EmptySortedSetSerializer extends CollectionSerializer<SortedSet<?>> {

    public EmptySortedSetSerializer(Fury fury, Class<SortedSet<?>> cls) {
      super(fury, cls, false, false);
    }

    @Override
    public void write(MemoryBuffer buffer, SortedSet<?> value) {}

    @Override
    public SortedSet<?> read(MemoryBuffer buffer) {
      return Collections.emptySortedSet();
    }
  }

  public static final class CollectionsSingletonListSerializer
      extends CollectionSerializer<List<?>> {

    public CollectionsSingletonListSerializer(Fury fury, Class<List<?>> cls) {
      super(fury, cls, false, false);
    }

    @Override
    public void write(MemoryBuffer buffer, List<?> value) {
      fury.writeReferencableToJava(buffer, value.get(0));
    }

    @Override
    public short getCrossLanguageTypeId() {
      return (short) -Type.LIST.getId();
    }

    @Override
    public void crossLanguageWrite(MemoryBuffer buffer, List<?> value) {
      buffer.writePositiveVarInt(1);
      fury.crossLanguageWriteReferencable(buffer, value.get(0));
    }

    @Override
    public List<?> read(MemoryBuffer buffer) {
      return Collections.singletonList(fury.readReferencableFromJava(buffer));
    }

    @Override
    public List<?> crossLanguageRead(MemoryBuffer buffer) {
      buffer.readPositiveVarInt();
      return Collections.singletonList(fury.crossLanguageReadReferencable(buffer));
    }
  }

  public static final class CollectionsSingletonSetSerializer extends CollectionSerializer<Set<?>> {

    public CollectionsSingletonSetSerializer(Fury fury, Class<Set<?>> cls) {
      super(fury, cls, false, false);
    }

    @Override
    public void write(MemoryBuffer buffer, Set<?> value) {
      fury.writeReferencableToJava(buffer, value.iterator().next());
    }

    @Override
    public short getCrossLanguageTypeId() {
      return (short) -Type.FURY_SET.getId();
    }

    @Override
    public void crossLanguageWrite(MemoryBuffer buffer, Set<?> value) {
      buffer.writePositiveVarInt(1);
      fury.crossLanguageWriteReferencable(buffer, value.iterator().next());
    }

    @Override
    public Set<?> read(MemoryBuffer buffer) {
      return Collections.singleton(fury.readReferencableFromJava(buffer));
    }

    @Override
    public Set<?> crossLanguageRead(MemoryBuffer buffer) {
      buffer.readPositiveVarInt();
      return Collections.singleton(fury.crossLanguageReadReferencable(buffer));
    }
  }

  public static void registerDefaultSerializers(Fury fury) {
    fury.registerSerializer(ArrayList.class, new ArrayListSerializer(fury));
    Class arrayAsListClass = Arrays.asList(1, 2).getClass();
    fury.registerSerializer(arrayAsListClass, new ArraysAsListSerializer(fury, arrayAsListClass));
    fury.registerSerializer(
        LinkedList.class, new CollectionSerializer(fury, LinkedList.class, true, false));
    fury.registerSerializer(HashSet.class, new HashSetSerializer(fury));
    fury.registerSerializer(LinkedHashSet.class, new LinkedHashSetSerializer(fury));
    fury.registerSerializer(TreeSet.class, new SortedSetSerializer<>(fury, TreeSet.class));
    fury.registerSerializer(
        Collections.EMPTY_LIST.getClass(),
        new EmptyListSerializer(fury, (Class<List<?>>) Collections.EMPTY_LIST.getClass()));
    fury.registerSerializer(
        Collections.emptySortedSet().getClass(),
        new EmptySortedSetSerializer(
            fury, (Class<SortedSet<?>>) Collections.emptySortedSet().getClass()));
    fury.registerSerializer(
        Collections.EMPTY_SET.getClass(),
        new EmptySetSerializer(fury, (Class<Set<?>>) Collections.EMPTY_SET.getClass()));
    fury.registerSerializer(
        Collections.singletonList(null).getClass(),
        new CollectionsSingletonListSerializer(
            fury, (Class<List<?>>) Collections.singletonList(null).getClass()));
    fury.registerSerializer(
        Collections.singleton(null).getClass(),
        new CollectionsSingletonSetSerializer(
            fury, (Class<Set<?>>) Collections.singleton(null).getClass()));
  }
}
