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

import static io.fury.type.TypeUtils.getRawType;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import io.fury.Fury;
import io.fury.Language;
import io.fury.annotation.CodegenInvoke;
import io.fury.exception.FuryException;
import io.fury.memory.MemoryBuffer;
import io.fury.resolver.ClassInfo;
import io.fury.resolver.ClassInfoCache;
import io.fury.resolver.ClassResolver;
import io.fury.resolver.RefResolver;
import io.fury.type.GenericType;
import io.fury.type.Type;
import io.fury.type.TypeUtils;
import io.fury.util.Platform;
import io.fury.util.ReflectionUtils;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Vector;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Serializers for classes implements {@link Collection}. All collection serializers should extend
 * {@link CollectionSerializer}.
 *
 * @author chaokunyang
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class CollectionSerializers {
  /**
   * Default unset bitmap flags.
   *
   * <ul>
   *   <li>TRACKING_REF: false
   *   <li>HAS_NULL: false
   *   <li>NOT_DECL_ELEMENT_TYPE: false
   *   <li>NOT_SAME_TYPE: false
   * </ul>
   */
  public static class Flags {
    /** Whether track elements ref. */
    public static int TRACKING_REF = 0b1;
    /** Whether collection has null. */
    public static int HAS_NULL = 0b10;
    /** Whether collection elements type is not declare type. */
    public static int NOT_DECL_ELEMENT_TYPE = 0b100;
    /** Whether collection elements type different. */
    public static int NOT_SAME_TYPE = 0b1000;
  }

  public static class CollectionSerializer<T extends Collection> extends Serializer<T> {
    private Constructor<?> constructor;
    private final boolean supportCodegenHook;
    // TODO remove elemSerializer, support generics in CompatibleSerializer.
    private Serializer<?> elemSerializer;
    protected final ClassInfoCache elementClassInfoCache;
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
      elementClassInfoCache = fury.getClassResolver().nilClassInfoCache();
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

    private GenericType getElementGenericType(Fury fury) {
      GenericType genericType = fury.getGenerics().nextGenericType();
      if (genericType == null || genericType.getTypeParametersCount() < 1) {
        genericType = collectionGenericType;
      }
      GenericType elemGenericType = null;
      if (genericType != null) {
        elemGenericType = genericType.getTypeParameter0();
      }
      return elemGenericType;
    }

    /**
     * Set element serializer for next serialization, the <code>serializer</code> will be cleared
     * when next serialization finished.
     */
    public void setElementSerializer(Serializer serializer) {
      elemSerializer = serializer;
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
     * Write elements data header. Keep this consistent with
     * `BaseObjectCodecBuilder#writeElementsHeader`.
     *
     * @return a bitmap, higher 24 bits are reserved.
     */
    protected final int writeElementsHeader(MemoryBuffer buffer, T value) {
      GenericType elemGenericType = getElementGenericType(fury);
      if (elemGenericType != null) {
        boolean trackingRef = elemGenericType.trackingRef(fury.getClassResolver());
        if (elemGenericType.isFinal()) {
          if (trackingRef) {
            buffer.writeByte(Flags.TRACKING_REF);
            return Flags.TRACKING_REF;
          } else {
            return writeNullabilityHeader(buffer, value);
          }
        } else {
          if (trackingRef) {
            return writeTypeHeader(buffer, value, elemGenericType.getCls(), elementClassInfoCache);
          } else {
            return writeTypeNullabilityHeader(
                buffer, value, elemGenericType.getCls(), elementClassInfoCache);
          }
        }
      } else {
        if (elemSerializer != null) {
          if (elemSerializer.needToWriteRef) {
            buffer.writeByte(Flags.TRACKING_REF);
            return Flags.TRACKING_REF;
          } else {
            return writeNullabilityHeader(buffer, value);
          }
        } else {
          if (fury.trackingRef()) {
            return writeTypeHeader(buffer, value, elementClassInfoCache);
          } else {
            return writeTypeNullabilityHeader(buffer, value, null, elementClassInfoCache);
          }
        }
      }
    }

    /** Element type is final, write whether any elements is null. */
    @CodegenInvoke
    public int writeNullabilityHeader(MemoryBuffer buffer, T value) {
      for (Object elem : value) {
        if (elem == null) {
          buffer.writeByte(Flags.HAS_NULL);
          return Flags.HAS_NULL;
        }
      }
      buffer.writeByte(0);
      return 0;
    }

    /** Need to track elements ref, can't check elements nullability. */
    @CodegenInvoke
    public int writeTypeHeader(
        MemoryBuffer buffer, T value, Class<?> declareElementType, ClassInfoCache cache) {
      int bitmap = Flags.TRACKING_REF;
      boolean hasDifferentClass = false;
      Class<?> elemClass = null;
      for (Object elem : value) {
        if (elem != null) {
          if (elemClass == null) {
            elemClass = elem.getClass();
            continue;
          }
          if (elemClass != elem.getClass()) {
            hasDifferentClass = true;
            break;
          }
        }
      }
      if (hasDifferentClass) {
        bitmap |= Flags.NOT_SAME_TYPE | Flags.NOT_DECL_ELEMENT_TYPE;
        buffer.writeByte(bitmap);
      } else {
        // Write class in case peer doesn't have this class.
        if (!fury.getConfig().shareMetaContext() && elemClass == declareElementType) {
          buffer.writeByte(bitmap);
        } else {
          bitmap |= Flags.NOT_DECL_ELEMENT_TYPE;
          buffer.writeByte(bitmap);
          ClassResolver classResolver = fury.getClassResolver();
          ClassInfo classInfo = classResolver.getClassInfo(elemClass, cache);
          classResolver.writeClass(buffer, classInfo);
        }
      }
      return bitmap;
    }

    /** Maybe track elements ref, or write elements nullability. */
    @CodegenInvoke
    public int writeTypeHeader(MemoryBuffer buffer, T value, ClassInfoCache cache) {
      int bitmap = Flags.NOT_DECL_ELEMENT_TYPE;
      boolean hasDifferentClass = false;
      Class<?> elemClass = null;
      boolean containsNull = false;
      for (Object elem : value) {
        if (elem == null) {
          containsNull = true;
        } else if (elemClass == null) {
          elemClass = elem.getClass();
        } else {
          if (!hasDifferentClass && elem.getClass() != elemClass) {
            hasDifferentClass = true;
          }
        }
      }
      if (containsNull) {
        bitmap |= Flags.HAS_NULL;
      }
      if (hasDifferentClass) {
        bitmap |= Flags.NOT_SAME_TYPE | Flags.TRACKING_REF;
        buffer.writeByte(bitmap);
      } else {
        ClassResolver classResolver = fury.getClassResolver();
        ClassInfo classInfo = classResolver.getClassInfo(elemClass, cache);
        if (classInfo.getSerializer().needToWriteRef) {
          bitmap |= Flags.TRACKING_REF;
        }
        buffer.writeByte(bitmap);
        classResolver.writeClass(buffer, classInfo);
      }
      return bitmap;
    }

    /**
     * Element type is not final by {@link ClassResolver#isFinal}, need to write element type.
     * Elements ref tracking is disabled, write whether any elements is null.
     */
    @CodegenInvoke
    public int writeTypeNullabilityHeader(
        MemoryBuffer buffer, T value, Class<?> declareElementType, ClassInfoCache cache) {
      int bitmap = 0;
      boolean containsNull = false;
      boolean hasDifferentClass = false;
      Class<?> elemClass = null;
      for (Object elem : value) {
        if (elem == null) {
          containsNull = true;
        } else if (elemClass == null) {
          elemClass = elem.getClass();
        } else {
          if (!hasDifferentClass && elem.getClass() != elemClass) {
            hasDifferentClass = true;
          }
        }
      }
      if (containsNull) {
        bitmap |= Flags.HAS_NULL;
      }
      if (hasDifferentClass) {
        bitmap |= Flags.NOT_SAME_TYPE | Flags.NOT_DECL_ELEMENT_TYPE;
        buffer.writeByte(bitmap);
      } else {
        if (elemClass != declareElementType) {
          bitmap |= Flags.NOT_DECL_ELEMENT_TYPE;
          buffer.writeByte(bitmap);
          ClassResolver classResolver = fury.getClassResolver();
          ClassInfo classInfo = classResolver.getClassInfo(elemClass, cache);
          classResolver.writeClass(buffer, classInfo);
        }
      }
      return bitmap;
    }

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
    public Collection newCollection(MemoryBuffer buffer, int numElements) {
      if (constructor == null) {
        constructor = ReflectionUtils.newAccessibleNoArgConstructor(type);
      }
      try {
        T instance = (T) constructor.newInstance();
        fury.getRefResolver().reference(instance);
        return instance;
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new IllegalArgumentException(
            "Please provide public no arguments constructor for class " + type, e);
      }
    }

    public T onCollectionRead(Collection collection) {
      return (T) collection;
    }

    @Override
    public void write(MemoryBuffer buffer, T value) {
      int len = value.size();
      buffer.writePositiveVarInt(len);
      writeHeader(buffer, value);
      if (len != 0) {
        writeElements(fury, buffer, value);
      }
    }

    protected final void writeElements(Fury fury, MemoryBuffer buffer, T value) {
      int flags = writeElementsHeader(buffer, value);
      Serializer serializer = this.elemSerializer;
      // clear the elemSerializer to avoid conflict if the nested
      // serialization has collection field.
      this.elemSerializer = null;
      if (serializer == null) {
        GenericType elemGenericType = getElementGenericType(fury);
        if (elemGenericType != null) {
          javaWriteWithGenerics(fury, buffer, value, elemGenericType, flags);
        } else {
          generalJavaWrite(fury, buffer, value, flags);
        }
      } else {
        compatibleWrite(fury, buffer, value, serializer, flags);
      }
    }

    // TODO use generics for compatible serializer.
    private static <T extends Collection> void compatibleWrite(
        Fury fury, MemoryBuffer buffer, T value, Serializer serializer, int flags) {
      if (serializer.needToWriteRef) {
        for (Object elem : value) {
          fury.writeRef(buffer, elem, serializer);
        }
      } else {
        boolean hasNull = (flags & Flags.HAS_NULL) == Flags.HAS_NULL;
        if (hasNull) {
          for (Object elem : value) {
            if (elem == null) {
              buffer.writeByte(Fury.NULL_FLAG);
            } else {
              buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
              serializer.write(buffer, elem);
            }
          }
        } else {
          for (Object elem : value) {
            serializer.write(buffer, elem);
          }
        }
      }
    }

    private void javaWriteWithGenerics(
        Fury fury, MemoryBuffer buffer, T collection, GenericType elemGenericType, int flags) {
      boolean hasGenericParameters = elemGenericType.hasGenericParameters();
      if (hasGenericParameters) {
        fury.getGenerics().pushGenericType(elemGenericType);
      }
      // Note: ObjectSerializer should mark `FinalElemType` in `Collection<FinalElemType>`
      // as non-final to write class def when meta share is enabled.
      if (elemGenericType.isFinal()) {
        Serializer serializer = elemGenericType.getSerializer(fury.getClassResolver());
        writeSameTypeElements(fury, buffer, serializer, flags, collection);
      } else {
        generalJavaWrite(fury, buffer, collection, flags);
      }
      if (hasGenericParameters) {
        fury.getGenerics().popGenericType();
      }
    }

    private void generalJavaWrite(Fury fury, MemoryBuffer buffer, T collection, int flags) {
      if ((flags & Flags.NOT_SAME_TYPE) != Flags.NOT_SAME_TYPE) {
        Serializer serializer = elementClassInfoCache.getSerializer();
        writeSameTypeElements(fury, buffer, serializer, flags, collection);
      } else {
        writeDifferentTypeElements(fury, buffer, flags, collection);
      }
    }

    private static <T extends Collection> void writeSameTypeElements(
        Fury fury, MemoryBuffer buffer, Serializer serializer, int flags, T collection) {
      fury.incDepth(1);
      if ((flags & Flags.TRACKING_REF) == Flags.TRACKING_REF) {
        RefResolver refResolver = fury.getRefResolver();
        for (Object elem : collection) {
          if (!refResolver.writeRefOrNull(buffer, elem)) {
            serializer.write(buffer, elem);
          }
        }
      } else {
        if ((flags & Flags.HAS_NULL) != Flags.HAS_NULL) {
          for (Object elem : collection) {
            serializer.write(buffer, elem);
          }
        } else {
          for (Object elem : collection) {
            if (elem == null) {
              buffer.writeByte(Fury.NULL_FLAG);
            } else {
              buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
              serializer.write(buffer, elem);
            }
          }
        }
      }
      fury.incDepth(-1);
    }

    private static <T extends Collection> void writeDifferentTypeElements(
        Fury fury, MemoryBuffer buffer, int flags, T collection) {
      if ((flags & Flags.TRACKING_REF) == Flags.TRACKING_REF) {
        for (Object elem : collection) {
          fury.writeRef(buffer, elem);
        }
      } else {
        if ((flags & Flags.HAS_NULL) != Flags.HAS_NULL) {
          for (Object elem : collection) {
            fury.writeNonRef(buffer, elem);
          }
        } else {
          for (Object elem : collection) {
            fury.writeNullable(buffer, elem);
          }
        }
      }
    }

    @Override
    public void xwrite(MemoryBuffer buffer, T value) {
      int len = value.size();
      buffer.writePositiveVarInt(len);
      xwriteElements(fury, buffer, value);
    }

    private void xwriteElements(Fury fury, MemoryBuffer buffer, Collection value) {
      GenericType elemGenericType = getElementGenericType(fury);
      if (elemGenericType != null) {
        boolean hasGenericParameters = elemGenericType.hasGenericParameters();
        if (hasGenericParameters) {
          fury.getGenerics().pushGenericType(elemGenericType);
        }
        if (elemGenericType.isFinal()) {
          Serializer elemSerializer = elemGenericType.getSerializer(fury.getClassResolver());
          for (Object elem : value) {
            fury.xwriteRef(buffer, elem, elemSerializer);
          }
        } else {
          for (Object elem : value) {
            fury.xwriteRef(buffer, elem);
          }
        }
        if (hasGenericParameters) {
          fury.getGenerics().popGenericType();
        }
      } else {
        for (Object elem : value) {
          fury.xwriteRef(buffer, elem);
        }
      }
    }

    @Override
    public T read(MemoryBuffer buffer) {
      int numElements = buffer.readPositiveVarInt();
      Collection collection = newCollection(buffer, numElements);
      if (numElements != 0) {
        readElements(fury, buffer, collection, numElements);
      }
      return onCollectionRead(collection);
    }

    private void readElements(
        Fury fury, MemoryBuffer buffer, Collection collection, int numElements) {
      int flags = buffer.readByte();
      Serializer serializer = this.elemSerializer;
      // clear the elemSerializer to avoid conflict if the nested
      // serialization has collection field.
      // TODO use generics for compatible serializer.
      this.elemSerializer = null;
      if (serializer == null) {
        GenericType elemGenericType = getElementGenericType(fury);
        if (elemGenericType != null) {
          javaReadWithGenerics(fury, buffer, collection, numElements, elemGenericType, flags);
        } else {
          generalJavaRead(fury, buffer, collection, numElements, flags, null);
        }
      } else {
        compatibleRead(fury, buffer, collection, numElements, serializer, flags);
      }
    }

    /** Code path for {@link CompatibleSerializer}. */
    private static void compatibleRead(
        Fury fury,
        MemoryBuffer buffer,
        Collection collection,
        int numElements,
        Serializer serializer,
        int flags) {
      if (serializer.needToWriteRef) {
        for (int i = 0; i < numElements; i++) {
          collection.add(fury.readRef(buffer, serializer));
        }
      } else {
        if ((flags & Flags.HAS_NULL) == Flags.HAS_NULL) {
          for (int i = 0; i < numElements; i++) {
            if (buffer.readByte() == Fury.NULL_FLAG) {
              collection.add(null);
            } else {
              Object elem = serializer.read(buffer);
              collection.add(elem);
            }
          }
        } else {
          for (int i = 0; i < numElements; i++) {
            Object elem = serializer.read(buffer);
            collection.add(elem);
          }
        }
      }
    }

    private void javaReadWithGenerics(
        Fury fury,
        MemoryBuffer buffer,
        Collection collection,
        int numElements,
        GenericType elemGenericType,
        int flags) {
      boolean hasGenericParameters = elemGenericType.hasGenericParameters();
      if (hasGenericParameters) {
        fury.getGenerics().pushGenericType(elemGenericType);
      }
      if (elemGenericType.isFinal()) {
        Serializer serializer = elemGenericType.getSerializer(fury.getClassResolver());
        readSameTypeElements(fury, buffer, serializer, flags, collection, numElements);
      } else {
        generalJavaRead(fury, buffer, collection, numElements, flags, elemGenericType);
      }
      if (hasGenericParameters) {
        fury.getGenerics().popGenericType();
      }
    }

    private void generalJavaRead(
        Fury fury,
        MemoryBuffer buffer,
        Collection collection,
        int numElements,
        int flags,
        GenericType elemGenericType) {
      if ((flags & Flags.NOT_SAME_TYPE) != Flags.NOT_SAME_TYPE) {
        Serializer serializer;
        ClassResolver classResolver = fury.getClassResolver();
        if ((flags & Flags.NOT_DECL_ELEMENT_TYPE) == Flags.NOT_DECL_ELEMENT_TYPE) {
          serializer = classResolver.readClassInfo(buffer, elementClassInfoCache).getSerializer();
        } else {
          Preconditions.checkNotNull(elemGenericType);
          serializer = elemGenericType.getSerializer(classResolver);
        }
        readSameTypeElements(fury, buffer, serializer, flags, collection, numElements);
      } else {
        readDifferentTypeElements(fury, buffer, flags, collection, numElements);
      }
    }

    /** Read elements whose type are same. */
    private static <T extends Collection> void readSameTypeElements(
        Fury fury,
        MemoryBuffer buffer,
        Serializer serializer,
        int flags,
        T collection,
        int numElements) {
      fury.incDepth(1);
      if ((flags & Flags.TRACKING_REF) == Flags.TRACKING_REF) {
        for (int i = 0; i < numElements; i++) {
          collection.add(fury.readRef(buffer, serializer));
        }
      } else {
        if ((flags & Flags.HAS_NULL) != Flags.HAS_NULL) {
          for (int i = 0; i < numElements; i++) {
            collection.add(serializer.read(buffer));
          }
        } else {
          for (int i = 0; i < numElements; i++) {
            if (buffer.readByte() == Fury.NULL_FLAG) {
              collection.add(null);
            } else {
              collection.add(serializer.read(buffer));
            }
          }
        }
      }
      fury.incDepth(-1);
    }

    /** Read elements whose type are different. */
    private static <T extends Collection> void readDifferentTypeElements(
        Fury fury, MemoryBuffer buffer, int flags, T collection, int numElements) {
      if ((flags & Flags.TRACKING_REF) == Flags.TRACKING_REF) {
        for (int i = 0; i < numElements; i++) {
          collection.add(fury.readRef(buffer));
        }
      } else {
        if ((flags & Flags.HAS_NULL) != Flags.HAS_NULL) {
          for (int i = 0; i < numElements; i++) {
            collection.add(fury.readNonRef(buffer));
          }
        } else {
          for (int i = 0; i < numElements; i++) {
            collection.add(fury.readNullable(buffer));
          }
        }
      }
    }

    @Override
    public T xread(MemoryBuffer buffer) {
      int numElements = buffer.readPositiveVarInt();
      Collection collection = newCollection(buffer, numElements);
      xreadElements(fury, buffer, collection, numElements);
      return onCollectionRead(collection);
    }

    public void xreadElements(
        Fury fury, MemoryBuffer buffer, Collection collection, int numElements) {
      GenericType elemGenericType = getElementGenericType(fury);
      if (elemGenericType != null) {
        boolean hasGenericParameters = elemGenericType.hasGenericParameters();
        if (hasGenericParameters) {
          fury.getGenerics().pushGenericType(elemGenericType);
        }
        if (elemGenericType.isFinal()) {
          Serializer elemSerializer = elemGenericType.getSerializer(fury.getClassResolver());
          for (int i = 0; i < numElements; i++) {
            Object elem = fury.xreadRefByNullableSerializer(buffer, elemSerializer);
            collection.add(elem);
          }
        } else {
          for (int i = 0; i < numElements; i++) {
            Object elem = fury.xreadRef(buffer);
            collection.add(elem);
          }
        }
        if (hasGenericParameters) {
          fury.getGenerics().popGenericType();
        }
      } else {
        for (int i = 0; i < numElements; i++) {
          Object elem = fury.xreadRef(buffer);
          collection.add(elem);
        }
      }
    }
  }

  public static final class ArrayListSerializer extends CollectionSerializer<ArrayList> {
    public ArrayListSerializer(Fury fury) {
      super(fury, ArrayList.class, true, false);
    }

    @Override
    public short getXtypeId() {
      return Type.LIST.getId();
    }

    @Override
    public ArrayList newCollection(MemoryBuffer buffer, int numElements) {
      ArrayList arrayList = new ArrayList(numElements);
      fury.getRefResolver().reference(arrayList);
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
    public short getXtypeId() {
      return (short) -Type.LIST.getId();
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
    public void xwrite(MemoryBuffer buffer, List<?> value) {
      // FIXME this may cause array data got duplicated when reference tracking enabled.
      super.xwrite(buffer, value);
    }

    @Override
    public List<?> read(MemoryBuffer buffer) {
      final Object[] array = (Object[]) fury.readRef(buffer);
      Preconditions.checkNotNull(array);
      return Arrays.asList(array);
    }

    @Override
    public List<?> xread(MemoryBuffer buffer) {
      int numElements = buffer.readPositiveVarInt();
      Object[] arr = new Object[numElements];
      for (int i = 0; i < numElements; i++) {
        Object elem = fury.xreadRef(buffer);
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
    public short getXtypeId() {
      return Type.FURY_SET.getId();
    }

    @Override
    public HashSet newCollection(MemoryBuffer buffer, int numElements) {
      HashSet hashSet = new HashSet(numElements);
      fury.getRefResolver().reference(hashSet);
      return hashSet;
    }
  }

  public static final class LinkedHashSetSerializer extends CollectionSerializer<LinkedHashSet> {
    public LinkedHashSetSerializer(Fury fury) {
      super(fury, LinkedHashSet.class, true, false);
    }

    @Override
    public short getXtypeId() {
      return Type.FURY_SET.getId();
    }

    @Override
    public LinkedHashSet newCollection(MemoryBuffer buffer, int numElements) {
      LinkedHashSet hashSet = new LinkedHashSet(numElements);
      fury.getRefResolver().reference(hashSet);
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
      fury.writeRef(buffer, value.comparator());
    }

    @SuppressWarnings("unchecked")
    @Override
    public T newCollection(MemoryBuffer buffer, int numElements) {
      T collection;
      Comparator comparator = (Comparator) fury.readRef(buffer);
      if (type == TreeSet.class) {
        collection = (T) new TreeSet(comparator);
      } else {
        try {
          collection = (T) constructor.newInstance(comparator);
        } catch (IllegalAccessException | InvocationTargetException | InstantiationException e) {
          throw new RuntimeException(e);
        }
      }
      fury.getRefResolver().reference(collection);
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
      super(fury, cls, false, false);
    }

    @Override
    public void write(MemoryBuffer buffer, List<?> value) {}

    @Override
    public short getXtypeId() {
      return (short) -Type.LIST.getId();
    }

    @Override
    public void xwrite(MemoryBuffer buffer, List<?> value) {
      // write length
      buffer.writePositiveVarInt(0);
    }

    @Override
    public List<?> read(MemoryBuffer buffer) {
      return Collections.EMPTY_LIST;
    }

    @Override
    public List<?> xread(MemoryBuffer buffer) {
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
    public short getXtypeId() {
      return (short) -Type.FURY_SET.getId();
    }

    @Override
    public void xwrite(MemoryBuffer buffer, Set<?> value) {
      // write length
      buffer.writePositiveVarInt(0);
    }

    @Override
    public Set<?> read(MemoryBuffer buffer) {
      return Collections.EMPTY_SET;
    }

    @Override
    public Set<?> xread(MemoryBuffer buffer) {
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
      fury.writeRef(buffer, value.get(0));
    }

    @Override
    public short getXtypeId() {
      return (short) -Type.LIST.getId();
    }

    @Override
    public void xwrite(MemoryBuffer buffer, List<?> value) {
      buffer.writePositiveVarInt(1);
      fury.xwriteRef(buffer, value.get(0));
    }

    @Override
    public List<?> read(MemoryBuffer buffer) {
      return Collections.singletonList(fury.readRef(buffer));
    }

    @Override
    public List<?> xread(MemoryBuffer buffer) {
      buffer.readPositiveVarInt();
      return Collections.singletonList(fury.xreadRef(buffer));
    }
  }

  public static final class CollectionsSingletonSetSerializer extends CollectionSerializer<Set<?>> {

    public CollectionsSingletonSetSerializer(Fury fury, Class<Set<?>> cls) {
      super(fury, cls, false, false);
    }

    @Override
    public void write(MemoryBuffer buffer, Set<?> value) {
      fury.writeRef(buffer, value.iterator().next());
    }

    @Override
    public short getXtypeId() {
      return (short) -Type.FURY_SET.getId();
    }

    @Override
    public void xwrite(MemoryBuffer buffer, Set<?> value) {
      buffer.writePositiveVarInt(1);
      fury.xwriteRef(buffer, value.iterator().next());
    }

    @Override
    public Set<?> read(MemoryBuffer buffer) {
      return Collections.singleton(fury.readRef(buffer));
    }

    @Override
    public Set<?> xread(MemoryBuffer buffer) {
      buffer.readPositiveVarInt();
      return Collections.singleton(fury.xreadRef(buffer));
    }
  }

  public static final class ConcurrentSkipListSetSerializer
      extends SortedSetSerializer<ConcurrentSkipListSet> {

    public ConcurrentSkipListSetSerializer(Fury fury, Class<ConcurrentSkipListSet> cls) {
      super(fury, cls);
    }

    @Override
    public ConcurrentSkipListSet newCollection(MemoryBuffer buffer, int numElements) {
      Comparator comparator = (Comparator) fury.readRef(buffer);
      ConcurrentSkipListSet skipListSet = new ConcurrentSkipListSet(comparator);
      fury.getRefResolver().reference(skipListSet);
      return skipListSet;
    }
  }

  public static final class VectorSerializer extends CollectionSerializer<Vector> {

    public VectorSerializer(Fury fury, Class<Vector> cls) {
      super(fury, cls, true, false);
    }

    @Override
    public Vector newCollection(MemoryBuffer buffer, int numElements) {
      Vector<Object> vector = new Vector<>(numElements);
      fury.getRefResolver().reference(vector);
      return vector;
    }
  }

  public static final class ArrayDequeSerializer extends CollectionSerializer<ArrayDeque> {

    public ArrayDequeSerializer(Fury fury, Class<ArrayDeque> cls) {
      super(fury, cls, true, false);
    }

    @Override
    public ArrayDeque newCollection(MemoryBuffer buffer, int numElements) {
      ArrayDeque deque = new ArrayDeque(numElements);
      fury.getRefResolver().reference(deque);
      return deque;
    }
  }

  public static class EnumSetSerializer extends CollectionSerializer<EnumSet> {
    public EnumSetSerializer(Fury fury, Class<EnumSet> type) {
      // getElementType(EnumSet.class) will be `E` without Enum as bound.
      // so no need to infer generics in init.
      super(fury, type, false, false);
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
      buffer.writePositiveVarIntAligned(object.size());
      for (Object element : object) {
        serializer.write(buffer, element);
      }
    }

    @Override
    public EnumSet read(MemoryBuffer buffer) {
      Class elemClass = fury.getClassResolver().readClassAndUpdateCache(buffer);
      EnumSet object = EnumSet.noneOf(elemClass);
      Serializer elemSerializer = fury.getClassResolver().getSerializer(elemClass);
      int length = buffer.readPositiveAlignedVarInt();
      for (int i = 0; i < length; i++) {
        object.add(elemSerializer.read(buffer));
      }
      return object;
    }
  }

  public static class BitSetSerializer extends Serializer<BitSet> {
    public BitSetSerializer(Fury fury, Class<BitSet> type) {
      super(fury, type);
    }

    @Override
    public void write(MemoryBuffer buffer, BitSet set) {
      long[] values = set.toLongArray();
      buffer.writePrimitiveArrayWithSizeEmbedded(
          values, Platform.LONG_ARRAY_OFFSET, Math.multiplyExact(values.length, 8));
    }

    @Override
    public BitSet read(MemoryBuffer buffer) {
      long[] values = buffer.readLongsWithSizeEmbedded();
      return BitSet.valueOf(values);
    }
  }

  public static class PriorityQueueSerializer extends CollectionSerializer<PriorityQueue> {
    public PriorityQueueSerializer(Fury fury, Class<PriorityQueue> cls) {
      super(fury, cls, true, false);
    }

    public void writeHeader(MemoryBuffer buffer, PriorityQueue value) {
      fury.writeRef(buffer, value.comparator());
    }

    @Override
    public PriorityQueue newCollection(MemoryBuffer buffer, int numElements) {
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
  public static final class DefaultJavaCollectionSerializer<T extends Collection>
      extends CollectionSerializer<T> {
    private Serializer<T> dataSerializer;

    public DefaultJavaCollectionSerializer(Fury fury, Class<T> cls) {
      super(fury, cls, false, false);
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
    public void write(MemoryBuffer buffer, T value) {
      dataSerializer.write(buffer, value);
    }

    @Override
    public T read(MemoryBuffer buffer) {
      return dataSerializer.read(buffer);
    }
  }

  /** Collection serializer for class with JDK custom serialization methods defined. */
  public static final class JDKCompatibleCollectionSerializer<T extends Collection>
      extends CollectionSerializer<T> {
    private final Serializer serializer;

    public JDKCompatibleCollectionSerializer(Fury fury, Class<T> cls) {
      super(fury, cls, false, false);
      // Collection which defined `writeReplace` may use this serializer, so check replace/resolve
      // is necessary.
      Class<? extends Serializer> serializerType =
          ClassResolver.useReplaceResolveSerializer(cls)
              ? ReplaceResolveSerializer.class
              : fury.getDefaultJDKStreamSerializerType();
      serializer = Serializers.newSerializer(fury, cls, serializerType);
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
  }

  // TODO add JDK11:JdkImmutableListSerializer,JdkImmutableMapSerializer,JdkImmutableSetSerializer
  //  by jit codegen those constructor for compiling in jdk8.
  // TODO Support ArraySubListSerializer, SubListSerializer

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
    fury.registerSerializer(
        ConcurrentSkipListSet.class,
        new ConcurrentSkipListSetSerializer(fury, ConcurrentSkipListSet.class));
    fury.registerSerializer(Vector.class, new VectorSerializer(fury, Vector.class));
    fury.registerSerializer(ArrayDeque.class, new ArrayDequeSerializer(fury, ArrayDeque.class));
    fury.registerSerializer(BitSet.class, new BitSetSerializer(fury, BitSet.class));
    fury.registerSerializer(
        PriorityQueue.class, new PriorityQueueSerializer(fury, PriorityQueue.class));
  }
}
