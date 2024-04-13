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
import java.util.Collection;
import org.apache.fury.Fury;
import org.apache.fury.annotation.CodegenInvoke;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.resolver.ClassInfo;
import org.apache.fury.resolver.ClassInfoHolder;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.RefResolver;
import org.apache.fury.serializer.CompatibleSerializer;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.type.GenericType;
import org.apache.fury.util.Preconditions;
import org.apache.fury.util.ReflectionUtils;

/**
 * Serializer for all collection like object. All collection serializer should extend this class.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class AbstractCollectionSerializer<T> extends Serializer<T> {
  private MethodHandle constructor;
  private int numElements;
  private final boolean supportCodegenHook;
  // TODO remove elemSerializer, support generics in CompatibleSerializer.
  private Serializer<?> elemSerializer;
  protected final ClassInfoHolder elementClassInfoHolder;

  // For subclass whose element type are instantiated already, such as
  // `Subclass extends ArrayList<String>`. If declared `Collection` doesn't specify
  // instantiated element type, then the serialization will need to write this element
  // type. Although we can extract this generics when creating the serializer,
  // we can't do it when jit `Serializer` for some class which contains one of such collection
  // field. So we will write this extra element class to keep protocol consistency between
  // interpreter and jit mode although it seems unnecessary.
  // With elements header, we can write this element class only once, the cost won't be too much.

  public AbstractCollectionSerializer(Fury fury, Class<T> cls) {
    this(fury, cls, !ReflectionUtils.isDynamicGeneratedCLass(cls));
  }

  public AbstractCollectionSerializer(Fury fury, Class<T> cls, boolean supportCodegenHook) {
    super(fury, cls);
    this.supportCodegenHook = supportCodegenHook;
    elementClassInfoHolder = fury.getClassResolver().nilClassInfoHolder();
  }

  private GenericType getElementGenericType(Fury fury) {
    GenericType genericType = fury.getGenerics().nextGenericType();
    GenericType elemGenericType = null;
    if (genericType != null) {
      elemGenericType = genericType.getTypeParameter0();
    }
    return elemGenericType;
  }

  /**
   * Set element serializer for next serialization, the <code>serializer</code> will be cleared when
   * next serialization finished.
   */
  public void setElementSerializer(Serializer serializer) {
    elemSerializer = serializer;
  }

  /**
   * Hook for java serialization codegen, read/write elements will call collection.get/add methods.
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
   *   <li>onCollectionWrite
   *   <li>write elements
   * </ol>
   */
  public abstract Collection onCollectionWrite(MemoryBuffer buffer, T value);

  /**
   * Write elements data header. Keep this consistent with
   * `BaseObjectCodecBuilder#writeElementsHeader`.
   *
   * @return a bitmap, higher 24 bits are reserved.
   */
  protected final int writeElementsHeader(MemoryBuffer buffer, Collection value) {
    GenericType elemGenericType = getElementGenericType(fury);
    if (elemGenericType != null) {
      boolean trackingRef = elemGenericType.trackingRef(fury.getClassResolver());
      if (elemGenericType.isMonomorphic()) {
        if (trackingRef) {
          buffer.writeByte(CollectionFlags.TRACKING_REF);
          return CollectionFlags.TRACKING_REF;
        } else {
          return writeNullabilityHeader(buffer, value);
        }
      } else {
        if (trackingRef) {
          return writeTypeHeader(buffer, value, elemGenericType.getCls(), elementClassInfoHolder);
        } else {
          return writeTypeNullabilityHeader(
              buffer, value, elemGenericType.getCls(), elementClassInfoHolder);
        }
      }
    } else {
      if (elemSerializer != null) {
        if (elemSerializer.needToWriteRef()) {
          buffer.writeByte(CollectionFlags.TRACKING_REF);
          return CollectionFlags.TRACKING_REF;
        } else {
          return writeNullabilityHeader(buffer, value);
        }
      } else {
        if (fury.trackingRef()) {
          return writeTypeHeader(buffer, value, elementClassInfoHolder);
        } else {
          return writeTypeNullabilityHeader(buffer, value, null, elementClassInfoHolder);
        }
      }
    }
  }

  /** Element type is final, write whether any elements is null. */
  @CodegenInvoke
  public int writeNullabilityHeader(MemoryBuffer buffer, Collection value) {
    for (Object elem : value) {
      if (elem == null) {
        buffer.writeByte(CollectionFlags.HAS_NULL);
        return CollectionFlags.HAS_NULL;
      }
    }
    buffer.writeByte(0);
    return 0;
  }

  /** Need to track elements ref, can't check elements nullability. */
  @CodegenInvoke
  public int writeTypeHeader(
      MemoryBuffer buffer, Collection value, Class<?> declareElementType, ClassInfoHolder cache) {
    int bitmap = CollectionFlags.TRACKING_REF;
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
      bitmap |= CollectionFlags.NOT_SAME_TYPE | CollectionFlags.NOT_DECL_ELEMENT_TYPE;
      buffer.writeByte(bitmap);
    } else {
      // Write class in case peer doesn't have this class.
      if (!fury.getConfig().shareMetaContext() && elemClass == declareElementType) {
        buffer.writeByte(bitmap);
      } else {
        bitmap |= CollectionFlags.NOT_DECL_ELEMENT_TYPE;
        buffer.writeByte(bitmap);
        // Update classinfo, the caller will use it.
        ClassResolver classResolver = fury.getClassResolver();
        ClassInfo classInfo = classResolver.getClassInfo(elemClass, cache);
        classResolver.writeClass(buffer, classInfo);
      }
    }
    return bitmap;
  }

  /** Maybe track elements ref, or write elements nullability. */
  @CodegenInvoke
  public int writeTypeHeader(MemoryBuffer buffer, Collection value, ClassInfoHolder cache) {
    int bitmap = CollectionFlags.NOT_DECL_ELEMENT_TYPE;
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
      bitmap |= CollectionFlags.HAS_NULL;
    }
    if (hasDifferentClass) {
      bitmap |= CollectionFlags.NOT_SAME_TYPE | CollectionFlags.TRACKING_REF;
      buffer.writeByte(bitmap);
    } else {
      ClassResolver classResolver = fury.getClassResolver();
      // When serialize a collection with all elements null directly, the declare type
      // will be equal to element type: null
      if (elemClass == null) {
        elemClass = Object.class;
      }
      ClassInfo classInfo = classResolver.getClassInfo(elemClass, cache);
      if (classInfo.getSerializer().needToWriteRef()) {
        bitmap |= CollectionFlags.TRACKING_REF;
      }
      buffer.writeByte(bitmap);
      classResolver.writeClass(buffer, classInfo);
    }
    return bitmap;
  }

  /**
   * Element type is not final by {@link ClassResolver#isMonomorphic}, need to write element type.
   * Elements ref tracking is disabled, write whether any elements is null.
   */
  @CodegenInvoke
  public int writeTypeNullabilityHeader(
      MemoryBuffer buffer, Collection value, Class<?> declareElementType, ClassInfoHolder cache) {
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
      bitmap |= CollectionFlags.HAS_NULL;
    }
    if (hasDifferentClass) {
      // If collection contains null only, the type header will be meaningless
      bitmap |= CollectionFlags.NOT_SAME_TYPE | CollectionFlags.NOT_DECL_ELEMENT_TYPE;
      buffer.writeByte(bitmap);
    } else {
      // When serialize a collection with all elements null directly, the declare type
      // will be equal to element type: null
      if (elemClass == null) {
        elemClass = Object.class;
      }
      // Write class in case peer doesn't have this class.
      if (!fury.getConfig().shareMetaContext() && elemClass == declareElementType) {
        buffer.writeByte(bitmap);
      } else {
        bitmap |= CollectionFlags.NOT_DECL_ELEMENT_TYPE;
        buffer.writeByte(bitmap);
        ClassResolver classResolver = fury.getClassResolver();
        ClassInfo classInfo = classResolver.getClassInfo(elemClass, cache);
        classResolver.writeClass(buffer, classInfo);
      }
    }
    return bitmap;
  }

  @Override
  public void write(MemoryBuffer buffer, T value) {
    Collection collection = onCollectionWrite(buffer, value);
    int len = collection.size();
    if (len != 0) {
      writeElements(fury, buffer, collection);
    }
  }

  protected final void writeElements(Fury fury, MemoryBuffer buffer, Collection value) {
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
        generalJavaWrite(fury, buffer, value, null, flags);
      }
    } else {
      compatibleWrite(fury, buffer, value, serializer, flags);
    }
  }

  // TODO use generics for compatible serializer.
  private static <T extends Collection> void compatibleWrite(
      Fury fury, MemoryBuffer buffer, T value, Serializer serializer, int flags) {
    if (serializer.needToWriteRef()) {
      for (Object elem : value) {
        fury.writeRef(buffer, elem, serializer);
      }
    } else {
      boolean hasNull = (flags & CollectionFlags.HAS_NULL) == CollectionFlags.HAS_NULL;
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
      Fury fury,
      MemoryBuffer buffer,
      Collection collection,
      GenericType elemGenericType,
      int flags) {
    boolean hasGenericParameters = elemGenericType.hasGenericParameters();
    if (hasGenericParameters) {
      fury.getGenerics().pushGenericType(elemGenericType);
    }
    // Note: ObjectSerializer should mark `FinalElemType` in `Collection<FinalElemType>`
    // as non-final to write class def when meta share is enabled.
    if (elemGenericType.isMonomorphic()) {
      Serializer serializer = elemGenericType.getSerializer(fury.getClassResolver());
      writeSameTypeElements(fury, buffer, serializer, flags, collection);
    } else {
      generalJavaWrite(fury, buffer, collection, elemGenericType, flags);
    }
    if (hasGenericParameters) {
      fury.getGenerics().popGenericType();
    }
  }

  private void generalJavaWrite(
      Fury fury,
      MemoryBuffer buffer,
      Collection collection,
      GenericType elemGenericType,
      int flags) {
    if ((flags & CollectionFlags.NOT_SAME_TYPE) != CollectionFlags.NOT_SAME_TYPE) {
      Serializer serializer;
      if ((flags & CollectionFlags.NOT_DECL_ELEMENT_TYPE)
          != CollectionFlags.NOT_DECL_ELEMENT_TYPE) {
        Preconditions.checkNotNull(elemGenericType);
        serializer = elemGenericType.getSerializer(fury.getClassResolver());
      } else {
        serializer = elementClassInfoHolder.getSerializer();
      }
      writeSameTypeElements(fury, buffer, serializer, flags, collection);
    } else {
      writeDifferentTypeElements(fury, buffer, flags, collection);
    }
  }

  private static <T extends Collection> void writeSameTypeElements(
      Fury fury, MemoryBuffer buffer, Serializer serializer, int flags, T collection) {
    fury.incDepth(1);
    if ((flags & CollectionFlags.TRACKING_REF) == CollectionFlags.TRACKING_REF) {
      RefResolver refResolver = fury.getRefResolver();
      for (Object elem : collection) {
        if (!refResolver.writeRefOrNull(buffer, elem)) {
          serializer.write(buffer, elem);
        }
      }
    } else {
      if ((flags & CollectionFlags.HAS_NULL) != CollectionFlags.HAS_NULL) {
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
    if ((flags & CollectionFlags.TRACKING_REF) == CollectionFlags.TRACKING_REF) {
      for (Object elem : collection) {
        fury.writeRef(buffer, elem);
      }
    } else {
      if ((flags & CollectionFlags.HAS_NULL) != CollectionFlags.HAS_NULL) {
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
    Collection collection = (Collection) value;
    int len = collection.size();
    buffer.writeVarUint32Small7(len);
    xwriteElements(fury, buffer, collection);
  }

  private void xwriteElements(Fury fury, MemoryBuffer buffer, Collection value) {
    GenericType elemGenericType = getElementGenericType(fury);
    if (elemGenericType != null) {
      boolean hasGenericParameters = elemGenericType.hasGenericParameters();
      if (hasGenericParameters) {
        fury.getGenerics().pushGenericType(elemGenericType);
      }
      if (elemGenericType.isMonomorphic()) {
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
  public abstract T read(MemoryBuffer buffer);

  /**
   * Read data except size and elements, return empty collection to be filled.
   *
   * <ol>
   *   In codegen, follows is call order:
   *   <li>read collection class if not final
   *   <li>newCollection: read and set collection size, read collection header and create
   *       collection.
   *   <li>read elements
   * </ol>
   *
   * <p>Collection must have default constructor to be invoked by fury, otherwise created object
   * can't be used to adding elements. For example:
   *
   * <pre>{@code new ArrayList<Integer> {add(1);}}</pre>
   *
   * <p>without default constructor, created list will have elementData as null, adding elements
   * will raise NPE.
   */
  public Collection newCollection(MemoryBuffer buffer) {
    numElements = buffer.readVarUint32Small7();
    if (constructor == null) {
      constructor = ReflectionUtils.getCtrHandle(type, true);
    }
    try {
      T instance = (T) constructor.invoke();
      fury.getRefResolver().reference(instance);
      return (Collection) instance;
    } catch (Throwable e) {
      // reduce code size of critical path.
      throw buildException(e);
    }
  }

  private RuntimeException buildException(Throwable e) {
    return new IllegalArgumentException(
        "Please provide public no arguments constructor for class " + type, e);
  }

  /**
   * Get and reset numElements of deserializing collection. Should be called after {@link
   * #newCollection}. Nested read may overwrite this element, reset is necessary to avoid use wrong
   * value by mistake.
   */
  public int getAndClearNumElements() {
    int size = numElements;
    numElements = -1; // nested read may overwrite this element.
    return size;
  }

  protected void setNumElements(int numElements) {
    this.numElements = numElements;
  }

  public abstract T onCollectionRead(Collection collection);

  protected void readElements(
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
    if (serializer.needToWriteRef()) {
      for (int i = 0; i < numElements; i++) {
        collection.add(fury.readRef(buffer, serializer));
      }
    } else {
      if ((flags & CollectionFlags.HAS_NULL) == CollectionFlags.HAS_NULL) {
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
    if (elemGenericType.isMonomorphic()) {
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
    if ((flags & CollectionFlags.NOT_SAME_TYPE) != CollectionFlags.NOT_SAME_TYPE) {
      Serializer serializer;
      ClassResolver classResolver = fury.getClassResolver();
      if ((flags & CollectionFlags.NOT_DECL_ELEMENT_TYPE)
          == CollectionFlags.NOT_DECL_ELEMENT_TYPE) {
        serializer = classResolver.readClassInfo(buffer, elementClassInfoHolder).getSerializer();
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
    if ((flags & CollectionFlags.TRACKING_REF) == CollectionFlags.TRACKING_REF) {
      for (int i = 0; i < numElements; i++) {
        collection.add(fury.readRef(buffer, serializer));
      }
    } else {
      if ((flags & CollectionFlags.HAS_NULL) != CollectionFlags.HAS_NULL) {
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
    if ((flags & CollectionFlags.TRACKING_REF) == CollectionFlags.TRACKING_REF) {
      for (int i = 0; i < numElements; i++) {
        collection.add(fury.readRef(buffer));
      }
    } else {
      if ((flags & CollectionFlags.HAS_NULL) != CollectionFlags.HAS_NULL) {
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
    Collection collection = newCollection(buffer);
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
      if (elemGenericType.isMonomorphic()) {
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
