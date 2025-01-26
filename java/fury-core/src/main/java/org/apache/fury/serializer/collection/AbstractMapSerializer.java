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

import static org.apache.fury.serializer.collection.MapFlags.KEY_DECL_TYPE;
import static org.apache.fury.serializer.collection.MapFlags.KEY_HAS_NULL;
import static org.apache.fury.serializer.collection.MapFlags.KV_NULL;
import static org.apache.fury.serializer.collection.MapFlags.NULL_KEY_VALUE_DECL_TYPE;
import static org.apache.fury.serializer.collection.MapFlags.NULL_KEY_VALUE_DECL_TYPE_TRACKING_REF;
import static org.apache.fury.serializer.collection.MapFlags.NULL_VALUE_KEY_DECL_TYPE;
import static org.apache.fury.serializer.collection.MapFlags.NULL_VALUE_KEY_DECL_TYPE_TRACKING_REF;
import static org.apache.fury.serializer.collection.MapFlags.TRACKING_KEY_REF;
import static org.apache.fury.serializer.collection.MapFlags.TRACKING_VALUE_REF;
import static org.apache.fury.serializer.collection.MapFlags.VALUE_DECL_TYPE;
import static org.apache.fury.serializer.collection.MapFlags.VALUE_HAS_NULL;
import static org.apache.fury.type.TypeUtils.MAP_TYPE;

import com.google.common.collect.ImmutableMap.Builder;
import java.lang.invoke.MethodHandle;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.fury.Fury;
import org.apache.fury.collection.IdentityMap;
import org.apache.fury.collection.Tuple2;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.reflect.TypeRef;
import org.apache.fury.resolver.ClassInfo;
import org.apache.fury.resolver.ClassInfoHolder;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.RefResolver;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.type.GenericType;
import org.apache.fury.type.Generics;
import org.apache.fury.type.TypeUtils;

/** Serializer for all map-like objects. */
@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class AbstractMapSerializer<T> extends Serializer<T> {
  private static final int MAX_CHUNK_SIZE = 255;

  protected MethodHandle constructor;
  protected final boolean supportCodegenHook;
  protected boolean useChunkSerialize;
  private Serializer keySerializer;
  private Serializer valueSerializer;
  protected final ClassInfoHolder keyClassInfoWriteCache;
  protected final ClassInfoHolder keyClassInfoReadCache;
  protected final ClassInfoHolder valueClassInfoWriteCache;
  protected final ClassInfoHolder valueClassInfoReadCache;
  // support map subclass whose key or value generics only are available,
  // or one of types is already instantiated in subclass, ex: `Subclass<T> implements Map<String,
  // T>`
  private final IdentityMap<GenericType, GenericType> partialGenericKVTypeMap;
  private final GenericType objType = fury.getClassResolver().buildGenericType(Object.class);
  // For subclass whose kv type are instantiated already, such as
  // `Subclass implements Map<String, Long>`. If declared `Map` doesn't specify
  // instantiated kv type, then the serialization will need to write those kv
  // types. Although we can extract this generics when creating the serializer,
  // we can't do it when jit `Serializer` for some class which contains one of such map
  // field. So we will write those extra kv classes to keep protocol consistency between
  // interpreter and jit mode although it seems unnecessary.
  // With kv header in future, we can write this kv classes only once, the cost won't be too much.
  private int numElements;
  private final ClassResolver classResolver;

  public AbstractMapSerializer(Fury fury, Class<T> cls) {
    this(fury, cls, !ReflectionUtils.isDynamicGeneratedCLass(cls));
  }

  public AbstractMapSerializer(Fury fury, Class<T> cls, boolean supportCodegenHook) {
    super(fury, cls);
    this.classResolver = fury.getClassResolver();
    this.supportCodegenHook = supportCodegenHook;
    keyClassInfoWriteCache = fury.getClassResolver().nilClassInfoHolder();
    keyClassInfoReadCache = fury.getClassResolver().nilClassInfoHolder();
    valueClassInfoWriteCache = fury.getClassResolver().nilClassInfoHolder();
    valueClassInfoReadCache = fury.getClassResolver().nilClassInfoHolder();
    partialGenericKVTypeMap = new IdentityMap<>();
  }

  public AbstractMapSerializer(
      Fury fury, Class<T> cls, boolean supportCodegenHook, boolean immutable) {
    super(fury, cls, immutable);
    this.classResolver = fury.getClassResolver();
    this.supportCodegenHook = supportCodegenHook;
    keyClassInfoWriteCache = fury.getClassResolver().nilClassInfoHolder();
    keyClassInfoReadCache = fury.getClassResolver().nilClassInfoHolder();
    valueClassInfoWriteCache = fury.getClassResolver().nilClassInfoHolder();
    valueClassInfoReadCache = fury.getClassResolver().nilClassInfoHolder();
    partialGenericKVTypeMap = new IdentityMap<>();
  }

  /**
   * Set key serializer for next serialization, the <code>serializer</code> will be cleared when
   * next serialization finished.
   */
  public void setKeySerializer(Serializer keySerializer) {
    this.keySerializer = keySerializer;
  }

  /**
   * Set value serializer for next serialization, the <code>serializer</code> will be cleared when
   * next serialization finished.
   */
  public void setValueSerializer(Serializer valueSerializer) {
    this.valueSerializer = valueSerializer;
  }

  @Override
  public void write(MemoryBuffer buffer, T value) {
    Map map = onMapWrite(buffer, value);
    if (useChunkSerialize) {
      chunkWriteElements(fury, buffer, map);
    } else {
      writeElements(fury, buffer, map);
    }
  }

  @Override
  public void xwrite(MemoryBuffer buffer, T value) {
    Map map = onMapWrite(buffer, value);
    xwriteElements(fury, buffer, map);
  }

  protected final void writeElements(Fury fury, MemoryBuffer buffer, Map map) {
    Serializer keySerializer = this.keySerializer;
    Serializer valueSerializer = this.valueSerializer;
    // clear the elemSerializer to avoid conflict if the nested
    // serialization has collection field.
    // TODO use generics for compatible serializer.
    this.keySerializer = null;
    this.valueSerializer = null;
    if (keySerializer != null && valueSerializer != null) {
      javaWriteWithKVSerializers(fury, buffer, map, keySerializer, valueSerializer);
    } else if (keySerializer != null) {
      ClassResolver classResolver = fury.getClassResolver();
      RefResolver refResolver = fury.getRefResolver();
      for (Object object : map.entrySet()) {
        Map.Entry entry = (Map.Entry) object;
        fury.writeRef(buffer, entry.getKey(), keySerializer);
        Object value = entry.getValue();
        writeJavaRefOptimized(
            fury, classResolver, refResolver, buffer, value, valueClassInfoWriteCache);
      }
    } else if (valueSerializer != null) {
      ClassResolver classResolver = fury.getClassResolver();
      RefResolver refResolver = fury.getRefResolver();
      for (Object object : map.entrySet()) {
        Map.Entry entry = (Map.Entry) object;
        Object key = entry.getKey();
        writeJavaRefOptimized(
            fury, classResolver, refResolver, buffer, key, keyClassInfoWriteCache);
        fury.writeRef(buffer, entry.getValue(), valueSerializer);
      }
    } else {
      genericJavaWrite(fury, buffer, map);
    }
  }

  protected final void chunkWriteElements(Fury fury, MemoryBuffer buffer, Map<Object, Object> map) {
    Serializer keySerializer = this.keySerializer;
    Serializer valueSerializer = this.valueSerializer;
    // clear the elemSerializer to avoid conflict if the nested
    // serialization has collection field.
    // TODO use generics for compatible serializer.
    this.keySerializer = null;
    this.valueSerializer = null;
    if (map.isEmpty()) {
      return;
    }
    ClassResolver classResolver = fury.getClassResolver();
    Iterator<Entry<Object, Object>> iterator = map.entrySet().iterator();
    Entry<Object, Object> entry = iterator.next();
    while (entry != null) {
      entry = writeJavaNullChunk(buffer, entry, iterator, keySerializer, valueSerializer);
      if (entry != null) {
        if (keySerializer != null || valueSerializer != null) {
          entry =
              writeJavaChunk(
                  classResolver, buffer, entry, iterator, keySerializer, valueSerializer);
        } else {
          Generics generics = fury.getGenerics();
          GenericType genericType = generics.nextGenericType();
          if (genericType == null) {
            entry = writeJavaChunk(classResolver, buffer, entry, iterator, null, null);
          } else {
            entry =
                writeJavaChunkGeneric(
                    classResolver, generics, genericType, buffer, entry, iterator);
          }
        }
      }
    }
  }

  public Entry writeJavaNullChunk(
      MemoryBuffer buffer,
      Entry entry,
      Iterator<Entry<Object, Object>> iterator,
      Serializer keySerializer,
      Serializer valueSerializer) {
    while (true) {
      Object key = entry.getKey();
      Object value = entry.getValue();
      if (key != null) {
        if (value != null) {
          return entry;
        }
        writeNullValueChunk(buffer, keySerializer, key);
      } else {
        writeNullKeyChunk(buffer, valueSerializer, value);
      }
      if (iterator.hasNext()) {
        entry = iterator.next();
      } else {
        return null;
      }
    }
  }

  private void writeNullValueChunk(MemoryBuffer buffer, Serializer keySerializer, Object key) {
    // noinspection Duplicates
    if (keySerializer != null) {
      if (keySerializer.needToWriteRef()) {
        buffer.writeByte(NULL_VALUE_KEY_DECL_TYPE_TRACKING_REF);
        fury.writeRef(buffer, key, keySerializer);
      } else {
        buffer.writeByte(NULL_VALUE_KEY_DECL_TYPE);
        keySerializer.write(buffer, key);
      }
    } else {
      buffer.writeByte(VALUE_HAS_NULL | TRACKING_KEY_REF);
      fury.writeRef(buffer, key, keyClassInfoWriteCache);
    }
  }

  /**
   * Write chunk of size 1, the key is null. Since we can have at most one key whose value is null,
   * this method is not in critical path, make it as a separate method to let caller eligible for
   * jit inline.
   */
  private void writeNullKeyChunk(MemoryBuffer buffer, Serializer valueSerializer, Object value) {
    if (value != null) {
      // noinspection Duplicates
      if (valueSerializer != null) {
        if (valueSerializer.needToWriteRef()) {
          buffer.writeByte(NULL_KEY_VALUE_DECL_TYPE_TRACKING_REF);
          fury.writeRef(buffer, value, valueSerializer);
        } else {
          buffer.writeByte(NULL_KEY_VALUE_DECL_TYPE);
          valueSerializer.write(buffer, value);
        }
      } else {
        buffer.writeByte(KEY_HAS_NULL | TRACKING_VALUE_REF);
        fury.writeRef(buffer, value, valueClassInfoWriteCache);
      }
    } else {
      buffer.writeByte(KV_NULL);
    }
  }

  // Make byte code of this method smaller than 325 for better jit inline
  private Entry writeJavaChunk(
      ClassResolver classResolver,
      MemoryBuffer buffer,
      Entry<Object, Object> entry,
      Iterator<Entry<Object, Object>> iterator,
      Serializer keySerializer,
      Serializer valueSerializer) {
    Object key = entry.getKey();
    Object value = entry.getValue();
    Class keyType = key.getClass();
    Class valueType = value.getClass();
    // place holder for chunk header and size.
    buffer.writeInt16((short) -1);
    int chunkSizeOffset = buffer.writerIndex() - 1;
    int chunkHeader = 0;
    if (keySerializer != null) {
      chunkHeader |= VALUE_DECL_TYPE;
    } else {
      keySerializer = writeKeyClassInfo(classResolver, keyType, buffer);
    }
    if (valueSerializer != null) {
      chunkHeader |= VALUE_DECL_TYPE;
    } else {
      valueSerializer = writeValueClassInfo(classResolver, valueType, buffer);
    }
    // noinspection Duplicates
    boolean keyWriteRef = keySerializer.needToWriteRef();
    boolean valueWriteRef = valueSerializer.needToWriteRef();
    if (keyWriteRef) {
      chunkHeader |= TRACKING_KEY_REF;
    }
    if (valueWriteRef) {
      chunkHeader |= TRACKING_VALUE_REF;
    }
    buffer.putByte(chunkSizeOffset - 1, (byte) chunkHeader);
    RefResolver refResolver = fury.getRefResolver();
    // Use int to make chunk size representable for 0~255 instead of 0~127.
    int chunkSize = 0;
    while (true) {
      if (key == null
          || value == null
          || (key.getClass() != keyType)
          || (value.getClass() != valueType)) {
        break;
      }
      if (!keyWriteRef || !refResolver.writeRefOrNull(buffer, key)) {
        keySerializer.write(buffer, key);
      }
      if (!valueWriteRef || !refResolver.writeRefOrNull(buffer, value)) {
        valueSerializer.write(buffer, value);
      }
      // noinspection Duplicates
      if (++chunkSize == MAX_CHUNK_SIZE) {
        break;
      }
      if (iterator.hasNext()) {
        entry = iterator.next();
        key = entry.getKey();
        value = entry.getValue();
      } else {
        entry = null;
        break;
      }
    }
    buffer.putByte(chunkSizeOffset, (byte) chunkSize);
    return entry;
  }

  private Serializer writeKeyClassInfo(
      ClassResolver classResolver, Class keyType, MemoryBuffer buffer) {
    ClassInfo classInfo = classResolver.getClassInfo(keyType, keyClassInfoWriteCache);
    classResolver.writeClass(buffer, classInfo);
    return classInfo.getSerializer();
  }

  private Serializer writeValueClassInfo(
      ClassResolver classResolver, Class valueType, MemoryBuffer buffer) {
    ClassInfo classInfo = classResolver.getClassInfo(valueType, valueClassInfoWriteCache);
    classResolver.writeClass(buffer, classInfo);
    return classInfo.getSerializer();
  }

  private Entry writeJavaChunkGeneric(
      ClassResolver classResolver,
      Generics generics,
      GenericType genericType,
      MemoryBuffer buffer,
      Entry<Object, Object> entry,
      Iterator<Entry<Object, Object>> iterator) {
    // type parameters count for `Map field` will be 0;
    // type parameters count for `SubMap<V> field` which SubMap is
    // `SubMap<V> implements Map<String, V>` will be 1;
    if (genericType.getTypeParametersCount() < 2) {
      genericType = getKVGenericType(genericType);
    }
    GenericType keyGenericType = genericType.getTypeParameter0();
    GenericType valueGenericType = genericType.getTypeParameter1();
    if (keyGenericType == objType && valueGenericType == objType) {
      return writeJavaChunk(classResolver, buffer, entry, iterator, null, null);
    }
    // Can't avoid push generics repeatedly in loop by stack depth, because push two
    // generic type changed generics stack top, which is depth index, update stack top
    // and depth will have some cost too.
    // Stack depth to avoid push generics repeatedly in loop.
    // Note push two generic type changed generics stack top, which is depth index,
    // stack top should be updated when using for serialization k/v.
    // int depth = fury.getDepth();
    // // depth + 1 to leave a slot for value generics, otherwise value generics will
    // // be overwritten by nested key generics.
    // fury.setDepth(depth + 1);
    // generics.pushGenericType(keyGenericType);
    // fury.setDepth(depth);
    // generics.pushGenericType(valueGenericType);
    boolean keyGenericTypeFinal = keyGenericType.isMonomorphic();
    boolean valueGenericTypeFinal = valueGenericType.isMonomorphic();
    Object key = entry.getKey();
    Object value = entry.getValue();
    Class keyType = key.getClass();
    Class valueType = value.getClass();
    Serializer keySerializer, valueSerializer;
    // place holder for chunk header and size.
    buffer.writeInt16((short) -1);
    int chunkSizeOffset = buffer.writerIndex() - 1;
    int chunkHeader = 0;
    // noinspection Duplicates
    if (keyGenericTypeFinal) {
      chunkHeader |= KEY_DECL_TYPE;
      keySerializer = keyGenericType.getSerializer(classResolver);
    } else {
      keySerializer = writeKeyClassInfo(classResolver, keyType, buffer);
    }
    if (valueGenericTypeFinal) {
      chunkHeader |= VALUE_DECL_TYPE;
      valueSerializer = valueGenericType.getSerializer(classResolver);
    } else {
      valueSerializer = writeValueClassInfo(classResolver, valueType, buffer);
    }
    boolean keyWriteRef = keySerializer.needToWriteRef();
    if (keyWriteRef) {
      chunkHeader |= TRACKING_KEY_REF;
    }
    boolean valueWriteRef = valueSerializer.needToWriteRef();
    if (valueWriteRef) {
      chunkHeader |= TRACKING_VALUE_REF;
    }
    buffer.putByte(chunkSizeOffset - 1, (byte) chunkHeader);
    RefResolver refResolver = fury.getRefResolver();
    // Use int to make chunk size representable for 0~255 instead of 0~127.
    int chunkSize = 0;
    while (true) {
      if (key == null
          || value == null
          || (key.getClass() != keyType)
          || (value.getClass() != valueType)) {
        break;
      }
      generics.pushGenericType(keyGenericType);
      if (!keyWriteRef || !refResolver.writeRefOrNull(buffer, key)) {
        keySerializer.write(buffer, key);
      }
      generics.popGenericType();
      generics.pushGenericType(valueGenericType);
      if (!valueWriteRef || !refResolver.writeRefOrNull(buffer, value)) {
        valueSerializer.write(buffer, value);
      }
      generics.popGenericType();
      // noinspection Duplicates
      if (++chunkSize == MAX_CHUNK_SIZE) {
        break;
      }
      if (iterator.hasNext()) {
        entry = iterator.next();
        key = entry.getKey();
        value = entry.getValue();
      } else {
        entry = null;
        break;
      }
    }
    buffer.putByte(chunkSizeOffset, (byte) chunkSize);
    return entry;
  }

  private void javaWriteWithKVSerializers(
      Fury fury,
      MemoryBuffer buffer,
      Map map,
      Serializer keySerializer,
      Serializer valueSerializer) {
    for (Object object : map.entrySet()) {
      Map.Entry entry = (Map.Entry) object;
      Object key = entry.getKey();
      Object value = entry.getValue();
      fury.writeRef(buffer, key, keySerializer);
      fury.writeRef(buffer, value, valueSerializer);
    }
  }

  private void genericJavaWrite(Fury fury, MemoryBuffer buffer, Map map) {
    Generics generics = fury.getGenerics();
    GenericType genericType = generics.nextGenericType();
    if (genericType == null) {
      generalJavaWrite(fury, buffer, map);
    } else {

      // type parameters count for `Map field` will be 0;
      // type parameters count for `SubMap<V> field` which SubMap is
      // `SubMap<V> implements Map<String, V>` will be 1;
      if (genericType.getTypeParametersCount() < 2) {
        genericType = getKVGenericType(genericType);
      }
      GenericType keyGenericType = genericType.getTypeParameter0();
      GenericType valueGenericType = genericType.getTypeParameter1();
      if (keyGenericType == objType && valueGenericType == objType) {
        generalJavaWrite(fury, buffer, map);
        return;
      }
      // Can't avoid push generics repeatedly in loop by stack depth, because push two
      // generic type changed generics stack top, which is depth index, update stack top
      // and depth will have some cost too.
      // Stack depth to avoid push generics repeatedly in loop.
      // Note push two generic type changed generics stack top, which is depth index,
      // stack top should be updated when using for serialization k/v.
      // int depth = fury.getDepth();
      // // depth + 1 to leave a slot for value generics, otherwise value generics will
      // // be overwritten by nested key generics.
      // fury.setDepth(depth + 1);
      // generics.pushGenericType(keyGenericType);
      // fury.setDepth(depth);
      // generics.pushGenericType(valueGenericType);
      boolean keyGenericTypeFinal = keyGenericType.isMonomorphic();
      boolean valueGenericTypeFinal = valueGenericType.isMonomorphic();
      if (keyGenericTypeFinal && valueGenericTypeFinal) {
        javaKVTypesFinalWrite(fury, buffer, map, keyGenericType, valueGenericType, generics);
      } else if (keyGenericTypeFinal) {
        javaKeyTypeFinalWrite(fury, buffer, map, keyGenericType, valueGenericType, generics);
      } else if (valueGenericTypeFinal) {
        javaValueTypeFinalWrite(fury, buffer, map, keyGenericType, valueGenericType, generics);
      } else {
        javaKVTypesNonFinalWrite(fury, buffer, map, keyGenericType, valueGenericType, generics);
      }
    }
  }

  private void javaKVTypesFinalWrite(
      Fury fury,
      MemoryBuffer buffer,
      Map map,
      GenericType keyGenericType,
      GenericType valueGenericType,
      Generics generics) {
    Serializer keySerializer = keyGenericType.getSerializer(fury.getClassResolver());
    Serializer valueSerializer = valueGenericType.getSerializer(fury.getClassResolver());
    for (Object object : map.entrySet()) {
      Map.Entry entry = (Map.Entry) object;
      generics.pushGenericType(keyGenericType);
      fury.writeRef(buffer, entry.getKey(), keySerializer);
      generics.popGenericType();
      generics.pushGenericType(valueGenericType);
      fury.writeRef(buffer, entry.getValue(), valueSerializer);
      generics.popGenericType();
    }
  }

  private void javaKeyTypeFinalWrite(
      Fury fury,
      MemoryBuffer buffer,
      Map map,
      GenericType keyGenericType,
      GenericType valueGenericType,
      Generics generics) {
    ClassResolver classResolver = fury.getClassResolver();
    RefResolver refResolver = fury.getRefResolver();
    boolean trackingValueRef = fury.getClassResolver().needToWriteRef(valueGenericType.getCls());
    Serializer keySerializer = keyGenericType.getSerializer(fury.getClassResolver());
    for (Object object : map.entrySet()) {
      Map.Entry entry = (Map.Entry) object;
      generics.pushGenericType(keyGenericType);
      fury.writeRef(buffer, entry.getKey(), keySerializer);
      generics.popGenericType();
      generics.pushGenericType(valueGenericType);
      writeJavaRefOptimized(
          fury,
          classResolver,
          refResolver,
          trackingValueRef,
          buffer,
          entry.getValue(),
          valueClassInfoWriteCache);
      generics.popGenericType();
    }
  }

  private void javaValueTypeFinalWrite(
      Fury fury,
      MemoryBuffer buffer,
      Map map,
      GenericType keyGenericType,
      GenericType valueGenericType,
      Generics generics) {
    ClassResolver classResolver = fury.getClassResolver();
    RefResolver refResolver = fury.getRefResolver();
    boolean trackingKeyRef = fury.getClassResolver().needToWriteRef(keyGenericType.getCls());
    Serializer valueSerializer = valueGenericType.getSerializer(fury.getClassResolver());
    for (Object object : map.entrySet()) {
      Map.Entry entry = (Map.Entry) object;
      generics.pushGenericType(keyGenericType);
      writeJavaRefOptimized(
          fury,
          classResolver,
          refResolver,
          trackingKeyRef,
          buffer,
          entry.getKey(),
          keyClassInfoWriteCache);
      generics.popGenericType();
      generics.pushGenericType(valueGenericType);
      fury.writeRef(buffer, entry.getValue(), valueSerializer);
      generics.popGenericType();
    }
  }

  private void javaKVTypesNonFinalWrite(
      Fury fury,
      MemoryBuffer buffer,
      Map map,
      GenericType keyGenericType,
      GenericType valueGenericType,
      Generics generics) {
    ClassResolver classResolver = fury.getClassResolver();
    RefResolver refResolver = fury.getRefResolver();
    boolean trackingKeyRef = fury.getClassResolver().needToWriteRef(keyGenericType.getCls());
    boolean trackingValueRef = fury.getClassResolver().needToWriteRef(valueGenericType.getCls());
    for (Object object : map.entrySet()) {
      Map.Entry entry = (Map.Entry) object;
      generics.pushGenericType(keyGenericType);
      writeJavaRefOptimized(
          fury,
          classResolver,
          refResolver,
          trackingKeyRef,
          buffer,
          entry.getKey(),
          keyClassInfoWriteCache);
      generics.popGenericType();
      generics.pushGenericType(valueGenericType);
      writeJavaRefOptimized(
          fury,
          classResolver,
          refResolver,
          trackingValueRef,
          buffer,
          entry.getValue(),
          valueClassInfoWriteCache);
      generics.popGenericType();
    }
  }

  private void generalJavaWrite(Fury fury, MemoryBuffer buffer, Map map) {
    ClassResolver classResolver = fury.getClassResolver();
    RefResolver refResolver = fury.getRefResolver();
    for (Object object : map.entrySet()) {
      Map.Entry entry = (Map.Entry) object;
      writeJavaRefOptimized(
          fury, classResolver, refResolver, buffer, entry.getKey(), keyClassInfoWriteCache);
      writeJavaRefOptimized(
          fury, classResolver, refResolver, buffer, entry.getValue(), valueClassInfoWriteCache);
    }
  }

  public static void xwriteElements(Fury fury, MemoryBuffer buffer, Map value) {
    Generics generics = fury.getGenerics();
    GenericType genericType = generics.nextGenericType();
    // TODO(chaokunyang) support map subclass whose key or value generics only are available.
    if (genericType == null || genericType.getTypeParametersCount() != 2) {
      for (Object object : value.entrySet()) {
        Map.Entry entry = (Map.Entry) object;
        fury.xwriteRef(buffer, entry.getKey());
        fury.xwriteRef(buffer, entry.getValue());
      }
    } else {
      // TODO(chaokunyang) use codegen to remove all branches.
      GenericType keyGenericType = genericType.getTypeParameter0();
      GenericType valueGenericType = genericType.getTypeParameter1();
      Serializer keySerializer = keyGenericType.getSerializer(fury.getClassResolver());
      Serializer valueSerializer = valueGenericType.getSerializer(fury.getClassResolver());
      if (!keyGenericType.hasGenericParameters() && !valueGenericType.hasGenericParameters()) {
        for (Object object : value.entrySet()) {
          Map.Entry entry = (Map.Entry) object;
          xwriteRefByNullableSerializer(fury, buffer, entry.getKey(), keySerializer);
          xwriteRefByNullableSerializer(fury, buffer, entry.getValue(), valueSerializer);
        }
      } else if (valueGenericType.hasGenericParameters()) {
        for (Object object : value.entrySet()) {
          Map.Entry entry = (Map.Entry) object;
          xwriteRefByNullableSerializer(fury, buffer, entry.getKey(), keySerializer);
          generics.pushGenericType(valueGenericType);
          xwriteRefByNullableSerializer(fury, buffer, entry.getValue(), valueSerializer);
          generics.popGenericType();
        }
      } else if (keyGenericType.hasGenericParameters()) {
        for (Object object : value.entrySet()) {
          Map.Entry entry = (Map.Entry) object;
          generics.pushGenericType(keyGenericType);
          xwriteRefByNullableSerializer(fury, buffer, entry.getKey(), keySerializer);
          generics.popGenericType();
          xwriteRefByNullableSerializer(fury, buffer, entry.getValue(), valueSerializer);
        }
      } else {
        for (Object object : value.entrySet()) {
          Map.Entry entry = (Map.Entry) object;
          generics.pushGenericType(keyGenericType);
          xwriteRefByNullableSerializer(fury, buffer, entry.getKey(), keySerializer);
          generics.pushGenericType(valueGenericType);
          xwriteRefByNullableSerializer(fury, buffer, entry.getValue(), valueSerializer);
        }
      }
      generics.popGenericType();
    }
  }

  public static <T> void xwriteRefByNullableSerializer(
      Fury fury, MemoryBuffer buffer, T obj, Serializer<T> serializer) {
    if (serializer == null) {
      fury.xwriteRef(buffer, obj);
    } else {
      fury.xwriteRef(buffer, obj, serializer);
    }
  }

  private GenericType getKVGenericType(GenericType genericType) {
    GenericType mapGenericType = partialGenericKVTypeMap.get(genericType);
    if (mapGenericType == null) {
      TypeRef<?> typeRef = genericType.getTypeRef();
      if (!MAP_TYPE.isSupertypeOf(typeRef)) {
        mapGenericType = GenericType.build(TypeUtils.mapOf(Object.class, Object.class));
        partialGenericKVTypeMap.put(genericType, mapGenericType);
        return mapGenericType;
      }
      Tuple2<TypeRef<?>, TypeRef<?>> mapKeyValueType = TypeUtils.getMapKeyValueType(typeRef);
      mapGenericType = GenericType.build(TypeUtils.mapOf(mapKeyValueType.f0, mapKeyValueType.f1));
      partialGenericKVTypeMap.put(genericType, mapGenericType);
    }
    return mapGenericType;
  }

  @Override
  public T xread(MemoryBuffer buffer) {
    Map map = newMap(buffer);
    xreadElements(fury, buffer, map, numElements);
    return onMapRead(map);
  }

  protected <K, V> void copyEntry(Map<K, V> originMap, Map<K, V> newMap) {
    ClassResolver classResolver = fury.getClassResolver();
    for (Map.Entry<K, V> entry : originMap.entrySet()) {
      K key = entry.getKey();
      if (key != null) {
        ClassInfo classInfo = classResolver.getClassInfo(key.getClass(), keyClassInfoWriteCache);
        if (!classInfo.getSerializer().isImmutable()) {
          key = fury.copyObject(key, classInfo.getClassId());
        }
      }
      V value = entry.getValue();
      if (value != null) {
        ClassInfo classInfo =
            classResolver.getClassInfo(value.getClass(), valueClassInfoWriteCache);
        if (!classInfo.getSerializer().isImmutable()) {
          value = fury.copyObject(value, classInfo.getClassId());
        }
      }
      newMap.put(key, value);
    }
  }

  protected <K, V> void copyEntry(Map<K, V> originMap, Builder<K, V> builder) {
    ClassResolver classResolver = fury.getClassResolver();
    for (Entry<K, V> entry : originMap.entrySet()) {
      K key = entry.getKey();
      if (key != null) {
        ClassInfo classInfo = classResolver.getClassInfo(key.getClass(), keyClassInfoWriteCache);
        if (!classInfo.getSerializer().isImmutable()) {
          key = fury.copyObject(key, classInfo.getClassId());
        }
      }
      V value = entry.getValue();
      if (value != null) {
        ClassInfo classInfo =
            classResolver.getClassInfo(value.getClass(), valueClassInfoWriteCache);
        if (!classInfo.getSerializer().isImmutable()) {
          value = fury.copyObject(value, classInfo.getClassId());
        }
      }
      builder.put(key, value);
    }
  }

  protected <K, V> void copyEntry(Map<K, V> originMap, Object[] elements) {
    ClassResolver classResolver = fury.getClassResolver();
    int index = 0;
    for (Entry<K, V> entry : originMap.entrySet()) {
      K key = entry.getKey();
      if (key != null) {
        ClassInfo classInfo = classResolver.getClassInfo(key.getClass(), keyClassInfoWriteCache);
        if (!classInfo.getSerializer().isImmutable()) {
          key = fury.copyObject(key, classInfo.getClassId());
        }
      }
      V value = entry.getValue();
      if (value != null) {
        ClassInfo classInfo =
            classResolver.getClassInfo(value.getClass(), valueClassInfoWriteCache);
        if (!classInfo.getSerializer().isImmutable()) {
          value = fury.copyObject(value, classInfo.getClassId());
        }
      }
      elements[index++] = key;
      elements[index++] = value;
    }
  }

  protected final void chunkReadElements(MemoryBuffer buffer, int size, Map map) {
    Serializer keySerializer = this.keySerializer;
    Serializer valueSerializer = this.valueSerializer;
    // clear the elemSerializer to avoid conflict if the nested
    // serialization has collection field.
    // TODO use generics for compatible serializer.
    this.keySerializer = null;
    this.valueSerializer = null;
    if (size == 0) {
      return;
    }

    int chunkHeader = buffer.readUnsignedByte();
    while (size > 0) {
      long sizeAndHeader =
          readJavaNullChunk(buffer, map, chunkHeader, size, keySerializer, valueSerializer);
      chunkHeader = (int) (sizeAndHeader & 0b11111111);
      size = (int) (sizeAndHeader >>> 8);
      if (keySerializer != null || valueSerializer != null) {
        sizeAndHeader =
            readJavaChunk(fury, buffer, map, size, chunkHeader, keySerializer, valueSerializer);
      } else {
        Generics generics = fury.getGenerics();
        GenericType genericType = generics.nextGenericType();
        if (genericType == null) {
          sizeAndHeader = readJavaChunk(fury, buffer, map, size, chunkHeader, null, null);
        } else {
          sizeAndHeader =
              readJavaChunkGeneric(fury, generics, genericType, buffer, map, size, chunkHeader);
        }
      }
      chunkHeader = (int) (sizeAndHeader & 0xff);
      size = (int) (sizeAndHeader >>> 8);
    }
  }

  public long readJavaNullChunk(
      MemoryBuffer buffer,
      Map map,
      int chunkHeader,
      long size,
      Serializer keySerializer,
      Serializer valueSerializer) {
    while (true) {
      boolean keyHasNull = (chunkHeader & KEY_HAS_NULL) != 0;
      boolean valueHasNull = (chunkHeader & VALUE_HAS_NULL) != 0;
      if (!keyHasNull) {
        if (!valueHasNull) {
          return (size << 8) | chunkHeader;
        } else {
          boolean trackKeyRef = (chunkHeader & TRACKING_KEY_REF) != 0;
          Object key;
          if ((chunkHeader & KEY_DECL_TYPE) != 0) {
            if (trackKeyRef) {
              key = fury.readRef(buffer, keySerializer);
            } else {
              key = keySerializer.read(buffer);
            }
          } else {
            key = fury.readRef(buffer, keyClassInfoReadCache);
          }
          map.put(key, null);
        }
      } else {
        readNullKeyChunk(buffer, map, chunkHeader, valueSerializer, valueHasNull);
      }
      if (size-- == 0) {
        return 0;
      } else {
        chunkHeader = buffer.readUnsignedByte();
      }
    }
  }

  /**
   * Read chunk of size 1, the key is null. Since we can have at most one key whose value is null,
   * this method is not in critical path, make it as a separate method to let caller eligible for
   * jit inline.
   */
  private void readNullKeyChunk(
      MemoryBuffer buffer,
      Map map,
      int chunkHeader,
      Serializer valueSerializer,
      boolean valueHasNull) {
    if (!valueHasNull) {
      Object value;
      boolean trackValueRef = (chunkHeader & TRACKING_VALUE_REF) != 0;
      if ((chunkHeader & VALUE_DECL_TYPE) != 0) {
        if (trackValueRef) {
          value = fury.readRef(buffer, valueSerializer);
        } else {
          value = valueSerializer.read(buffer);
        }
      } else {
        value = fury.readRef(buffer, valueClassInfoReadCache);
      }
      map.put(null, value);
    } else {
      map.put(null, null);
    }
  }

  private long readJavaChunk(
      Fury fury,
      MemoryBuffer buffer,
      Map map,
      long size,
      int chunkHeader,
      Serializer keySerializer,
      Serializer valueSerializer) {
    // noinspection Duplicates
    boolean trackKeyRef = (chunkHeader & TRACKING_KEY_REF) != 0;
    boolean trackValueRef = (chunkHeader & TRACKING_VALUE_REF) != 0;
    boolean keyIsDeclaredType = (chunkHeader & KEY_DECL_TYPE) != 0;
    boolean valueIsDeclaredType = (chunkHeader & VALUE_DECL_TYPE) != 0;
    int chunkSize = buffer.readUnsignedByte();
    if (!keyIsDeclaredType) {
      keySerializer = classResolver.readClassInfo(buffer, keyClassInfoReadCache).getSerializer();
    }
    if (!valueIsDeclaredType) {
      valueSerializer =
          classResolver.readClassInfo(buffer, valueClassInfoReadCache).getSerializer();
    }
    for (int i = 0; i < chunkSize; i++) {
      Object key = trackKeyRef ? fury.readRef(buffer, keySerializer) : keySerializer.read(buffer);
      Object value =
          trackValueRef ? fury.readRef(buffer, valueSerializer) : valueSerializer.read(buffer);
      map.put(key, value);
      size--;
    }
    return size > 0 ? (size << 8) | buffer.readUnsignedByte() : 0;
  }

  private long readJavaChunkGeneric(
      Fury fury,
      Generics generics,
      GenericType genericType,
      MemoryBuffer buffer,
      Map map,
      long size,
      int chunkHeader) {
    // type parameters count for `Map field` will be 0;
    // type parameters count for `SubMap<V> field` which SubMap is
    // `SubMap<V> implements Map<String, V>` will be 1;
    if (genericType.getTypeParametersCount() < 2) {
      genericType = getKVGenericType(genericType);
    }
    GenericType keyGenericType = genericType.getTypeParameter0();
    GenericType valueGenericType = genericType.getTypeParameter1();
    // noinspection Duplicates
    boolean trackKeyRef = (chunkHeader & TRACKING_KEY_REF) != 0;
    boolean trackValueRef = (chunkHeader & TRACKING_VALUE_REF) != 0;
    boolean keyIsDeclaredType = (chunkHeader & KEY_DECL_TYPE) != 0;
    boolean valueIsDeclaredType = (chunkHeader & VALUE_DECL_TYPE) != 0;
    int chunkSize = buffer.readUnsignedByte();
    Serializer keySerializer, valueSerializer;
    if (!keyIsDeclaredType) {
      keySerializer = classResolver.readClassInfo(buffer, keyClassInfoReadCache).getSerializer();
    } else {
      keySerializer = keyGenericType.getSerializer(classResolver);
    }
    if (!valueIsDeclaredType) {
      valueSerializer =
          classResolver.readClassInfo(buffer, valueClassInfoReadCache).getSerializer();
    } else {
      valueSerializer = valueGenericType.getSerializer(classResolver);
    }
    for (int i = 0; i < chunkSize; i++) {
      generics.pushGenericType(keyGenericType);
      Object key = trackKeyRef ? fury.readRef(buffer, keySerializer) : keySerializer.read(buffer);
      generics.popGenericType();
      generics.pushGenericType(valueGenericType);
      Object value =
          trackValueRef ? fury.readRef(buffer, valueSerializer) : valueSerializer.read(buffer);
      generics.popGenericType();
      map.put(key, value);
      size--;
    }
    return size > 0 ? (size << 8) | buffer.readUnsignedByte() : 0;
  }

  @SuppressWarnings("unchecked")
  protected final void readElements(MemoryBuffer buffer, int size, Map map) {
    Serializer keySerializer = this.keySerializer;
    Serializer valueSerializer = this.valueSerializer;
    // clear the elemSerializer to avoid conflict if the nested
    // serialization has collection field.
    // TODO use generics for compatible serializer.
    this.keySerializer = null;
    this.valueSerializer = null;
    if (keySerializer != null && valueSerializer != null) {
      for (int i = 0; i < size; i++) {
        Object key = fury.readRef(buffer, keySerializer);
        Object value = fury.readRef(buffer, valueSerializer);
        map.put(key, value);
      }
    } else if (keySerializer != null) {
      for (int i = 0; i < size; i++) {
        Object key = fury.readRef(buffer, keySerializer);
        map.put(key, fury.readRef(buffer, keyClassInfoReadCache));
      }
    } else if (valueSerializer != null) {
      for (int i = 0; i < size; i++) {
        Object key = fury.readRef(buffer);
        Object value = fury.readRef(buffer, valueSerializer);
        map.put(key, value);
      }
    } else {
      genericJavaRead(fury, buffer, map, size);
    }
  }

  private void genericJavaRead(Fury fury, MemoryBuffer buffer, Map map, int size) {
    Generics generics = fury.getGenerics();
    GenericType genericType = generics.nextGenericType();
    if (genericType == null) {
      generalJavaRead(fury, buffer, map, size);
    } else {
      if (genericType.getTypeParametersCount() < 2) {
        genericType = getKVGenericType(genericType);
      }
      GenericType keyGenericType = genericType.getTypeParameter0();
      GenericType valueGenericType = genericType.getTypeParameter1();
      if (keyGenericType == objType && valueGenericType == objType) {
        generalJavaRead(fury, buffer, map, size);
        return;
      }
      boolean keyGenericTypeFinal = keyGenericType.isMonomorphic();
      boolean valueGenericTypeFinal = valueGenericType.isMonomorphic();
      if (keyGenericTypeFinal && valueGenericTypeFinal) {
        javaKVTypesFinalRead(fury, buffer, map, keyGenericType, valueGenericType, generics, size);
      } else if (keyGenericTypeFinal) {
        javaKeyTypeFinalRead(fury, buffer, map, keyGenericType, valueGenericType, generics, size);
      } else if (valueGenericTypeFinal) {
        javaValueTypeFinalRead(fury, buffer, map, keyGenericType, valueGenericType, generics, size);
      } else {
        javaKVTypesNonFinalRead(
            fury, buffer, map, keyGenericType, valueGenericType, generics, size);
      }
      generics.popGenericType();
    }
  }

  private void javaKVTypesFinalRead(
      Fury fury,
      MemoryBuffer buffer,
      Map map,
      GenericType keyGenericType,
      GenericType valueGenericType,
      Generics generics,
      int size) {
    Serializer keySerializer = keyGenericType.getSerializer(fury.getClassResolver());
    Serializer valueSerializer = valueGenericType.getSerializer(fury.getClassResolver());
    for (int i = 0; i < size; i++) {
      generics.pushGenericType(keyGenericType);
      Object key = fury.readRef(buffer, keySerializer);
      generics.popGenericType();
      generics.pushGenericType(valueGenericType);
      Object value = fury.readRef(buffer, valueSerializer);
      generics.popGenericType();
      map.put(key, value);
    }
  }

  private void javaKeyTypeFinalRead(
      Fury fury,
      MemoryBuffer buffer,
      Map map,
      GenericType keyGenericType,
      GenericType valueGenericType,
      Generics generics,
      int size) {
    RefResolver refResolver = fury.getRefResolver();
    boolean trackingValueRef = fury.getClassResolver().needToWriteRef(valueGenericType.getCls());
    Serializer keySerializer = keyGenericType.getSerializer(fury.getClassResolver());
    for (int i = 0; i < size; i++) {
      generics.pushGenericType(keyGenericType);
      Object key = fury.readRef(buffer, keySerializer);
      generics.popGenericType();
      generics.pushGenericType(valueGenericType);
      Object value =
          readJavaRefOptimized(
              fury, refResolver, trackingValueRef, buffer, valueClassInfoWriteCache);
      generics.popGenericType();
      map.put(key, value);
    }
  }

  private void javaValueTypeFinalRead(
      Fury fury,
      MemoryBuffer buffer,
      Map map,
      GenericType keyGenericType,
      GenericType valueGenericType,
      Generics generics,
      int size) {
    boolean trackingKeyRef = fury.getClassResolver().needToWriteRef(keyGenericType.getCls());
    Serializer valueSerializer = valueGenericType.getSerializer(fury.getClassResolver());
    RefResolver refResolver = fury.getRefResolver();
    for (int i = 0; i < size; i++) {
      generics.pushGenericType(keyGenericType);
      Object key =
          readJavaRefOptimized(fury, refResolver, trackingKeyRef, buffer, keyClassInfoWriteCache);
      generics.popGenericType();
      generics.pushGenericType(valueGenericType);
      Object value = fury.readRef(buffer, valueSerializer);
      generics.popGenericType();
      map.put(key, value);
    }
  }

  private void javaKVTypesNonFinalRead(
      Fury fury,
      MemoryBuffer buffer,
      Map map,
      GenericType keyGenericType,
      GenericType valueGenericType,
      Generics generics,
      int size) {
    ClassResolver classResolver = fury.getClassResolver();
    RefResolver refResolver = fury.getRefResolver();
    boolean trackingKeyRef = classResolver.needToWriteRef(keyGenericType.getCls());
    boolean trackingValueRef = classResolver.needToWriteRef(valueGenericType.getCls());
    for (int i = 0; i < size; i++) {
      generics.pushGenericType(keyGenericType);
      Object key =
          readJavaRefOptimized(fury, refResolver, trackingKeyRef, buffer, keyClassInfoWriteCache);
      generics.popGenericType();
      generics.pushGenericType(valueGenericType);
      Object value =
          readJavaRefOptimized(
              fury, refResolver, trackingValueRef, buffer, valueClassInfoWriteCache);
      generics.popGenericType();
      map.put(key, value);
    }
  }

  private void generalJavaRead(Fury fury, MemoryBuffer buffer, Map map, int size) {
    for (int i = 0; i < size; i++) {
      Object key = fury.readRef(buffer, keyClassInfoReadCache);
      Object value = fury.readRef(buffer, valueClassInfoReadCache);
      map.put(key, value);
    }
  }

  @SuppressWarnings("unchecked")
  public static void xreadElements(Fury fury, MemoryBuffer buffer, Map map, int size) {
    Generics generics = fury.getGenerics();
    GenericType genericType = generics.nextGenericType();
    if (genericType == null || genericType.getTypeParametersCount() != 2) {
      for (int i = 0; i < size; i++) {
        Object key = fury.xreadRef(buffer);
        Object value = fury.xreadRef(buffer);
        map.put(key, value);
      }
    } else {
      // TODO(chaokunyang) use codegen to remove all branches.
      GenericType keyGenericType = genericType.getTypeParameter0();
      GenericType valueGenericType = genericType.getTypeParameter1();
      Serializer keySerializer = keyGenericType.getSerializer(fury.getClassResolver());
      Serializer valueSerializer = valueGenericType.getSerializer(fury.getClassResolver());
      if (!keyGenericType.hasGenericParameters() && !valueGenericType.hasGenericParameters()) {
        for (int i = 0; i < size; i++) {
          Object key = xreadRefByNullableSerializer(fury, buffer, keySerializer);
          Object value = xreadRefByNullableSerializer(fury, buffer, valueSerializer);
          map.put(key, value);
        }
      } else if (valueGenericType.hasGenericParameters()) {
        for (int i = 0; i < size; i++) {
          Object key = xreadRefByNullableSerializer(fury, buffer, keySerializer);
          generics.pushGenericType(valueGenericType);
          Object value = xreadRefByNullableSerializer(fury, buffer, valueSerializer);
          generics.popGenericType();
          map.put(key, value);
        }
      } else if (keyGenericType.hasGenericParameters()) {
        for (int i = 0; i < size; i++) {
          generics.pushGenericType(keyGenericType);
          Object key = xreadRefByNullableSerializer(fury, buffer, keySerializer);
          generics.popGenericType();
          Object value = xreadRefByNullableSerializer(fury, buffer, valueSerializer);
          map.put(key, value);
        }
      } else {
        for (int i = 0; i < size; i++) {
          // FIXME(chaokunyang) nested generics may be get by mistake.
          generics.pushGenericType(keyGenericType);
          Object key = xreadRefByNullableSerializer(fury, buffer, keySerializer);
          generics.pushGenericType(valueGenericType);
          Object value = xreadRefByNullableSerializer(fury, buffer, valueSerializer);
          map.put(key, value);
        }
      }
      generics.popGenericType();
    }
  }

  public static Object xreadRefByNullableSerializer(
      Fury fury, MemoryBuffer buffer, Serializer<?> serializer) {
    if (serializer == null) {
      return fury.xreadRef(buffer);
    } else {
      return fury.xreadRef(buffer, serializer);
    }
  }

  /**
   * Hook for java serialization codegen, read/write key/value by entrySet.
   *
   * <p>For key/value type which is final, using codegen may get a big performance gain
   *
   * @return true if read/write key/value support calling entrySet method
   */
  public final boolean supportCodegenHook() {
    return supportCodegenHook;
  }

  public boolean isUseChunkSerialize() {
    return useChunkSerialize;
  }

  public void setUseChunkSerialize(boolean useChunkSerialize) {
    this.useChunkSerialize = useChunkSerialize;
  }

  /**
   * Write data except size and elements.
   *
   * <ol>
   *   In codegen, follows is call order:
   *   <li>write map class if not final
   *   <li>write map size
   *   <li>onCollectionWrite
   *   <li>write keys/values
   * </ol>
   */
  public abstract Map onMapWrite(MemoryBuffer buffer, T value);

  /** Check null first to avoid ref tracking for some types with ref tracking disabled. */
  private void writeJavaRefOptimized(
      Fury fury,
      ClassResolver classResolver,
      RefResolver refResolver,
      MemoryBuffer buffer,
      Object obj,
      ClassInfoHolder classInfoHolder) {
    if (!refResolver.writeNullFlag(buffer, obj)) {
      fury.writeRef(buffer, obj, classResolver.getClassInfo(obj.getClass(), classInfoHolder));
    }
  }

  private void writeJavaRefOptimized(
      Fury fury,
      ClassResolver classResolver,
      RefResolver refResolver,
      boolean trackingRef,
      MemoryBuffer buffer,
      Object obj,
      ClassInfoHolder classInfoHolder) {
    if (trackingRef) {
      if (!refResolver.writeNullFlag(buffer, obj)) {
        fury.writeRef(buffer, obj, classResolver.getClassInfo(obj.getClass(), classInfoHolder));
      }
    } else {
      if (obj == null) {
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
        fury.writeNonRef(buffer, obj, classResolver.getClassInfo(obj.getClass(), classInfoHolder));
      }
    }
  }

  @Override
  public abstract T read(MemoryBuffer buffer);

  /**
   * Read data except size and elements, return empty map to be filled.
   *
   * <ol>
   *   In codegen, follows is call order:
   *   <li>read map class if not final
   *   <li>newMap: read and set map size, read map header and create map.
   *   <li>read keys/values
   * </ol>
   *
   * <p>Map must have default constructor to be invoked by fury, otherwise created object can't be
   * used to adding elements. For example:
   *
   * <pre>{@code new ArrayList<Integer> {add(1);}}</pre>
   *
   * <p>without default constructor, created list will have elementData as null, adding elements
   * will raise NPE.
   */
  public Map newMap(MemoryBuffer buffer) {
    numElements = buffer.readVarUint32Small7();
    if (constructor == null) {
      constructor = ReflectionUtils.getCtrHandle(type, true);
    }
    try {
      Map instance = (Map) constructor.invoke();
      fury.getRefResolver().reference(instance);
      return instance;
    } catch (Throwable e) {
      throw new IllegalArgumentException(
          "Please provide public no arguments constructor for class " + type, e);
    }
  }

  /** Create a new empty map for copy. */
  public Map newMap(Map map) {
    numElements = map.size();
    if (constructor == null) {
      constructor = ReflectionUtils.getCtrHandle(type, true);
    }
    try {
      return (Map) constructor.invoke();
    } catch (Throwable e) {
      throw new IllegalArgumentException(
          "Please provide public no arguments constructor for class " + type, e);
    }
  }

  /**
   * Get and reset numElements of deserializing collection. Should be called after {@link
   * #newMap(MemoryBuffer buffer)}. Nested read may overwrite this element, reset is necessary to
   * avoid use wrong value by mistake.
   */
  public int getAndClearNumElements() {
    int size = numElements;
    numElements = -1; // nested read may overwrite this element.
    return size;
  }

  public void setNumElements(int numElements) {
    this.numElements = numElements;
  }

  public abstract T onMapCopy(Map map);

  public abstract T onMapRead(Map map);

  private Object readJavaRefOptimized(
      Fury fury,
      RefResolver refResolver,
      boolean trackingRef,
      MemoryBuffer buffer,
      ClassInfoHolder classInfoHolder) {
    if (trackingRef) {
      int nextReadRefId = refResolver.tryPreserveRefId(buffer);
      if (nextReadRefId >= Fury.NOT_NULL_VALUE_FLAG) {
        Object obj = fury.readNonRef(buffer, classInfoHolder);
        refResolver.setReadObject(nextReadRefId, obj);
        return obj;
      } else {
        return refResolver.getReadObject();
      }
    } else {
      byte headFlag = buffer.readByte();
      if (headFlag == Fury.NULL_FLAG) {
        return null;
      } else {
        return fury.readNonRef(buffer, classInfoHolder);
      }
    }
  }
}
