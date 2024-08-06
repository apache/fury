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

import static org.apache.fury.type.TypeUtils.MAP_TYPE;

import java.lang.invoke.MethodHandle;
import java.util.Map;
import org.apache.fury.Fury;
import org.apache.fury.collection.IdentityMap;
import org.apache.fury.collection.Tuple2;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.reflect.TypeRef;
import org.apache.fury.resolver.ClassInfoHolder;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.RefResolver;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.type.GenericType;
import org.apache.fury.type.Generics;
import org.apache.fury.type.TypeUtils;
import org.apache.fury.util.Preconditions;

/** Serializer for all map-like objects. */
@SuppressWarnings({"unchecked", "rawtypes"})
public abstract class AbstractMapSerializer<T> extends Serializer<T> {
  protected MethodHandle constructor;
  protected final boolean supportCodegenHook;
  private Serializer keySerializer;
  private Serializer valueSerializer;
  protected final ClassInfoHolder keyClassInfoWriteCache;
  protected final ClassInfoHolder keyClassInfoReadCache;
  protected final ClassInfoHolder valueClassInfoWriteCache;
  protected final ClassInfoHolder valueClassInfoReadCache;
  // support map subclass whose key or value generics only are available,
  // or one of types is already instantiated in subclass, ex: `Subclass<T> implements Map<String,
  // T>`
  private final IdentityMap<GenericType, Tuple2<GenericType, GenericType>> partialGenericKVTypeMap;
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

  public AbstractMapSerializer(Fury fury, Class<T> cls) {
    this(fury, cls, !ReflectionUtils.isDynamicGeneratedCLass(cls));
  }

  public AbstractMapSerializer(Fury fury, Class<T> cls, boolean supportCodegenHook) {
    super(fury, cls);
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
    if (fury.isChunkSerializeMapEnabled()) {
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

  protected final void chunkWriteElements(Fury fury, MemoryBuffer buffer, Map map) {
    Serializer keySerializer = this.keySerializer;
    Serializer valueSerializer = this.valueSerializer;
    // clear the elemSerializer to avoid conflict if the nested
    // serialization has collection field.
    // TODO use generics for compatible serializer.
    this.keySerializer = null;
    this.valueSerializer = null;
    if (keySerializer != null && valueSerializer != null) {
      javaChunkWriteWithKVSerializers(fury, buffer, map, keySerializer, valueSerializer);
    } else if (keySerializer != null) {
      javaChunkWriteWithKeySerializers(map, buffer, keySerializer);
    } else if (valueSerializer != null) {
      javaChunkWriteWithValueSerializers(map, buffer, valueSerializer);
    } else {
      genericJavaChunkWrite(fury, buffer, map);
    }
  }

  private void javaChunkWriteWithKeySerializers(
      Map map, MemoryBuffer buffer, Serializer keySerializer) {
    MapChunkWriter mapChunkWriter = new MapChunkWriter(fury);
    ClassResolver classResolver = fury.getClassResolver();
    RefResolver refResolver = fury.getRefResolver();
    for (Object object : map.entrySet()) {
      Map.Entry entry = (Map.Entry) object;
      mapChunkWriter = mapChunkWriter.finalKeyNext(entry.getKey(), entry.getValue(), buffer);
      mapChunkWriter.writeFinalKey(entry.getKey(), buffer, keySerializer);
      Object value = entry.getValue();
      mapChunkWriter.writeValue(
          value, buffer, classResolver, refResolver, fury.trackingRef(), valueClassInfoWriteCache);
      mapChunkWriter.increaseChunkSize();
    }
    mapChunkWriter.writeHeader(buffer);
  }

  private void javaChunkWriteWithValueSerializers(
      Map map, MemoryBuffer buffer, Serializer valueSerializer) {
    MapChunkWriter mapChunkWriter = new MapChunkWriter(fury);
    ClassResolver classResolver = fury.getClassResolver();
    RefResolver refResolver = fury.getRefResolver();
    for (Object object : map.entrySet()) {
      Map.Entry entry = (Map.Entry) object;
      Object key = entry.getKey();
      mapChunkWriter = mapChunkWriter.finalValueNext(entry.getKey(), entry.getValue(), buffer);
      mapChunkWriter.writeKey(
          key, buffer, classResolver, refResolver, fury.trackingRef(), keyClassInfoWriteCache);
      mapChunkWriter.writeFinalValue(entry.getValue(), buffer, valueSerializer);
      mapChunkWriter.increaseChunkSize();
    }
    mapChunkWriter.writeHeader(buffer);
  }

  private void javaChunkWriteWithKVSerializers(
      Fury fury,
      MemoryBuffer buffer,
      Map map,
      Serializer keySerializer,
      Serializer valueSerializer) {
    MapChunkWriter mapChunkWriter = new MapChunkWriter(fury);
    for (Object object : map.entrySet()) {
      Map.Entry entry = (Map.Entry) object;
      Object key = entry.getKey();
      Object value = entry.getValue();
      mapChunkWriter = mapChunkWriter.finalKVNext(entry.getKey(), entry.getValue(), buffer);
      mapChunkWriter.writeFinalKey(key, buffer, keySerializer);
      mapChunkWriter.writeFinalValue(value, buffer, valueSerializer);
      mapChunkWriter.increaseChunkSize();
    }
    mapChunkWriter.writeHeader(buffer);
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
      GenericType keyGenericType = genericType.getTypeParameter0();
      GenericType valueGenericType = genericType.getTypeParameter1();
      // type parameters count for `Map field` will be 0;
      // type parameters count for `SubMap<V> field` which SubMap is
      // `SubMap<V> implements Map<String, V>` will be 1;
      if (genericType.getTypeParametersCount() < 2) {
        Tuple2<GenericType, GenericType> kvGenericType = getKVGenericType(genericType);
        if (keyGenericType == objType && valueGenericType == objType) {
          generalJavaWrite(fury, buffer, map);
          return;
        }
        keyGenericType = kvGenericType.f0;
        valueGenericType = kvGenericType.f1;
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

  private void genericJavaChunkWrite(Fury fury, MemoryBuffer buffer, Map map) {
    Generics generics = fury.getGenerics();
    GenericType genericType = generics.nextGenericType();
    if (genericType == null) {
      generalJavaChunkWrite(fury, buffer, map);
    } else {
      GenericType keyGenericType = genericType.getTypeParameter0();
      GenericType valueGenericType = genericType.getTypeParameter1();
      // type parameters count for `Map field` will be 0;
      // type parameters count for `SubMap<V> field` which SubMap is
      // `SubMap<V> implements Map<String, V>` will be 1;
      if (genericType.getTypeParametersCount() < 2) {
        Tuple2<GenericType, GenericType> kvGenericType = getKVGenericType(genericType);
        if (keyGenericType == objType && valueGenericType == objType) {
          generalJavaChunkWrite(fury, buffer, map);
          return;
        }
        keyGenericType = kvGenericType.f0;
        valueGenericType = kvGenericType.f1;
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
        javaKVTypesFinalChunkWrite(fury, buffer, map, keyGenericType, valueGenericType, generics);
      } else if (keyGenericTypeFinal) {
        javaKeyTypeFinalChunkWrite(fury, buffer, map, keyGenericType, valueGenericType, generics);
      } else if (valueGenericTypeFinal) {
        javaValueTypeFinalChunkWrite(fury, buffer, map, keyGenericType, valueGenericType, generics);
      } else {
        javaKVTypesNonFinalChunkWrite(
            fury, buffer, map, keyGenericType, valueGenericType, generics);
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

  /**
   * kv final write do not need to predict , since key and value is almost same type unless null
   *
   * @param fury
   * @param buffer
   * @param map
   * @param keyGenericType
   * @param valueGenericType
   * @param generics
   */
  private void javaKVTypesFinalChunkWrite(
      Fury fury,
      MemoryBuffer buffer,
      Map map,
      GenericType keyGenericType,
      GenericType valueGenericType,
      Generics generics) {
    MapChunkWriter mapChunkWriter = new MapChunkWriter(fury);
    Serializer keySerializer = keyGenericType.getSerializer(fury.getClassResolver());
    Serializer valueSerializer = valueGenericType.getSerializer(fury.getClassResolver());
    for (Object object : map.entrySet()) {
      Map.Entry entry = (Map.Entry) object;
      Object key = entry.getKey();
      Object value = entry.getValue();
      mapChunkWriter = mapChunkWriter.finalKVNext(key, value, buffer);
      generics.pushGenericType(keyGenericType);
      mapChunkWriter.writeFinalKey(key, buffer, keySerializer);
      generics.popGenericType();
      generics.pushGenericType(valueGenericType);
      mapChunkWriter.writeFinalValue(value, buffer, valueSerializer);
      generics.popGenericType();
      mapChunkWriter.increaseChunkSize();
    }
    mapChunkWriter.writeHeader(buffer);
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

  private void javaKeyTypeFinalChunkWrite(
      Fury fury,
      MemoryBuffer buffer,
      Map map,
      GenericType keyGenericType,
      GenericType valueGenericType,
      Generics generics) {
    MapChunkWriter mapChunkWriter = new MapChunkWriter(fury);
    ClassResolver classResolver = fury.getClassResolver();
    RefResolver refResolver = fury.getRefResolver();
    Serializer keySerializer = keyGenericType.getSerializer(fury.getClassResolver());
    boolean trackingValueRef = fury.getClassResolver().needToWriteRef(valueGenericType.getCls());
    for (Object object : map.entrySet()) {
      Map.Entry entry = (Map.Entry) object;
      Object key = entry.getKey();
      Object value = entry.getValue();
      mapChunkWriter = mapChunkWriter.finalKeyNext(key, value, buffer);
      generics.pushGenericType(keyGenericType);
      mapChunkWriter.writeFinalKey(key, buffer, keySerializer);
      generics.popGenericType();
      generics.pushGenericType(valueGenericType);
      mapChunkWriter.writeValue(
          value, buffer, classResolver, refResolver, trackingValueRef, valueClassInfoWriteCache);
      generics.popGenericType();
      mapChunkWriter.increaseChunkSize();
    }
    mapChunkWriter.writeHeader(buffer);
  }

  private void javaValueTypeFinalChunkWrite(
      Fury fury,
      MemoryBuffer buffer,
      Map map,
      GenericType keyGenericType,
      GenericType valueGenericType,
      Generics generics) {
    MapChunkWriter mapChunkWriter = new MapChunkWriter(fury);
    ClassResolver classResolver = fury.getClassResolver();
    RefResolver refResolver = fury.getRefResolver();
    Serializer valueSerializer = valueGenericType.getSerializer(fury.getClassResolver());
    boolean trackingKeyRef = fury.getClassResolver().needToWriteRef(keyGenericType.getCls());
    for (Object object : map.entrySet()) {
      Map.Entry entry = (Map.Entry) object;
      Object key = entry.getKey();
      Object value = entry.getValue();
      mapChunkWriter = mapChunkWriter.finalValueNext(key, value, buffer);
      generics.pushGenericType(keyGenericType);
      mapChunkWriter.writeKey(
          key, buffer, classResolver, refResolver, trackingKeyRef, keyClassInfoWriteCache);
      generics.popGenericType();
      generics.pushGenericType(valueGenericType);
      mapChunkWriter.writeFinalValue(value, buffer, valueSerializer);
      generics.popGenericType();
      mapChunkWriter.increaseChunkSize();
    }
    mapChunkWriter.writeHeader(buffer);
  }

  private void javaKVTypesNonFinalChunkWrite(
      Fury fury,
      MemoryBuffer buffer,
      Map map,
      GenericType keyGenericType,
      GenericType valueGenericType,
      Generics generics) {
    MapChunkWriter mapChunkWriter = new MapChunkWriter(fury);
    ClassResolver classResolver = fury.getClassResolver();
    RefResolver refResolver = fury.getRefResolver();
    boolean trackingKeyRef = fury.getClassResolver().needToWriteRef(keyGenericType.getCls());
    boolean trackingValueRef = fury.getClassResolver().needToWriteRef(valueGenericType.getCls());
    for (Object object : map.entrySet()) {
      Map.Entry entry = (Map.Entry) object;
      Object key = entry.getKey();
      Object value = entry.getValue();
      mapChunkWriter = mapChunkWriter.next(key, value, buffer);
      if (mapChunkWriter.isMarkChunkWriteFinish()) {
        writeJavaRefOptimized(
            fury, classResolver, refResolver, trackingKeyRef, buffer, key, keyClassInfoWriteCache);
        writeJavaRefOptimized(
            fury,
            classResolver,
            refResolver,
            trackingValueRef,
            buffer,
            value,
            keyClassInfoWriteCache);
      } else {
        generics.pushGenericType(keyGenericType);
        mapChunkWriter.writeKey(
            key, buffer, classResolver, refResolver, trackingKeyRef, keyClassInfoWriteCache);
        generics.popGenericType();
        generics.pushGenericType(valueGenericType);
        mapChunkWriter.writeValue(
            value, buffer, classResolver, refResolver, trackingValueRef, valueClassInfoWriteCache);
        generics.popGenericType();
        mapChunkWriter.increaseChunkSize();
      }
    }
    mapChunkWriter.writeHeader(buffer);
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

  protected void generalJavaChunkWrite(Fury fury, MemoryBuffer buffer, Map map) {
    MapChunkWriter chunkWriter = new MapChunkWriter(fury);
    ClassResolver classResolver = fury.getClassResolver();
    RefResolver refResolver = fury.getRefResolver();
    for (Object object : map.entrySet()) {
      Map.Entry entry = (Map.Entry) object;
      Object key = entry.getKey();
      Object value = entry.getValue();
      chunkWriter = chunkWriter.next(key, value, buffer);
      if (!chunkWriter.isMarkChunkWriteFinish()) {
        chunkWriter.generalChunkWrite(
            key,
            value,
            buffer,
            classResolver,
            refResolver,
            keyClassInfoWriteCache,
            valueClassInfoWriteCache);
      } else {
        writeJavaRefOptimized(
            fury, classResolver, refResolver, buffer, entry.getKey(), keyClassInfoWriteCache);
        writeJavaRefOptimized(
            fury, classResolver, refResolver, buffer, entry.getValue(), valueClassInfoWriteCache);
      }
    }
    chunkWriter.writeHeader(buffer);
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
          fury.xwriteRefByNullableSerializer(buffer, entry.getKey(), keySerializer);
          fury.xwriteRefByNullableSerializer(buffer, entry.getValue(), valueSerializer);
        }
      } else if (valueGenericType.hasGenericParameters()) {
        for (Object object : value.entrySet()) {
          Map.Entry entry = (Map.Entry) object;
          fury.xwriteRefByNullableSerializer(buffer, entry.getKey(), keySerializer);
          generics.pushGenericType(valueGenericType);
          fury.xwriteRefByNullableSerializer(buffer, entry.getValue(), valueSerializer);
          generics.popGenericType();
        }
      } else if (keyGenericType.hasGenericParameters()) {
        for (Object object : value.entrySet()) {
          Map.Entry entry = (Map.Entry) object;
          generics.pushGenericType(keyGenericType);
          fury.xwriteRefByNullableSerializer(buffer, entry.getKey(), keySerializer);
          generics.popGenericType();
          fury.xwriteRefByNullableSerializer(buffer, entry.getValue(), valueSerializer);
        }
      } else {
        for (Object object : value.entrySet()) {
          Map.Entry entry = (Map.Entry) object;
          generics.pushGenericType(keyGenericType);
          fury.xwriteRefByNullableSerializer(buffer, entry.getKey(), keySerializer);
          generics.pushGenericType(valueGenericType);
          fury.xwriteRefByNullableSerializer(buffer, entry.getValue(), valueSerializer);
        }
      }
      generics.popGenericType();
    }
  }

  private Tuple2<GenericType, GenericType> getKVGenericType(GenericType genericType) {
    Tuple2<GenericType, GenericType> genericTypes = partialGenericKVTypeMap.get(genericType);
    if (genericTypes == null) {
      TypeRef<?> typeRef = genericType.getTypeRef();
      if (!MAP_TYPE.isSupertypeOf(typeRef)) {
        Tuple2<GenericType, GenericType> typeTuple = Tuple2.of(objType, objType);
        partialGenericKVTypeMap.put(genericType, typeTuple);
        return typeTuple;
      }
      Tuple2<TypeRef<?>, TypeRef<?>> mapKeyValueType = TypeUtils.getMapKeyValueType(typeRef);
      genericTypes =
          Tuple2.of(
              fury.getClassResolver().buildGenericType(mapKeyValueType.f0.getType()),
              fury.getClassResolver().buildGenericType(mapKeyValueType.f1.getType()));
      partialGenericKVTypeMap.put(genericType, genericTypes);
    }
    return genericTypes;
  }

  @Override
  public T xread(MemoryBuffer buffer) {
    Map map = newMap(buffer);
    xreadElements(fury, buffer, map, numElements);
    return onMapRead(map);
  }

  @SuppressWarnings("unchecked")
  protected final void chunkReadElements(MemoryBuffer buffer, int size, Map map) {
    Serializer keySerializer = this.keySerializer;
    Serializer valueSerializer = this.valueSerializer;
    // clear the elemSerializer to avoid conflict if the nested
    // serialization has collection field.
    // TODO use generics for compatible serializer.
    this.keySerializer = null;
    this.valueSerializer = null;
    if (keySerializer != null && valueSerializer != null) {
      javaChunkReadWithKVSerializers(buffer, map, size, keySerializer, valueSerializer);
    } else if (keySerializer != null) {
      javaChunkReadWithKeySerializer(buffer, map, size, keySerializer);
    } else if (valueSerializer != null) {
      javaChunkReadWithValueSerializer(buffer, map, size, valueSerializer);
    } else {
      genericJavaChunkRead(fury, buffer, map, size);
    }
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
      GenericType keyGenericType = genericType.getTypeParameter0();
      GenericType valueGenericType = genericType.getTypeParameter1();
      if (genericType.getTypeParametersCount() < 2) {
        Tuple2<GenericType, GenericType> kvGenericType = getKVGenericType(genericType);
        if (keyGenericType == objType && valueGenericType == objType) {
          generalJavaRead(fury, buffer, map, size);
          return;
        }
        keyGenericType = kvGenericType.f0;
        valueGenericType = kvGenericType.f1;
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

  private void javaChunkReadWithKeySerializer(
      MemoryBuffer buffer, Map map, int size, Serializer keySerializer) {
    MapChunkWriter mapChunkWriter = new MapChunkWriter(fury);
    while (size > 0) {
      byte chunkSize = buffer.readByte();
      byte header = buffer.readByte();
      Preconditions.checkArgument(
          chunkSize >= 0,
          "chunkSize < 0, which means serialization protocol is not same with deserialization protocol");
      for (byte i = 0; i < chunkSize; i++) {
        Object key;
        Object value;
        key = mapChunkWriter.readFinalKey(buffer, header, keySerializer);
        value =
            mapChunkWriter.readValue(
                header,
                buffer,
                fury.getClassResolver(),
                fury.trackingRef(),
                valueClassInfoReadCache);
        map.put(key, value);
        size--;
      }
    }
  }

  private void javaChunkReadWithValueSerializer(
      MemoryBuffer buffer, Map map, int size, Serializer valueSerializer) {
    MapChunkWriter mapChunkWriter = new MapChunkWriter(fury);
    while (size > 0) {
      byte chunkSize = buffer.readByte();
      byte header = buffer.readByte();
      Preconditions.checkArgument(
          chunkSize >= 0,
          "chunkSize < 0, which means serialization protocol is not same with deserialization protocol");
      for (byte i = 0; i < chunkSize; i++) {
        Object key;
        Object value;
        key =
            mapChunkWriter.readKey(
                header, buffer, fury.getClassResolver(), fury.trackingRef(), keyClassInfoReadCache);
        value = mapChunkWriter.readFinalValue(buffer, header, valueSerializer);
        map.put(key, value);
        size--;
      }
    }
  }

  private void javaChunkReadWithKVSerializers(
      MemoryBuffer buffer,
      Map map,
      int size,
      Serializer keySerializer,
      Serializer valueSerializer) {
    MapChunkWriter mapChunkWriter = new MapChunkWriter(fury);
    while (size > 0) {
      byte chunkSize = buffer.readByte();
      byte header = buffer.readByte();
      Preconditions.checkArgument(
          chunkSize >= 0,
          "chunkSize < 0, which means serialization protocol is not same with deserialization protocol");
      for (byte i = 0; i < chunkSize; i++) {
        Object key;
        Object value;
        key = mapChunkWriter.readFinalKey(buffer, header, keySerializer);
        value = mapChunkWriter.readFinalValue(buffer, header, valueSerializer);
        map.put(key, value);
        size--;
      }
    }
  }

  private void genericJavaChunkRead(Fury fury, MemoryBuffer buffer, Map map, int size) {
    Generics generics = fury.getGenerics();
    GenericType genericType = generics.nextGenericType();
    if (genericType == null) {
      generalJavaChunkRead(fury, buffer, map, size);
    } else {
      GenericType keyGenericType = genericType.getTypeParameter0();
      GenericType valueGenericType = genericType.getTypeParameter1();
      if (genericType.getTypeParametersCount() < 2) {
        Tuple2<GenericType, GenericType> kvGenericType = getKVGenericType(genericType);
        if (keyGenericType == objType && valueGenericType == objType) {
          generalJavaChunkRead(fury, buffer, map, size);
          return;
        }
        keyGenericType = kvGenericType.f0;
        valueGenericType = kvGenericType.f1;
      }
      boolean keyGenericTypeFinal = keyGenericType.isMonomorphic();
      boolean valueGenericTypeFinal = valueGenericType.isMonomorphic();
      if (keyGenericTypeFinal && valueGenericTypeFinal) {
        javaKVTypesFinalChunkRead(
            fury, buffer, map, keyGenericType, valueGenericType, generics, size);
      } else if (keyGenericTypeFinal) {
        javaKeyTypeFinalChunkRead(
            fury, buffer, map, keyGenericType, valueGenericType, generics, size);
      } else if (valueGenericTypeFinal) {
        javaValueTypeFinalChunkRead(
            fury, buffer, map, keyGenericType, valueGenericType, generics, size);
      } else {
        javaKVTypesNonFinalChunkRead(
            fury, buffer, map, keyGenericType, valueGenericType, generics, size);
      }
      generics.popGenericType();
    }
  }

  private void javaKVTypesFinalChunkRead(
      Fury fury,
      MemoryBuffer buffer,
      Map map,
      GenericType keyGenericType,
      GenericType valueGenericType,
      Generics generics,
      int size) {
    Serializer keySerializer = keyGenericType.getSerializer(fury.getClassResolver());
    Serializer valueSerializer = valueGenericType.getSerializer(fury.getClassResolver());
    MapChunkWriter mapChunkWriter = new MapChunkWriter(fury);
    while (size > 0) {
      byte chunkSize = buffer.readByte();
      byte header = buffer.readByte();
      mapChunkWriter.setKeyReadSerializer(null);
      mapChunkWriter.setValueReadSerializer(null);
      Preconditions.checkArgument(
          chunkSize >= 0,
          "chunkSize < 0, which means serialization protocol is not same with deserialization protocol");
      for (byte i = 0; i < chunkSize; i++) {
        Object key;
        Object value;
        generics.pushGenericType(keyGenericType);
        key = mapChunkWriter.readFinalKey(buffer, header, keySerializer);
        generics.popGenericType();
        generics.pushGenericType(valueGenericType);
        value = mapChunkWriter.readFinalValue(buffer, header, valueSerializer);
        generics.popGenericType();
        map.put(key, value);
        size--;
      }
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

  private void javaKeyTypeFinalChunkRead(
      Fury fury,
      MemoryBuffer buffer,
      Map map,
      GenericType keyGenericType,
      GenericType valueGenericType,
      Generics generics,
      int size) {
    final MapChunkWriter mapChunkWriter = new MapChunkWriter(fury);
    ClassResolver classResolver = fury.getClassResolver();
    boolean trackingValueRef = classResolver.needToWriteRef(valueGenericType.getCls());
    Serializer keySerializer = keyGenericType.getSerializer(fury.getClassResolver());
    while (size > 0) {
      byte chunkSize = buffer.readByte();
      Preconditions.checkArgument(
          chunkSize >= 0,
          "chunkSize < 0, which means serialization protocol is not same with deserialization protocol");
      byte header = buffer.readByte();
      mapChunkWriter.setKeyReadSerializer(null);
      mapChunkWriter.setValueReadSerializer(null);
      while (chunkSize > 0) {
        generics.pushGenericType(keyGenericType);
        Object key = mapChunkWriter.readFinalKey(buffer, header, keySerializer);
        generics.pushGenericType(valueGenericType);
        Object value =
            mapChunkWriter.readValue(
                header, buffer, classResolver, trackingValueRef, valueClassInfoReadCache);
        generics.pushGenericType(valueGenericType);
        generics.popGenericType();
        chunkSize--;
        size--;
        map.put(key, value);
      }
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

  private void javaValueTypeFinalChunkRead(
      Fury fury,
      MemoryBuffer buffer,
      Map map,
      GenericType keyGenericType,
      GenericType valueGenericType,
      Generics generics,
      int size) {
    MapChunkWriter mapChunkWriter = new MapChunkWriter(fury);
    boolean trackingKeyRef = fury.getClassResolver().needToWriteRef(keyGenericType.getCls());
    Serializer valueSerializer = valueGenericType.getSerializer(fury.getClassResolver());
    ClassResolver classResolver = fury.getClassResolver();
    while (size > 0) {
      byte chunkSize = buffer.readByte();
      Preconditions.checkArgument(
          chunkSize >= 0,
          "chunkSize < 0, which means serialization protocol is not same with deserialization protocol");
      byte header = buffer.readByte();
      mapChunkWriter.setKeyReadSerializer(null);
      mapChunkWriter.setValueReadSerializer(null);
      while (chunkSize > 0) {
        generics.pushGenericType(keyGenericType);
        Object key =
            mapChunkWriter.readKey(
                header, buffer, classResolver, trackingKeyRef, keyClassInfoReadCache);
        generics.pushGenericType(valueGenericType);
        Object value = mapChunkWriter.readFinalValue(buffer, header, valueSerializer);
        generics.pushGenericType(valueGenericType);
        generics.popGenericType();
        chunkSize--;
        size--;
        map.put(key, value);
      }
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

  private void javaKVTypesNonFinalChunkRead(
      Fury fury,
      MemoryBuffer buffer,
      Map map,
      GenericType keyGenericType,
      GenericType valueGenericType,
      Generics generics,
      int size) {
    final MapChunkWriter mapChunkWriter = new MapChunkWriter(fury);
    ClassResolver classResolver = fury.getClassResolver();
    RefResolver refResolver = fury.getRefResolver();
    boolean trackingKeyRef = classResolver.needToWriteRef(keyGenericType.getCls());
    boolean trackingValueRef = classResolver.needToWriteRef(valueGenericType.getCls());
    while (size > 0) {
      byte chunkSize = buffer.readByte();
      Preconditions.checkArgument(
          chunkSize >= 0,
          "chunkSize < 0, which means serialization protocol is not same with deserialization protocol");
      if (chunkSize == 0) {
        while (size > 0) {
          generics.pushGenericType(keyGenericType);
          Object key =
              readJavaRefOptimized(
                  fury, refResolver, trackingKeyRef, buffer, keyClassInfoReadCache);
          generics.popGenericType();
          generics.pushGenericType(valueGenericType);
          Object value =
              readJavaRefOptimized(
                  fury, refResolver, trackingValueRef, buffer, valueClassInfoReadCache);
          generics.popGenericType();
          map.put(key, value);
          size--;
        }
      } else {
        byte header = buffer.readByte();
        mapChunkWriter.setKeyReadSerializer(null);
        mapChunkWriter.setValueReadSerializer(null);
        while (chunkSize > 0) {
          generics.pushGenericType(keyGenericType);
          Object key =
              mapChunkWriter.readKey(
                  header, buffer, classResolver, trackingKeyRef, keyClassInfoReadCache);
          generics.popGenericType();
          generics.pushGenericType(valueGenericType);
          Object value =
              mapChunkWriter.readValue(
                  header, buffer, classResolver, trackingValueRef, valueClassInfoReadCache);
          generics.popGenericType();
          chunkSize--;
          size--;
          map.put(key, value);
        }
      }
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

  private void generalJavaChunkRead(Fury fury, MemoryBuffer buffer, Map map, int size) {
    final MapChunkWriter mapChunkWriter = new MapChunkWriter(fury);
    ClassResolver classResolver = fury.getClassResolver();
    boolean trackingKeyRef = fury.trackingRef();
    while (size > 0) {
      byte chunkSize = buffer.readByte();
      Preconditions.checkArgument(
          chunkSize >= 0,
          "chunkSize < 0, which means serialization protocol is not same with deserialization protocol");
      if (chunkSize == 0) {
        while (size > 0) {
          Object key = fury.readRef(buffer, keyClassInfoReadCache);
          Object value = fury.readRef(buffer, keyClassInfoReadCache);
          map.put(key, value);
          size--;
        }
      } else {
        byte header = buffer.readByte();
        mapChunkWriter.setKeyReadSerializer(null);
        mapChunkWriter.setValueReadSerializer(null);
        while (chunkSize > 0) {
          Object key =
              mapChunkWriter.readKey(
                  header, buffer, classResolver, trackingKeyRef, keyClassInfoReadCache);
          Object value =
              mapChunkWriter.readValue(
                  header, buffer, classResolver, trackingKeyRef, valueClassInfoReadCache);
          chunkSize--;
          size--;
          map.put(key, value);
        }
      }
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
          Object key = fury.xreadRefByNullableSerializer(buffer, keySerializer);
          Object value = fury.xreadRefByNullableSerializer(buffer, valueSerializer);
          map.put(key, value);
        }
      } else if (valueGenericType.hasGenericParameters()) {
        for (int i = 0; i < size; i++) {
          Object key = fury.xreadRefByNullableSerializer(buffer, keySerializer);
          generics.pushGenericType(valueGenericType);
          Object value = fury.xreadRefByNullableSerializer(buffer, valueSerializer);
          generics.popGenericType();
          map.put(key, value);
        }
      } else if (keyGenericType.hasGenericParameters()) {
        for (int i = 0; i < size; i++) {
          generics.pushGenericType(keyGenericType);
          Object key = fury.xreadRefByNullableSerializer(buffer, keySerializer);
          generics.popGenericType();
          Object value = fury.xreadRefByNullableSerializer(buffer, valueSerializer);
          map.put(key, value);
        }
      } else {
        for (int i = 0; i < size; i++) {
          // FIXME(chaokunyang) nested generics may be get by mistake.
          generics.pushGenericType(keyGenericType);
          Object key = fury.xreadRefByNullableSerializer(buffer, keySerializer);
          generics.pushGenericType(valueGenericType);
          Object value = fury.xreadRefByNullableSerializer(buffer, valueSerializer);
          map.put(key, value);
        }
      }
      generics.popGenericType();
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

  /**
   * Get and reset numElements of deserializing collection. Should be called after {@link #newMap}.
   * Nested read may overwrite this element, reset is necessary to avoid use wrong value by mistake.
   */
  public int getAndClearNumElements() {
    int size = numElements;
    numElements = -1; // nested read may overwrite this element.
    return size;
  }

  public void setNumElements(int numElements) {
    this.numElements = numElements;
  }

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
