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

import com.google.common.reflect.TypeToken;
import java.lang.invoke.MethodHandle;
import java.util.Map;
import org.apache.fury.Fury;
import org.apache.fury.collection.IdentityMap;
import org.apache.fury.collection.Tuple2;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.resolver.ClassInfoHolder;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.RefResolver;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.type.GenericType;
import org.apache.fury.type.Generics;
import org.apache.fury.type.TypeUtils;
import org.apache.fury.util.ReflectionUtils;

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
    writeElements(fury, buffer, map);
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
      TypeToken<?> typeToken = genericType.getTypeToken();
      if (!MAP_TYPE.isSupertypeOf(typeToken)) {
        Tuple2<GenericType, GenericType> typeTuple = Tuple2.of(objType, objType);
        partialGenericKVTypeMap.put(genericType, typeTuple);
        return typeTuple;
      }
      Tuple2<TypeToken<?>, TypeToken<?>> mapKeyValueType = TypeUtils.getMapKeyValueType(typeToken);
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
