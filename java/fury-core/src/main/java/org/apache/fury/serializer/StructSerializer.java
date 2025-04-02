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

package org.apache.fury.serializer;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.fury.Fury;
import org.apache.fury.config.Language;
import org.apache.fury.exception.ClassNotCompatibleException;
import org.apache.fury.logging.Logger;
import org.apache.fury.logging.LoggerFactory;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.Platform;
import org.apache.fury.reflect.FieldAccessor;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.reflect.TypeRef;
import org.apache.fury.resolver.ClassInfo;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.serializer.collection.CollectionSerializer;
import org.apache.fury.serializer.collection.MapSerializer;
import org.apache.fury.type.Descriptor;
import org.apache.fury.type.GenericType;
import org.apache.fury.type.Generics;
import org.apache.fury.type.TypeUtils;
import org.apache.fury.type.Types;
import org.apache.fury.util.ExceptionUtils;
import org.apache.fury.util.Preconditions;
import org.apache.fury.util.StringUtils;

/**
 * A serializer used for cross-language serialization for custom objects.
 *
 * <p>TODO(chaokunyang) support generics optimization for {@code SomeClass<T>}.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class StructSerializer<T> extends Serializer<T> {
  private static final Logger LOG = LoggerFactory.getLogger(StructSerializer.class);
  private final Constructor<T> constructor;
  private final FieldAccessor[] fieldAccessors;
  private GenericType[] fieldGenerics;
  private GenericType genericType;
  private final IdentityHashMap<GenericType, GenericType[]> genericTypesCache;
  private int typeHash;

  public StructSerializer(Fury fury, Class<T> cls) {
    super(fury, cls);
    if (fury.getLanguage() == Language.JAVA) {
      LOG.warn("Type of class {} shouldn't be serialized using cross-language serializer", cls);
    }
    Constructor<T> ctr = null;
    try {
      ctr = cls.getConstructor();
      if (!ctr.isAccessible()) {
        ctr.setAccessible(true);
      }
    } catch (Exception e) {
      ExceptionUtils.ignore(e);
    }
    this.constructor = ctr;
    fieldAccessors =
        Descriptor.getFields(cls).stream()
            .map(
                f ->
                    new AbstractMap.SimpleEntry<>(
                        f, StringUtils.lowerCamelToLowerUnderscore(f.getName())))
            .sorted(Map.Entry.comparingByValue())
            .map(Map.Entry::getKey)
            .map(FieldAccessor::createAccessor)
            .toArray(FieldAccessor[]::new);
    fieldGenerics = buildFieldGenerics(fury, TypeRef.of(cls), fieldAccessors);
    genericTypesCache = new IdentityHashMap<>();
    genericTypesCache.put(null, fieldGenerics);
  }

  private <T> GenericType[] buildFieldGenerics(
      Fury fury, TypeRef<T> type, FieldAccessor[] fieldAccessors) {
    return Arrays.stream(fieldAccessors)
        .map(fieldAccessor -> getGenericType(fury, type, fieldAccessor))
        .toArray(GenericType[]::new);
  }

  private static <T> GenericType getGenericType(
      Fury fury, TypeRef<T> type, FieldAccessor fieldAccessor) {
    GenericType t = GenericType.build(type, fieldAccessor.getField().getGenericType());
    if (t.getTypeParametersCount() > 0) {
      boolean skip = Arrays.stream(t.getTypeParameters()).allMatch(p -> p.getCls() == Object.class);
      if (skip) {
        t = new GenericType(t.getTypeRef(), t.isMonomorphic());
      }
    }
    ClassResolver resolver = fury.getClassResolver();
    Class cls = t.getCls();
    if (resolver.isMonomorphic(cls)) {
      t.setSerializer(fury.getXtypeResolver().getClassInfo(cls).getSerializer());
      return t;
    }
    // We have one type id for map, there is no map polymorphic support.
    // If one want to deserialize map data into different map type, he should
    // use the concrete map subclass type to declare the field type or use some hints
    // such as field annotation.
    if (resolver.isMap(cls)) {
      t.setSerializer(
          ReflectionUtils.isAbstract(cls)
              ? new MapSerializer(fury, HashMap.class)
              : resolver.getSerializer(cls));
    } else if (resolver.isCollection(cls)) {
      t.setSerializer(
          ReflectionUtils.isAbstract(cls)
              ? new CollectionSerializer(fury, ArrayList.class)
              : resolver.getSerializer(cls));
    } else if (cls.isArray()) {
      t.setSerializer(new ArraySerializers.ObjectArraySerializer(fury, cls));
    }
    return t;
  }

  @Override
  public void write(MemoryBuffer buffer, T value) {
    xwrite(buffer, value);
  }

  @Override
  public T read(MemoryBuffer buffer) {
    return xread(buffer);
  }

  @Override
  public void xwrite(MemoryBuffer buffer, T value) {
    // TODO(chaokunyang) support fields back and forward compatible.
    //  Maybe need to serialize fields name too.
    int typeHash = this.typeHash;
    if (typeHash == 0) {
      typeHash = computeStructHash();
      this.typeHash = typeHash;
    }
    buffer.writeInt32(typeHash);
    Generics generics = fury.getGenerics();
    GenericType[] fieldGenerics = getGenericTypes(generics);
    for (int i = 0; i < fieldAccessors.length; i++) {
      FieldAccessor fieldAccessor = fieldAccessors[i];
      GenericType fieldGeneric = fieldGenerics[i];
      boolean hasGenerics = fieldGeneric.hasGenericParameters();
      if (hasGenerics) {
        generics.pushGenericType(fieldGeneric);
      }
      Serializer serializer = fieldGeneric.getSerializer();
      if (serializer != null) {
        fury.xwriteRef(buffer, fieldAccessor.get(value), serializer);
      } else {
        fury.xwriteRef(buffer, fieldAccessor.get(value));
      }
      if (hasGenerics) {
        generics.popGenericType();
      }
    }
  }

  private GenericType[] getGenericTypes(Generics generics) {
    GenericType[] fieldGenerics = this.fieldGenerics;
    // support generics <T> in Pojo<T>
    GenericType genericType = generics.nextGenericType();
    if (genericType != this.genericType) {
      this.genericType = genericType;
      fieldGenerics = genericTypesCache.get(genericType);
      if (fieldGenerics == null) {
        fieldGenerics = buildFieldGenerics(fury, genericType.getTypeRef(), fieldAccessors);
        genericTypesCache.put(genericType, fieldGenerics);
      }
      this.fieldGenerics = fieldGenerics;
    }
    return fieldGenerics;
  }

  @Override
  public T xread(MemoryBuffer buffer) {
    int typeHash = this.typeHash;
    if (typeHash == 0) {
      typeHash = computeStructHash();
      this.typeHash = typeHash;
    }
    int newHash = buffer.readInt32();
    if (newHash != typeHash) {
      throw new ClassNotCompatibleException(
          String.format(
              "Hash %d is not consistent with %s for class %s",
              newHash, typeHash, fury.getClassResolver().getCurrentReadClass()));
    }
    T obj = newBean();
    fury.getRefResolver().reference(obj);
    Generics generics = fury.getGenerics();
    GenericType[] fieldGenerics = getGenericTypes(generics);
    for (int i = 0; i < fieldAccessors.length; i++) {
      FieldAccessor fieldAccessor = fieldAccessors[i];
      GenericType fieldGeneric = fieldGenerics[i];
      boolean hasGenerics = fieldGeneric.hasGenericParameters();
      if (hasGenerics) {
        generics.pushGenericType(fieldGeneric);
      }
      Object fieldValue;
      Serializer serializer = fieldGeneric.getSerializer();
      if (serializer == null) {
        fieldValue = fury.xreadRef(buffer);
      } else {
        fieldValue = fury.xreadRef(buffer, serializer);
      }
      fieldAccessor.set(obj, fieldValue);
      if (hasGenerics) {
        generics.popGenericType();
      }
    }
    return obj;
  }

  private T newBean() {
    if (constructor != null) {
      try {
        return constructor.newInstance();
      } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
        Platform.throwException(e);
      }
    }
    return Platform.newInstance(type);
  }

  int computeStructHash() {
    int hash = 17;
    for (GenericType fieldGeneric : fieldGenerics) {
      hash = computeFieldHash(hash, fieldGeneric);
    }
    Preconditions.checkState(hash != 0);
    return hash;
  }

  int computeFieldHash(int hash, GenericType fieldGeneric) {
    int id;
    if (fieldGeneric.getTypeRef().isSubtypeOf(List.class)) {
      // TODO(chaokunyang) add list element type into schema hash
      id = Types.LIST;
    } else if (fieldGeneric.getTypeRef().isSubtypeOf(Map.class)) {
      // TODO(chaokunyang) add map key&value type into schema hash
      id = Types.MAP;
    } else {
      try {
        ClassInfo classInfo = fury.getXtypeResolver().getClassInfo(fieldGeneric.getCls());
        int xtypeId = classInfo.getXtypeId();
        if (Types.isStructType((byte) xtypeId)) {
          id =
              TypeUtils.computeStringHash(classInfo.decodeNamespace() + classInfo.decodeTypeName());
        } else {
          id = Math.abs(xtypeId);
        }
      } catch (Exception e) {
        id = 0;
      }
    }
    long newHash = ((long) hash) * 31 + id;
    while (newHash >= Integer.MAX_VALUE) {
      newHash /= 7;
    }
    return (int) newHash;
  }
}
