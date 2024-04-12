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

import com.google.common.reflect.TypeToken;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import org.apache.fury.Fury;
import org.apache.fury.config.Language;
import org.apache.fury.exception.ClassNotCompatibleException;
import org.apache.fury.logging.Logger;
import org.apache.fury.logging.LoggerFactory;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.type.Descriptor;
import org.apache.fury.type.GenericType;
import org.apache.fury.type.Generics;
import org.apache.fury.type.Type;
import org.apache.fury.type.TypeUtils;
import org.apache.fury.util.FieldAccessor;
import org.apache.fury.util.Platform;
import org.apache.fury.util.Preconditions;
import org.apache.fury.util.Utils;

/**
 * A serializer used for cross-language serialization for custom objects.
 *
 * <p>TODO(chaokunyang) support generics optimization for {@code SomeClass<T>}.
 */
@SuppressWarnings({"unchecked", "rawtypes", "UnstableApiUsage"})
public class StructSerializer<T> extends Serializer<T> {
  private static final Logger LOG = LoggerFactory.getLogger(StructSerializer.class);
  private final String typeTag;
  private final Constructor<T> constructor;
  private final FieldAccessor[] fieldAccessors;
  private GenericType[] fieldGenerics;
  private GenericType genericType;
  private final IdentityHashMap<GenericType, GenericType[]> genericTypesCache;
  private int typeHash;

  public StructSerializer(Fury fury, Class<T> cls, String typeTag) {
    super(fury, cls);
    this.typeTag = typeTag;
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
      Utils.ignore(e);
    }
    this.constructor = ctr;
    fieldAccessors =
        Descriptor.getFields(cls).stream()
            .sorted(Comparator.comparing(Field::getName))
            .map(FieldAccessor::createAccessor)
            .toArray(FieldAccessor[]::new);
    fieldGenerics = buildFieldGenerics(TypeToken.of(cls), fieldAccessors);
    genericTypesCache = new IdentityHashMap<>();
    genericTypesCache.put(null, fieldGenerics);
  }

  private static <T> GenericType[] buildFieldGenerics(
      TypeToken<T> type, FieldAccessor[] fieldAccessors) {
    return Arrays.stream(fieldAccessors)
        .map(fieldAccessor -> GenericType.build(type, fieldAccessor.getField().getGenericType()))
        .toArray(GenericType[]::new);
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
  public short getXtypeId() {
    return Fury.FURY_TYPE_TAG_ID;
  }

  @Override
  public String getCrossLanguageTypeTag() {
    return typeTag;
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
      Serializer serializer = fieldGeneric.getSerializerOrNull(fury.getClassResolver());
      boolean hasGenerics = fieldGeneric.hasGenericParameters();
      if (hasGenerics) {
        generics.pushGenericType(fieldGeneric);
      }
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
        fieldGenerics = buildFieldGenerics(genericType.getTypeToken(), fieldAccessors);
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
      Serializer serializer = fieldGeneric.getSerializerOrNull(fury.getClassResolver());
      boolean hasGenerics = fieldGeneric.hasGenericParameters();
      if (hasGenerics) {
        generics.pushGenericType(fieldGeneric);
      }
      Object fieldValue = fury.xreadRefByNullableSerializer(buffer, serializer);
      fieldAccessor.set(obj, fieldValue);
      if (hasGenerics) {
        generics.pushGenericType(fieldGeneric);
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
    if (fieldGeneric.getTypeToken().isSubtypeOf(List.class)) {
      // TODO(chaokunyang) add list element type into schema hash
      id = Type.LIST.getId();
    } else if (fieldGeneric.getTypeToken().isSubtypeOf(Map.class)) {
      // TODO(chaokunyang) add map key&value type into schema hash
      id = Type.MAP.getId();
    } else {
      try {
        Serializer<?> serializer = fury.getClassResolver().getSerializer(fieldGeneric.getCls());
        short xtypeId = serializer.getXtypeId();
        if (xtypeId == Fury.NOT_SUPPORT_CROSS_LANGUAGE) {
          return hash;
        }
        id = Math.abs(xtypeId);
        if (id == Type.FURY_TYPE_TAG.getId()) {
          id = TypeUtils.computeStringHash(serializer.getCrossLanguageTypeTag());
        }
      } catch (Exception e) {
        return hash;
      }
    }
    long newHash = ((long) hash) * 31 + id;
    while (newHash >= Integer.MAX_VALUE) {
      newHash /= 7;
    }
    return (int) newHash;
  }
}
