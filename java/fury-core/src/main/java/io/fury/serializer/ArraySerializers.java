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

import com.google.common.base.Preconditions;
import io.fury.Fury;
import io.fury.memory.MemoryBuffer;
import io.fury.resolver.ReferenceResolver;
import io.fury.type.Type;
import java.lang.reflect.Array;
import java.lang.reflect.Modifier;

/**
 * Serializers for array types.
 *
 * @author chaokunyang
 */
public class ArraySerializers {

  /** May be multi-dimension array, or multi-dimension primitive array. */
  @SuppressWarnings("unchecked")
  public static final class ObjectArraySerializer<T> extends Serializer<T[]> {
    private final Class<T> innerType;
    private final Serializer componentTypeSerializer;
    private final int dimension;
    private final int[] stubDims;

    public ObjectArraySerializer(Fury fury, Class<T[]> cls) {
      super(fury, cls);
      Preconditions.checkArgument(cls.isArray());
      Class<?> t = cls;
      Class<?> innerType = cls;
      int dimension = 0;
      while (t != null && t.isArray()) {
        dimension++;
        t = t.getComponentType();
        if (t != null) {
          innerType = t;
        }
      }
      this.dimension = dimension;
      this.innerType = (Class<T>) innerType;
      Class<?> componentType = cls.getComponentType();
      if (Modifier.isFinal(componentType.getModifiers())) {
        this.componentTypeSerializer = fury.getClassResolver().getSerializer(componentType);
      } else {
        // TODO add ClassInfo cache for non-final component type.
        this.componentTypeSerializer = null;
      }
      this.stubDims = new int[dimension];
    }

    @Override
    public short getCrossLanguageTypeId() {
      return (short) -Type.LIST.getId();
    }

    @Override
    public void write(MemoryBuffer buffer, T[] arr) {
      int len = arr.length;
      buffer.writeInt(len);
      final Serializer componentTypeSerializer = this.componentTypeSerializer;
      if (componentTypeSerializer != null) {
        ReferenceResolver referenceResolver = fury.getReferenceResolver();
        for (T t : arr) {
          if (!referenceResolver.writeReferenceOrNull(buffer, t)) {
            componentTypeSerializer.write(buffer, t);
          }
        }
      } else {
        for (T t : arr) {
          fury.writeReferencableToJava(buffer, t);
        }
      }
    }

    @Override
    public void crossLanguageWrite(MemoryBuffer buffer, T[] arr) {
      int len = arr.length;
      buffer.writeInt(len);
      // TODO(chaokunyang) use generics by creating component serializers to multi-dimension array.
      for (T t : arr) {
        fury.crossLanguageWriteReferencable(buffer, t);
      }
    }

    @Override
    public T[] read(MemoryBuffer buffer) {
      int numElements = buffer.readInt();
      Object[] value = newArray(numElements);
      ReferenceResolver referenceResolver = fury.getReferenceResolver();
      referenceResolver.reference(value);
      final Serializer componentTypeSerializer = this.componentTypeSerializer;
      if (componentTypeSerializer != null) {
        for (int i = 0; i < numElements; i++) {
          Object elem;
          int nextReadRefId = referenceResolver.tryPreserveReferenceId(buffer);
          if (nextReadRefId >= Fury.NOT_NULL_VALUE_FLAG) {
            elem = componentTypeSerializer.read(buffer);
            referenceResolver.setReadObject(nextReadRefId, elem);
          } else {
            elem = referenceResolver.getReadObject();
          }
          value[i] = elem;
        }
      } else {
        for (int i = 0; i < numElements; i++) {
          value[i] = fury.readReferencableFromJava(buffer);
        }
      }
      return (T[]) value;
    }

    @Override
    public T[] crossLanguageRead(MemoryBuffer buffer) {
      int numElements = buffer.readInt();
      Object[] value = newArray(numElements);
      for (int i = 0; i < numElements; i++) {
        value[i] = fury.crossLanguageReadReferencable(buffer);
      }
      return (T[]) value;
    }

    private Object[] newArray(int numElements) {
      Object[] value;
      if ((Class) type == Object[].class) {
        value = new Object[numElements];
      } else {
        stubDims[0] = numElements;
        value = (Object[]) Array.newInstance(innerType, stubDims);
      }
      return value;
    }
  }

  public static void registerDefaultSerializers(Fury fury) {
    fury.registerSerializer(Object[].class, new ObjectArraySerializer<>(fury, Object[].class));
  }
}
