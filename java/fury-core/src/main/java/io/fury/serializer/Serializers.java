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
import com.google.common.primitives.Primitives;
import io.fury.Fury;
import io.fury.memory.MemoryBuffer;
import io.fury.type.Type;
import io.fury.util.Platform;
import io.fury.util.Utils;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.IdentityHashMap;

/**
 * Serialization utils and common serializers.
 *
 * @author chaokunyang
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class Serializers {

  /**
   * Serializer subclass must have a constructor which take parameters of type {@link Fury} and
   * {@link Class}, or {@link Fury} or {@link Class} or no-arg constructor.
   */
  public static <T> Serializer<T> newSerializer(
      Fury fury, Class type, Class<? extends Serializer> serializerClass) {
    Serializer serializer = fury.getClassResolver().getSerializer(type, false);
    try {
      try {
        try {
          Constructor<? extends Serializer> ctr =
              serializerClass.getConstructor(Fury.class, Class.class);
          ctr.setAccessible(true);
          return ctr.newInstance(fury, type);
        } catch (NoSuchMethodException e) {
          Utils.ignore(e);
        }
        try {
          Constructor<? extends Serializer> ctr = serializerClass.getConstructor(Fury.class);
          ctr.setAccessible(true);
          return ctr.newInstance(fury);
        } catch (NoSuchMethodException e) {
          Utils.ignore(e);
        }
        try {
          Constructor<? extends Serializer> ctr = serializerClass.getConstructor(Class.class);
          ctr.setAccessible(true);
          return ctr.newInstance(type);
        } catch (NoSuchMethodException e) {
          Utils.ignore(e);
        }
        return serializerClass.newInstance();
      } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
        Platform.throwException(e);
      }
    } catch (Throwable t) {
      // Some serializer may set itself in constructor as serializer, but the
      // constructor failed later. For example, some final type field doesn't
      // support serialization.
      fury.getClassResolver().resetSerializer(type, serializer);
      Platform.throwException(t);
    }
    throw new IllegalStateException("unreachable");
  }

  public abstract static class CrossLanguageCompatibleSerializer<T> extends Serializer<T> {
    private final short typeId;

    public CrossLanguageCompatibleSerializer(Fury fury, Class<T> cls, short typeId) {
      super(fury, cls);
      this.typeId = typeId;
    }

    public CrossLanguageCompatibleSerializer(
        Fury fury, Class<T> cls, short typeId, boolean needToWriteReference) {
      super(fury, cls, needToWriteReference);
      this.typeId = typeId;
    }

    @Override
    public short getCrossLanguageTypeId() {
      return typeId;
    }

    @Override
    public void crossLanguageWrite(MemoryBuffer buffer, T value) {
      write(buffer, value);
    }

    @Override
    public T crossLanguageRead(MemoryBuffer buffer) {
      return read(buffer);
    }
  }

  public static final class BooleanSerializer extends CrossLanguageCompatibleSerializer<Boolean> {
    public BooleanSerializer(Fury fury, Class<?> cls) {
      super(
          fury,
          (Class) cls,
          Type.BOOL.getId(),
          !(cls.isPrimitive() || fury.isBasicTypesReferenceIgnored()));
    }

    @Override
    public void write(MemoryBuffer buffer, Boolean value) {
      buffer.writeBoolean(value);
    }

    @Override
    public Boolean read(MemoryBuffer buffer) {
      return buffer.readBoolean();
    }
  }

  public static final class ByteSerializer extends CrossLanguageCompatibleSerializer<Byte> {
    public ByteSerializer(Fury fury, Class<?> cls) {
      super(
          fury,
          (Class) cls,
          Type.INT8.getId(),
          !(cls.isPrimitive() || fury.isBasicTypesReferenceIgnored()));
    }

    @Override
    public void write(MemoryBuffer buffer, Byte value) {
      buffer.writeByte(value);
    }

    @Override
    public Byte read(MemoryBuffer buffer) {
      return buffer.readByte();
    }
  }

  public static final class Uint8Serializer extends Serializer<Integer> {
    public Uint8Serializer(Fury fury) {
      super(fury, Integer.class);
    }

    @Override
    public short getCrossLanguageTypeId() {
      return Type.UINT8.getId();
    }

    @Override
    public void crossLanguageWrite(MemoryBuffer buffer, Integer value) {
      Preconditions.checkArgument(value >= 0 && value <= 255);
      buffer.writeByte(value.byteValue());
    }

    @Override
    public Integer crossLanguageRead(MemoryBuffer buffer) {
      int b = buffer.readByte();
      return b >>> 24;
    }
  }

  public static final class Uint16Serializer extends Serializer<Integer> {
    public Uint16Serializer(Fury fury) {
      super(fury, Integer.class);
    }

    @Override
    public short getCrossLanguageTypeId() {
      return Type.UINT16.getId();
    }

    @Override
    public void crossLanguageWrite(MemoryBuffer buffer, Integer value) {
      Preconditions.checkArgument(value >= 0 && value <= 65535);
      buffer.writeByte(value.byteValue());
    }

    @Override
    public Integer crossLanguageRead(MemoryBuffer buffer) {
      int b = buffer.readByte();
      return b >>> 16;
    }
  }

  public static final class CharSerializer extends Serializer<Character> {
    public CharSerializer(Fury fury, Class<?> cls) {
      super(fury, (Class) cls, !(cls.isPrimitive() || fury.isBasicTypesReferenceIgnored()));
    }

    @Override
    public void write(MemoryBuffer buffer, Character value) {
      buffer.writeChar(value);
    }

    @Override
    public Character read(MemoryBuffer buffer) {
      return buffer.readChar();
    }
  }

  public static final class ShortSerializer extends CrossLanguageCompatibleSerializer<Short> {
    public ShortSerializer(Fury fury, Class<?> cls) {
      super(
          fury,
          (Class) cls,
          Type.INT16.getId(),
          !(cls.isPrimitive() || fury.isBasicTypesReferenceIgnored()));
    }

    @Override
    public void write(MemoryBuffer buffer, Short value) {
      buffer.writeShort(value);
    }

    @Override
    public Short read(MemoryBuffer buffer) {
      return buffer.readShort();
    }
  }

  public static final class IntSerializer extends CrossLanguageCompatibleSerializer<Integer> {
    private final boolean compressNumber;

    public IntSerializer(Fury fury, Class<?> cls) {
      super(
          fury,
          (Class) cls,
          Type.INT32.getId(),
          !(cls.isPrimitive() || fury.isBasicTypesReferenceIgnored()));
      compressNumber = fury.compressNumber();
    }

    @Override
    public void write(MemoryBuffer buffer, Integer value) {
      if (compressNumber) {
        buffer.writeVarInt(value);
      } else {
        buffer.writeInt(value);
      }
    }

    @Override
    public Integer read(MemoryBuffer buffer) {
      if (compressNumber) {
        return buffer.readVarInt();
      } else {
        return buffer.readInt();
      }
    }

    @Override
    public void crossLanguageWrite(MemoryBuffer buffer, Integer value) {
      // TODO support varint in cross-language serialization
      buffer.writeInt(value);
    }

    @Override
    public Integer crossLanguageRead(MemoryBuffer buffer) {
      return buffer.readInt();
    }
  }

  public static final class LongSerializer extends CrossLanguageCompatibleSerializer<Long> {
    private final boolean compressNumber;

    public LongSerializer(Fury fury, Class<?> cls) {
      super(
          fury,
          (Class) cls,
          Type.INT64.getId(),
          !(cls.isPrimitive() || fury.isBasicTypesReferenceIgnored()));
      compressNumber = fury.compressNumber();
    }

    @Override
    public void write(MemoryBuffer buffer, Long value) {
      if (compressNumber) {
        buffer.writeVarLong(value);
      } else {
        buffer.writeLong(value);
      }
    }

    @Override
    public Long read(MemoryBuffer buffer) {
      if (compressNumber) {
        return buffer.readVarLong();
      } else {
        return buffer.readLong();
      }
    }

    @Override
    public void crossLanguageWrite(MemoryBuffer buffer, Long value) {
      // TODO support var long in cross-language serialization
      buffer.writeLong(value);
    }

    @Override
    public Long crossLanguageRead(MemoryBuffer buffer) {
      return buffer.readLong();
    }
  }

  public static final class FloatSerializer extends CrossLanguageCompatibleSerializer<Float> {
    public FloatSerializer(Fury fury, Class<?> cls) {
      super(
          fury,
          (Class) cls,
          Type.FLOAT.getId(),
          !(cls.isPrimitive() || fury.isBasicTypesReferenceIgnored()));
    }

    @Override
    public void write(MemoryBuffer buffer, Float value) {
      buffer.writeFloat(value);
    }

    @Override
    public Float read(MemoryBuffer buffer) {
      return buffer.readFloat();
    }
  }

  public static final class DoubleSerializer extends CrossLanguageCompatibleSerializer<Double> {
    public DoubleSerializer(Fury fury, Class<?> cls) {
      super(
          fury,
          (Class) cls,
          Type.DOUBLE.getId(),
          !(cls.isPrimitive() || fury.isBasicTypesReferenceIgnored()));
    }

    @Override
    public void write(MemoryBuffer buffer, Double value) {
      buffer.writeDouble(value);
    }

    @Override
    public Double read(MemoryBuffer buffer) {
      return buffer.readDouble();
    }
  }

  public static final class ClassSerializer extends Serializer<Class> {
    private static final byte USE_CLASS_ID = 0;
    private static final byte USE_CLASSNAME = 1;
    private static final byte PRIMITIVE_FLAG = 2;
    private final IdentityHashMap<Class<?>, Byte> primitivesMap = new IdentityHashMap<>();
    private final Class<?>[] id2PrimitiveClasses = new Class[Primitives.allPrimitiveTypes().size()];

    public ClassSerializer(Fury fury) {
      super(fury, Class.class);
      byte count = 0;
      for (Class<?> primitiveType : Primitives.allPrimitiveTypes()) {
        primitivesMap.put(primitiveType, count);
        id2PrimitiveClasses[count] = primitiveType;
        count++;
      }
    }

    @Override
    public void write(MemoryBuffer buffer, Class value) {
      fury.getClassResolver().writeClassInternal(buffer, value);
    }

    @Override
    public Class read(MemoryBuffer buffer) {
      return fury.getClassResolver().readClassInternal(buffer);
    }
  }
}
