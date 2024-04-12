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

import static org.apache.fury.type.TypeUtils.PRIMITIVE_LONG_TYPE;

import org.apache.fury.Fury;
import org.apache.fury.codegen.Expression;
import org.apache.fury.codegen.Expression.Invoke;
import org.apache.fury.config.LongEncoding;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.type.Type;
import org.apache.fury.util.Platform;
import org.apache.fury.util.Preconditions;

/** Serializers for java primitive types. */
@SuppressWarnings({"rawtypes", "unchecked"})
public class PrimitiveSerializers {
  public static final class BooleanSerializer
      extends Serializers.CrossLanguageCompatibleSerializer<Boolean> {
    public BooleanSerializer(Fury fury, Class<?> cls) {
      super(
          fury,
          (Class) cls,
          Type.BOOL.getId(),
          !(cls.isPrimitive() || fury.isBasicTypesRefIgnored()));
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

  public static final class ByteSerializer
      extends Serializers.CrossLanguageCompatibleSerializer<Byte> {
    public ByteSerializer(Fury fury, Class<?> cls) {
      super(
          fury,
          (Class) cls,
          Type.INT8.getId(),
          !(cls.isPrimitive() || fury.isBasicTypesRefIgnored()));
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
    public short getXtypeId() {
      return Type.UINT8.getId();
    }

    @Override
    public void xwrite(MemoryBuffer buffer, Integer value) {
      Preconditions.checkArgument(value >= 0 && value <= 255);
      buffer.writeByte(value.byteValue());
    }

    @Override
    public Integer xread(MemoryBuffer buffer) {
      int b = buffer.readByte();
      return b >>> 24;
    }
  }

  public static final class Uint16Serializer extends Serializer<Integer> {
    public Uint16Serializer(Fury fury) {
      super(fury, Integer.class);
    }

    @Override
    public short getXtypeId() {
      return Type.UINT16.getId();
    }

    @Override
    public void xwrite(MemoryBuffer buffer, Integer value) {
      Preconditions.checkArgument(value >= 0 && value <= 65535);
      buffer.writeByte(value.byteValue());
    }

    @Override
    public Integer xread(MemoryBuffer buffer) {
      int b = buffer.readByte();
      return b >>> 16;
    }
  }

  public static final class CharSerializer extends Serializer<Character> {
    public CharSerializer(Fury fury, Class<?> cls) {
      super(fury, (Class) cls, !(cls.isPrimitive() || fury.isBasicTypesRefIgnored()));
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

  public static final class ShortSerializer
      extends Serializers.CrossLanguageCompatibleSerializer<Short> {
    public ShortSerializer(Fury fury, Class<?> cls) {
      super(
          fury,
          (Class) cls,
          Type.INT16.getId(),
          !(cls.isPrimitive() || fury.isBasicTypesRefIgnored()));
    }

    @Override
    public void write(MemoryBuffer buffer, Short value) {
      buffer.writeInt16(value);
    }

    @Override
    public Short read(MemoryBuffer buffer) {
      return buffer.readInt16();
    }
  }

  public static final class IntSerializer
      extends Serializers.CrossLanguageCompatibleSerializer<Integer> {
    private final boolean compressNumber;

    public IntSerializer(Fury fury, Class<?> cls) {
      super(
          fury,
          (Class) cls,
          Type.INT32.getId(),
          !(cls.isPrimitive() || fury.isBasicTypesRefIgnored()));
      compressNumber = fury.compressInt();
    }

    @Override
    public void write(MemoryBuffer buffer, Integer value) {
      if (compressNumber) {
        buffer.writeVarInt32(value);
      } else {
        buffer.writeInt32(value);
      }
    }

    @Override
    public Integer read(MemoryBuffer buffer) {
      if (compressNumber) {
        return buffer.readVarInt32();
      } else {
        return buffer.readInt32();
      }
    }

    @Override
    public void xwrite(MemoryBuffer buffer, Integer value) {
      // TODO support varint in cross-language serialization
      buffer.writeInt32(value);
    }

    @Override
    public Integer xread(MemoryBuffer buffer) {
      return buffer.readInt32();
    }
  }

  public static final class LongSerializer
      extends Serializers.CrossLanguageCompatibleSerializer<Long> {
    private final LongEncoding longEncoding;

    public LongSerializer(Fury fury, Class<?> cls) {
      super(
          fury,
          (Class) cls,
          Type.INT64.getId(),
          !(cls.isPrimitive() || fury.isBasicTypesRefIgnored()));
      longEncoding = fury.longEncoding();
    }

    @Override
    public void write(MemoryBuffer buffer, Long value) {
      writeInt64(buffer, value, longEncoding);
    }

    @Override
    public Long read(MemoryBuffer buffer) {
      return readInt64(buffer, longEncoding);
    }

    public static Expression writeInt64(
        Expression buffer, Expression v, LongEncoding longEncoding, boolean ensureBounds) {
      switch (longEncoding) {
        case LE_RAW_BYTES:
          return new Invoke(buffer, "writeInt64", v);
        case SLI:
          return new Invoke(buffer, ensureBounds ? "writeSliInt64" : "_unsafeWriteSliInt64", v);
        case PVL:
          return new Invoke(buffer, ensureBounds ? "writeVarInt64" : "_unsafeWriteVarInt64", v);
        default:
          throw new UnsupportedOperationException("Unsupported long encoding " + longEncoding);
      }
    }

    public static void writeInt64(MemoryBuffer buffer, long value, LongEncoding longEncoding) {
      if (longEncoding == LongEncoding.SLI) {
        buffer.writeSliInt64(value);
      } else if (longEncoding == LongEncoding.LE_RAW_BYTES) {
        buffer.writeInt64(value);
      } else {
        buffer.writeVarInt64(value);
      }
    }

    public static long readInt64(MemoryBuffer buffer, LongEncoding longEncoding) {
      if (longEncoding == LongEncoding.SLI) {
        return buffer.readSliInt64();
      } else if (longEncoding == LongEncoding.LE_RAW_BYTES) {
        return buffer.readInt64();
      } else {
        return buffer.readVarInt64();
      }
    }

    public static Expression readInt64(Expression buffer, LongEncoding longEncoding) {
      return new Invoke(buffer, readLongFunc(longEncoding), PRIMITIVE_LONG_TYPE);
    }

    public static String readLongFunc(LongEncoding longEncoding) {
      switch (longEncoding) {
        case LE_RAW_BYTES:
          return Platform.IS_LITTLE_ENDIAN ? "_readInt64OnLE" : "_readInt64OnBE";
        case SLI:
          return Platform.IS_LITTLE_ENDIAN ? "_readSliInt64OnLE" : "_readSliInt64OnBE";
        case PVL:
          return Platform.IS_LITTLE_ENDIAN ? "_readVarInt64OnLE" : "_readVarInt64OnBE";
        default:
          throw new UnsupportedOperationException("Unsupported long encoding " + longEncoding);
      }
    }

    @Override
    public void xwrite(MemoryBuffer buffer, Long value) {
      // TODO support var long in cross-language serialization
      buffer.writeInt64(value);
    }

    @Override
    public Long xread(MemoryBuffer buffer) {
      return buffer.readInt64();
    }
  }

  public static final class FloatSerializer
      extends Serializers.CrossLanguageCompatibleSerializer<Float> {
    public FloatSerializer(Fury fury, Class<?> cls) {
      super(
          fury,
          (Class) cls,
          Type.FLOAT.getId(),
          !(cls.isPrimitive() || fury.isBasicTypesRefIgnored()));
    }

    @Override
    public void write(MemoryBuffer buffer, Float value) {
      buffer.writeFloat32(value);
    }

    @Override
    public Float read(MemoryBuffer buffer) {
      return buffer.readFloat32();
    }
  }

  public static final class DoubleSerializer
      extends Serializers.CrossLanguageCompatibleSerializer<Double> {
    public DoubleSerializer(Fury fury, Class<?> cls) {
      super(
          fury,
          (Class) cls,
          Type.DOUBLE.getId(),
          !(cls.isPrimitive() || fury.isBasicTypesRefIgnored()));
    }

    @Override
    public void write(MemoryBuffer buffer, Double value) {
      buffer.writeFloat64(value);
    }

    @Override
    public Double read(MemoryBuffer buffer) {
      return buffer.readFloat64();
    }
  }

  public static void registerDefaultSerializers(Fury fury) {
    // primitive types will be boxed.
    fury.registerSerializer(boolean.class, new BooleanSerializer(fury, boolean.class));
    fury.registerSerializer(byte.class, new ByteSerializer(fury, byte.class));
    fury.registerSerializer(short.class, new ShortSerializer(fury, short.class));
    fury.registerSerializer(char.class, new CharSerializer(fury, char.class));
    fury.registerSerializer(int.class, new IntSerializer(fury, int.class));
    fury.registerSerializer(long.class, new LongSerializer(fury, long.class));
    fury.registerSerializer(float.class, new FloatSerializer(fury, float.class));
    fury.registerSerializer(double.class, new DoubleSerializer(fury, double.class));
    fury.registerSerializer(Boolean.class, new BooleanSerializer(fury, Boolean.class));
    fury.registerSerializer(Byte.class, new ByteSerializer(fury, Byte.class));
    fury.registerSerializer(Short.class, new ShortSerializer(fury, Short.class));
    fury.registerSerializer(Character.class, new CharSerializer(fury, Character.class));
    fury.registerSerializer(Integer.class, new IntSerializer(fury, Integer.class));
    fury.registerSerializer(Long.class, new LongSerializer(fury, Long.class));
    fury.registerSerializer(Float.class, new FloatSerializer(fury, Float.class));
    fury.registerSerializer(Double.class, new DoubleSerializer(fury, Double.class));
  }
}
