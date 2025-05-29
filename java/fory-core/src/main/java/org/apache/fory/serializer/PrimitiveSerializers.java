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

package org.apache.fory.serializer;

import static org.apache.fory.type.TypeUtils.PRIMITIVE_LONG_TYPE;

import org.apache.fory.Fory;
import org.apache.fory.codegen.Expression;
import org.apache.fory.codegen.Expression.Invoke;
import org.apache.fory.config.LongEncoding;
import org.apache.fory.memory.MemoryBuffer;
import org.apache.fory.memory.Platform;
import org.apache.fory.resolver.ClassResolver;
import org.apache.fory.util.Preconditions;

/** Serializers for java primitive types. */
@SuppressWarnings({"rawtypes", "unchecked"})
public class PrimitiveSerializers {
  public static final class BooleanSerializer
      extends Serializers.CrossLanguageCompatibleSerializer<Boolean> {
    public BooleanSerializer(Fory fory, Class<?> cls) {
      super(fory, (Class) cls, !(cls.isPrimitive() || fory.isBasicTypesRefIgnored()), true);
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
    public ByteSerializer(Fory fory, Class<?> cls) {
      super(fory, (Class) cls, !(cls.isPrimitive() || fory.isBasicTypesRefIgnored()), true);
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
    public Uint8Serializer(Fory fory) {
      super(fory, Integer.class);
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
    public Uint16Serializer(Fory fory) {
      super(fory, Integer.class);
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

  public static final class CharSerializer extends ImmutableSerializer<Character> {
    public CharSerializer(Fory fory, Class<?> cls) {
      super(fory, (Class) cls, !(cls.isPrimitive() || fory.isBasicTypesRefIgnored()));
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
    public ShortSerializer(Fory fory, Class<?> cls) {
      super(fory, (Class) cls, !(cls.isPrimitive() || fory.isBasicTypesRefIgnored()), true);
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

    public IntSerializer(Fory fory, Class<?> cls) {
      super(fory, (Class) cls, !(cls.isPrimitive() || fory.isBasicTypesRefIgnored()), true);
      compressNumber = fory.compressInt();
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
      buffer.writeVarInt32(value);
    }

    @Override
    public Integer xread(MemoryBuffer buffer) {
      return buffer.readVarInt32();
    }
  }

  public static final class LongSerializer
      extends Serializers.CrossLanguageCompatibleSerializer<Long> {
    private final LongEncoding longEncoding;

    public LongSerializer(Fory fory, Class<?> cls) {
      super(fory, (Class) cls, !(cls.isPrimitive() || fory.isBasicTypesRefIgnored()), true);
      longEncoding = fory.longEncoding();
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
      // TODO(chaokunyang) support var long in cross-language serialization
      buffer.writeVarInt64(value);
    }

    @Override
    public Long xread(MemoryBuffer buffer) {
      return buffer.readVarInt64();
    }
  }

  public static final class FloatSerializer
      extends Serializers.CrossLanguageCompatibleSerializer<Float> {
    public FloatSerializer(Fory fory, Class<?> cls) {
      super(fory, (Class) cls, !(cls.isPrimitive() || fory.isBasicTypesRefIgnored()), true);
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
    public DoubleSerializer(Fory fory, Class<?> cls) {
      super(fory, (Class) cls, !(cls.isPrimitive() || fory.isBasicTypesRefIgnored()), true);
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

  public static void registerDefaultSerializers(Fory fory) {
    // primitive types will be boxed.
    ClassResolver resolver = fory.getClassResolver();
    resolver.registerSerializer(boolean.class, new BooleanSerializer(fory, boolean.class));
    resolver.registerSerializer(byte.class, new ByteSerializer(fory, byte.class));
    resolver.registerSerializer(short.class, new ShortSerializer(fory, short.class));
    resolver.registerSerializer(char.class, new CharSerializer(fory, char.class));
    resolver.registerSerializer(int.class, new IntSerializer(fory, int.class));
    resolver.registerSerializer(long.class, new LongSerializer(fory, long.class));
    resolver.registerSerializer(float.class, new FloatSerializer(fory, float.class));
    resolver.registerSerializer(double.class, new DoubleSerializer(fory, double.class));
    resolver.registerSerializer(Boolean.class, new BooleanSerializer(fory, Boolean.class));
    resolver.registerSerializer(Byte.class, new ByteSerializer(fory, Byte.class));
    resolver.registerSerializer(Short.class, new ShortSerializer(fory, Short.class));
    resolver.registerSerializer(Character.class, new CharSerializer(fory, Character.class));
    resolver.registerSerializer(Integer.class, new IntSerializer(fory, Integer.class));
    resolver.registerSerializer(Long.class, new LongSerializer(fory, Long.class));
    resolver.registerSerializer(Float.class, new FloatSerializer(fory, Float.class));
    resolver.registerSerializer(Double.class, new DoubleSerializer(fory, Double.class));
  }
}
