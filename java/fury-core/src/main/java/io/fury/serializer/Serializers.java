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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Currency;
import java.util.IdentityHashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

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
      if (serializerClass == ObjectSerializer.class) {
        return new ObjectSerializer(fury, type);
      }
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

  public static final class StringBuilderSerializer extends Serializer<StringBuilder> {
    private final StringSerializer stringSerializer;

    public StringBuilderSerializer(Fury fury) {
      super(fury, StringBuilder.class);
      stringSerializer = new StringSerializer(fury);
    }

    @Override
    public void write(MemoryBuffer buffer, StringBuilder value) {
      stringSerializer.writeJavaString(buffer, value.toString());
    }

    @Override
    public StringBuilder read(MemoryBuffer buffer) {
      return new StringBuilder(stringSerializer.readJavaString(buffer));
    }
  }

  public static final class StringBufferSerializer extends Serializer<StringBuffer> {
    private final StringSerializer stringSerializer;

    public StringBufferSerializer(Fury fury) {
      super(fury, StringBuffer.class);
      stringSerializer = new StringSerializer(fury);
    }

    @Override
    public short getCrossLanguageTypeId() {
      return (short) -Type.STRING.getId();
    }

    @Override
    public void write(MemoryBuffer buffer, StringBuffer value) {
      stringSerializer.writeJavaString(buffer, value.toString());
    }

    @Override
    public void crossLanguageWrite(MemoryBuffer buffer, StringBuffer value) {
      stringSerializer.writeUTF8String(buffer, value.toString());
    }

    @Override
    public StringBuffer read(MemoryBuffer buffer) {
      return new StringBuffer(stringSerializer.readJavaString(buffer));
    }

    @Override
    public StringBuffer crossLanguageRead(MemoryBuffer buffer) {
      return new StringBuffer(stringSerializer.readUTF8String(buffer));
    }
  }

  public static final class EnumSerializer extends Serializer<Enum> {
    private final Enum[] enumConstants;

    public EnumSerializer(Fury fury, Class<Enum> cls) {
      super(fury, cls, false);
      if (cls.isEnum()) {
        enumConstants = cls.getEnumConstants();
      } else {
        Preconditions.checkArgument(Enum.class.isAssignableFrom(cls) && cls != Enum.class);
        @SuppressWarnings("unchecked")
        Class<Enum> enclosingClass = (Class<Enum>) cls.getEnclosingClass();
        Preconditions.checkNotNull(enclosingClass);
        Preconditions.checkArgument(enclosingClass.isEnum());
        enumConstants = enclosingClass.getEnumConstants();
      }
    }

    @Override
    public void write(MemoryBuffer buffer, Enum value) {
      buffer.writePositiveVarInt(value.ordinal());
    }

    @Override
    public Enum read(MemoryBuffer buffer) {
      return enumConstants[buffer.readPositiveVarInt()];
    }
  }

  public static final class BigDecimalSerializer extends Serializer<BigDecimal> {
    public BigDecimalSerializer(Fury fury) {
      super(fury, BigDecimal.class);
    }

    @Override
    public void write(MemoryBuffer buffer, BigDecimal value) {
      final byte[] bytes = value.unscaledValue().toByteArray();
      Preconditions.checkArgument(bytes.length <= 16);
      buffer.writeByte((byte) value.scale());
      buffer.writeByte((byte) bytes.length);
      buffer.writeBytes(bytes);
    }

    @Override
    public BigDecimal read(MemoryBuffer buffer) {
      int scale = buffer.readByte();
      int len = buffer.readByte();
      byte[] bytes = buffer.readBytes(len);
      final BigInteger bigInteger = new BigInteger(bytes);
      return new BigDecimal(bigInteger, scale);
    }
  }

  public static final class BigIntegerSerializer extends Serializer<BigInteger> {
    public BigIntegerSerializer(Fury fury) {
      super(fury, BigInteger.class);
    }

    @Override
    public void write(MemoryBuffer buffer, BigInteger value) {
      final byte[] bytes = value.toByteArray();
      Preconditions.checkArgument(bytes.length <= 16);
      buffer.writeByte((byte) bytes.length);
      buffer.writeBytes(bytes);
    }

    @Override
    public BigInteger read(MemoryBuffer buffer) {
      int len = buffer.readByte();
      byte[] bytes = buffer.readBytes(len);
      return new BigInteger(bytes);
    }
  }

  public static final class AtomicBooleanSerializer extends Serializer<AtomicBoolean> {

    public AtomicBooleanSerializer(Fury fury) {
      super(fury, AtomicBoolean.class);
    }

    @Override
    public void write(MemoryBuffer buffer, AtomicBoolean value) {
      buffer.writeBoolean(value.get());
    }

    @Override
    public AtomicBoolean read(MemoryBuffer buffer) {
      return new AtomicBoolean(buffer.readBoolean());
    }
  }

  public static final class AtomicIntegerSerializer extends Serializer<AtomicInteger> {

    public AtomicIntegerSerializer(Fury fury) {
      super(fury, AtomicInteger.class);
    }

    @Override
    public void write(MemoryBuffer buffer, AtomicInteger value) {
      buffer.writeInt(value.get());
    }

    @Override
    public AtomicInteger read(MemoryBuffer buffer) {
      return new AtomicInteger(buffer.readInt());
    }
  }

  public static final class AtomicLongSerializer extends Serializer<AtomicLong> {

    public AtomicLongSerializer(Fury fury) {
      super(fury, AtomicLong.class);
    }

    @Override
    public void write(MemoryBuffer buffer, AtomicLong value) {
      buffer.writeLong(value.get());
    }

    @Override
    public AtomicLong read(MemoryBuffer buffer) {
      return new AtomicLong(buffer.readLong());
    }
  }

  public static final class AtomicReferenceSerializer extends Serializer<AtomicReference> {

    public AtomicReferenceSerializer(Fury fury) {
      super(fury, AtomicReference.class);
    }

    @Override
    public void write(MemoryBuffer buffer, AtomicReference value) {
      fury.writeReferencableToJava(buffer, value.get());
    }

    @Override
    public AtomicReference read(MemoryBuffer buffer) {
      return new AtomicReference(fury.readReferencableFromJava(buffer));
    }
  }

  public static final class CurrencySerializer extends Serializer<Currency> {
    public CurrencySerializer(Fury fury) {
      super(fury, Currency.class);
    }

    @Override
    public void write(MemoryBuffer buffer, Currency object) {
      fury.writeJavaString(buffer, object.getCurrencyCode());
    }

    @Override
    public Currency read(MemoryBuffer buffer) {
      String currencyCode = fury.readJavaString(buffer);
      return Currency.getInstance(currencyCode);
    }
  }

  /** Serializer for {@link Charset}. */
  public static final class CharsetSerializer<T extends Charset> extends Serializer<T> {
    public CharsetSerializer(Fury fury, Class<T> type) {
      super(fury, type);
    }

    public void write(MemoryBuffer buffer, T object) {
      fury.writeJavaString(buffer, object.name());
    }

    public T read(MemoryBuffer buffer) {
      return (T) Charset.forName(fury.readJavaString(buffer));
    }
  }

  public static final class URISerializer extends Serializer<java.net.URI> {

    public URISerializer(Fury fury) {
      super(fury, URI.class);
    }

    @Override
    public void write(MemoryBuffer buffer, final URI uri) {
      fury.writeString(buffer, uri.toString());
    }

    @Override
    public URI read(MemoryBuffer buffer) {
      return URI.create(fury.readString(buffer));
    }
  }

  public static final class RegexSerializer extends Serializer<Pattern> {
    public RegexSerializer(Fury fury) {
      super(fury, Pattern.class);
    }

    @Override
    public void write(MemoryBuffer buffer, Pattern pattern) {
      fury.writeJavaString(buffer, pattern.pattern());
      buffer.writeInt(pattern.flags());
    }

    @Override
    public Pattern read(MemoryBuffer buffer) {
      String regex = fury.readJavaString(buffer);
      int flags = buffer.readInt();
      return Pattern.compile(regex, flags);
    }
  }

  public static final class UUIDSerializer extends Serializer<UUID> {

    public UUIDSerializer(Fury fury) {
      super(fury, UUID.class);
    }

    @Override
    public void write(MemoryBuffer buffer, final UUID uuid) {
      buffer.writeLong(uuid.getMostSignificantBits());
      buffer.writeLong(uuid.getLeastSignificantBits());
    }

    @Override
    public UUID read(MemoryBuffer buffer) {
      return new UUID(buffer.readLong(), buffer.readLong());
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
    fury.registerSerializer(Class.class, new ClassSerializer(fury));
    fury.registerSerializer(StringBuilder.class, new StringBuilderSerializer(fury));
    fury.registerSerializer(StringBuffer.class, new StringBufferSerializer(fury));
    fury.registerSerializer(BigInteger.class, new BigIntegerSerializer(fury));
    fury.registerSerializer(BigDecimal.class, new BigDecimalSerializer(fury));
    fury.registerSerializer(AtomicBoolean.class, new AtomicBooleanSerializer(fury));
    fury.registerSerializer(AtomicInteger.class, new AtomicIntegerSerializer(fury));
    fury.registerSerializer(AtomicLong.class, new AtomicLongSerializer(fury));
    fury.registerSerializer(AtomicReference.class, new AtomicReferenceSerializer(fury));
    fury.registerSerializer(Currency.class, new CurrencySerializer(fury));
    fury.registerSerializer(URI.class, new URISerializer(fury));
    fury.registerSerializer(Pattern.class, new RegexSerializer(fury));
    fury.registerSerializer(UUID.class, new UUIDSerializer(fury));
  }
}
