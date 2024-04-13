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

import static org.apache.fury.util.function.Functions.makeGetterFunction;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Currency;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.regex.Pattern;
import org.apache.fury.Fury;
import org.apache.fury.collection.Tuple2;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.type.Type;
import org.apache.fury.util.GraalvmSupport;
import org.apache.fury.util.Platform;
import org.apache.fury.util.Preconditions;
import org.apache.fury.util.ReflectionUtils;
import org.apache.fury.util.Utils;
import org.apache.fury.util.unsafe._JDKAccess;

/** Serialization utils and common serializers. */
@SuppressWarnings({"rawtypes", "unchecked"})
public class Serializers {
  // avoid duplicate reflect inspection and cache for graalvm support too.
  private static final Cache<Class, Tuple2<MethodType, MethodHandle>> CTR_MAP;

  static {
    if (GraalvmSupport.isGraalBuildtime()) {
      CTR_MAP = CacheBuilder.newBuilder().concurrencyLevel(32).build();
    } else {
      CTR_MAP = CacheBuilder.newBuilder().weakKeys().softValues().build();
    }
  }

  private static final MethodType SIG1 = MethodType.methodType(void.class, Fury.class, Class.class);
  private static final MethodType SIG2 = MethodType.methodType(void.class, Fury.class);
  private static final MethodType SIG3 = MethodType.methodType(void.class, Class.class);
  private static final MethodType SIG4 = MethodType.methodType(void.class);

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
      if (serializerClass == CompatibleSerializer.class) {
        return new CompatibleSerializer(fury, type);
      }
      Tuple2<MethodType, MethodHandle> ctrInfo = CTR_MAP.getIfPresent(serializerClass);
      if (ctrInfo != null) {
        MethodType sig = ctrInfo.f0;
        MethodHandle handle = ctrInfo.f1;
        if (sig.equals(SIG1)) {
          return (Serializer<T>) handle.invoke(fury, type);
        } else if (sig.equals(SIG2)) {
          return (Serializer<T>) handle.invoke(fury);
        } else if (sig.equals(SIG3)) {
          return (Serializer<T>) handle.invoke(type);
        } else {
          return (Serializer<T>) handle.invoke();
        }
      } else {
        return createSerializer(fury, type, serializerClass);
      }
    } catch (InvocationTargetException e) {
      fury.getClassResolver().resetSerializer(type, serializer);
      if (e.getCause() != null) {
        Platform.throwException(e.getCause());
      } else {
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

  private static <T> Serializer<T> createSerializer(
      Fury fury, Class<?> type, Class<? extends Serializer> serializerClass) throws Throwable {
    MethodHandles.Lookup lookup = _JDKAccess._trustedLookup(serializerClass);
    try {
      MethodHandle ctr = lookup.findConstructor(serializerClass, SIG1);
      CTR_MAP.put(serializerClass, Tuple2.of(SIG1, ctr));
      return (Serializer<T>) ctr.invoke(fury, type);
    } catch (NoSuchMethodException e) {
      Utils.ignore(e);
    }
    try {
      MethodHandle ctr = lookup.findConstructor(serializerClass, SIG2);
      CTR_MAP.put(serializerClass, Tuple2.of(SIG2, ctr));
      return (Serializer<T>) ctr.invoke(fury);
    } catch (NoSuchMethodException e) {
      Utils.ignore(e);
    }
    try {
      MethodHandle ctr = lookup.findConstructor(serializerClass, SIG3);
      CTR_MAP.put(serializerClass, Tuple2.of(SIG3, ctr));
      return (Serializer<T>) ctr.invoke(type);
    } catch (NoSuchMethodException e) {
      MethodHandle ctr = ReflectionUtils.getCtrHandle(serializerClass);
      CTR_MAP.put(serializerClass, Tuple2.of(SIG4, ctr));
      return (Serializer<T>) ctr.invoke();
    }
  }

  public static Object readPrimitiveValue(Fury fury, MemoryBuffer buffer, short classId) {
    switch (classId) {
      case ClassResolver.PRIMITIVE_BOOLEAN_CLASS_ID:
        return buffer.readBoolean();
      case ClassResolver.PRIMITIVE_BYTE_CLASS_ID:
        return buffer.readByte();
      case ClassResolver.PRIMITIVE_CHAR_CLASS_ID:
        return buffer.readChar();
      case ClassResolver.PRIMITIVE_SHORT_CLASS_ID:
        return buffer.readInt16();
      case ClassResolver.PRIMITIVE_INT_CLASS_ID:
        if (fury.compressInt()) {
          return buffer.readVarInt32();
        } else {
          return buffer.readInt32();
        }
      case ClassResolver.PRIMITIVE_FLOAT_CLASS_ID:
        return buffer.readFloat32();
      case ClassResolver.PRIMITIVE_LONG_CLASS_ID:
        return fury.readInt64(buffer);
      case ClassResolver.PRIMITIVE_DOUBLE_CLASS_ID:
        return buffer.readFloat64();
      default:
        {
          throw new IllegalStateException("unreachable");
        }
    }
  }

  public abstract static class CrossLanguageCompatibleSerializer<T> extends Serializer<T> {
    private final short typeId;

    public CrossLanguageCompatibleSerializer(Fury fury, Class<T> cls, short typeId) {
      super(fury, cls);
      this.typeId = typeId;
    }

    public CrossLanguageCompatibleSerializer(
        Fury fury, Class<T> cls, short typeId, boolean needToWriteRef) {
      super(fury, cls, needToWriteRef);
      this.typeId = typeId;
    }

    @Override
    public short getXtypeId() {
      return typeId;
    }

    @Override
    public void xwrite(MemoryBuffer buffer, T value) {
      write(buffer, value);
    }

    @Override
    public T xread(MemoryBuffer buffer) {
      return read(buffer);
    }
  }

  private static final ToIntFunction GET_CODER;
  private static final Function GET_VALUE;

  static {
    GET_VALUE = (Function) makeGetterFunction(StringBuilder.class.getSuperclass(), "getValue");
    ToIntFunction<CharSequence> getCoder;
    try {
      Method getCoderMethod = StringBuilder.class.getSuperclass().getDeclaredMethod("getCoder");
      getCoder = (ToIntFunction<CharSequence>) makeGetterFunction(getCoderMethod, int.class);
    } catch (NoSuchMethodException e) {
      getCoder = null;
    }
    GET_CODER = getCoder;
  }

  public abstract static class AbstractStringBuilderSerializer<T extends CharSequence>
      extends Serializer<T> {
    protected final StringSerializer stringSerializer;

    public AbstractStringBuilderSerializer(Fury fury, Class<T> type) {
      super(fury, type);
      stringSerializer = new StringSerializer(fury);
    }

    @Override
    public void xwrite(MemoryBuffer buffer, T value) {
      stringSerializer.writeUTF8String(buffer, value.toString());
    }

    @Override
    public short getXtypeId() {
      return (short) -Type.STRING.getId();
    }

    @Override
    public void write(MemoryBuffer buffer, T value) {
      if (GET_CODER != null) {
        int coder = GET_CODER.applyAsInt(value);
        byte[] v = (byte[]) GET_VALUE.apply(value);
        int bytesLen = value.length();
        if (coder != 0) {
          if (coder != 1) {
            throw new UnsupportedOperationException("Unsupported coder " + coder);
          }
          bytesLen <<= 1;
        }
        long header = ((long) bytesLen << 2) | coder;
        buffer.writeVarUint64(header);
        buffer.writeBytes(v, 0, bytesLen);
      } else {
        char[] v = (char[]) GET_VALUE.apply(value);
        if (StringSerializer.isLatin(v)) {
          stringSerializer.writeCharsLatin(buffer, v, value.length());
        } else {
          stringSerializer.writeCharsUTF16(buffer, v, value.length());
        }
      }
    }
  }

  public static final class StringBuilderSerializer
      extends AbstractStringBuilderSerializer<StringBuilder> {

    public StringBuilderSerializer(Fury fury) {
      super(fury, StringBuilder.class);
    }

    @Override
    public StringBuilder read(MemoryBuffer buffer) {
      return new StringBuilder(stringSerializer.readJavaString(buffer));
    }

    @Override
    public StringBuilder xread(MemoryBuffer buffer) {
      return new StringBuilder(stringSerializer.readUTF8String(buffer));
    }
  }

  public static final class StringBufferSerializer
      extends AbstractStringBuilderSerializer<StringBuffer> {

    public StringBufferSerializer(Fury fury) {
      super(fury, StringBuffer.class);
    }

    @Override
    public StringBuffer read(MemoryBuffer buffer) {
      return new StringBuffer(stringSerializer.readJavaString(buffer));
    }

    @Override
    public StringBuffer xread(MemoryBuffer buffer) {
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
      buffer.writeVarUint32Small7(value.ordinal());
    }

    @Override
    public Enum read(MemoryBuffer buffer) {
      return enumConstants[buffer.readVarUint32Small7()];
    }
  }

  public static final class BigDecimalSerializer extends Serializer<BigDecimal> {
    public BigDecimalSerializer(Fury fury) {
      super(fury, BigDecimal.class);
    }

    @Override
    public void write(MemoryBuffer buffer, BigDecimal value) {
      final byte[] bytes = value.unscaledValue().toByteArray();
      buffer.writeVarUint32Small7(value.scale());
      buffer.writeVarUint32Small7(value.precision());
      buffer.writeVarUint32Small7(bytes.length);
      buffer.writeBytes(bytes);
    }

    @Override
    public BigDecimal read(MemoryBuffer buffer) {
      int scale = buffer.readVarUint32Small7();
      int precision = buffer.readVarUint32Small7();
      int len = buffer.readVarUint32Small7();
      byte[] bytes = buffer.readBytes(len);
      final BigInteger bigInteger = new BigInteger(bytes);
      return new BigDecimal(bigInteger, scale, new MathContext(precision));
    }
  }

  public static final class BigIntegerSerializer extends Serializer<BigInteger> {
    public BigIntegerSerializer(Fury fury) {
      super(fury, BigInteger.class);
    }

    @Override
    public void write(MemoryBuffer buffer, BigInteger value) {
      final byte[] bytes = value.toByteArray();
      buffer.writeVarUint32Small7(bytes.length);
      buffer.writeBytes(bytes);
    }

    @Override
    public BigInteger read(MemoryBuffer buffer) {
      int len = buffer.readVarUint32Small7();
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
      buffer.writeInt32(value.get());
    }

    @Override
    public AtomicInteger read(MemoryBuffer buffer) {
      return new AtomicInteger(buffer.readInt32());
    }
  }

  public static final class AtomicLongSerializer extends Serializer<AtomicLong> {

    public AtomicLongSerializer(Fury fury) {
      super(fury, AtomicLong.class);
    }

    @Override
    public void write(MemoryBuffer buffer, AtomicLong value) {
      buffer.writeInt64(value.get());
    }

    @Override
    public AtomicLong read(MemoryBuffer buffer) {
      return new AtomicLong(buffer.readInt64());
    }
  }

  public static final class AtomicReferenceSerializer extends Serializer<AtomicReference> {

    public AtomicReferenceSerializer(Fury fury) {
      super(fury, AtomicReference.class);
    }

    @Override
    public void write(MemoryBuffer buffer, AtomicReference value) {
      fury.writeRef(buffer, value.get());
    }

    @Override
    public AtomicReference read(MemoryBuffer buffer) {
      return new AtomicReference(fury.readRef(buffer));
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
      buffer.writeInt32(pattern.flags());
    }

    @Override
    public Pattern read(MemoryBuffer buffer) {
      String regex = fury.readJavaString(buffer);
      int flags = buffer.readInt32();
      return Pattern.compile(regex, flags);
    }
  }

  public static final class UUIDSerializer extends Serializer<UUID> {

    public UUIDSerializer(Fury fury) {
      super(fury, UUID.class);
    }

    @Override
    public void write(MemoryBuffer buffer, final UUID uuid) {
      buffer.writeInt64(uuid.getMostSignificantBits());
      buffer.writeInt64(uuid.getLeastSignificantBits());
    }

    @Override
    public UUID read(MemoryBuffer buffer) {
      return new UUID(buffer.readInt64(), buffer.readInt64());
    }
  }

  public static final class ClassSerializer extends Serializer<Class> {
    public ClassSerializer(Fury fury) {
      super(fury, Class.class);
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

  /**
   * Serializer for empty object of type {@link Object}. Fury disabled serialization for jdk
   * internal types which doesn't implement {@link java.io.Serializable} for security, but empty
   * object is safe and used sometimes, so fury should support its serialization without disable
   * serializable or class registration checks.
   */
  // Use a separate serializer to avoid codegen for emtpy object.
  public static final class EmptyObjectSerializer extends Serializer<Object> {

    public EmptyObjectSerializer(Fury fury) {
      super(fury, Object.class);
    }

    @Override
    public void write(MemoryBuffer buffer, Object value) {}

    @Override
    public Object read(MemoryBuffer buffer) {
      return new Object();
    }
  }

  public static void registerDefaultSerializers(Fury fury) {
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
    fury.registerSerializer(Object.class, new EmptyObjectSerializer(fury));
  }
}
