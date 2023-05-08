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
import io.fury.type.TypeUtils;
import io.fury.util.Platform;
import java.lang.reflect.Array;
import java.lang.reflect.Modifier;
import java.util.IdentityHashMap;

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

  public static final class PrimitiveArrayBufferObject implements BufferObject {
    private final Object array;
    private final int offset;
    private final int elemSize;
    private final int length;

    public PrimitiveArrayBufferObject(Object array, int offset, int elemSize, int length) {
      this.array = array;
      this.offset = offset;
      this.elemSize = elemSize;
      this.length = length;
    }

    @Override
    public int totalBytes() {
      return length * elemSize;
    }

    @Override
    public void writeTo(MemoryBuffer buffer) {
      int size = Math.multiplyExact(length, elemSize);
      int writerIndex = buffer.writerIndex();
      int end = writerIndex + size;
      buffer.ensure(end);
      buffer.copyFromUnsafe(writerIndex, array, offset, size);
      buffer.writerIndex(end);
    }

    @Override
    public MemoryBuffer toBuffer() {
      MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(totalBytes());
      writeTo(buffer);
      return buffer.slice(0, buffer.writerIndex());
    }
  }

  public abstract static class PrimitiveArraySerializer<T>
      extends Serializers.CrossLanguageCompatibleSerializer<T> {
    private final int offset;
    private final int elemSize;

    public PrimitiveArraySerializer(Fury fury, Class<T> cls) {
      super(fury, cls, (short) primitiveInfo.get(TypeUtils.getArrayComponentInfo(cls).f0)[2]);
      Class<?> innerType = TypeUtils.getArrayComponentInfo(cls).f0;
      this.offset = primitiveInfo.get(innerType)[0];
      this.elemSize = primitiveInfo.get(innerType)[1];
    }

    @Override
    public void write(MemoryBuffer buffer, T value) {
      if (fury.getBufferCallback() == null) {
        int size = Math.multiplyExact(length(value), elemSize);
        buffer.writePrimitiveArrayWithSizeEmbedded(value, offset, size);
      } else {
        fury.writeBufferObject(
            buffer, new PrimitiveArrayBufferObject(value, offset, elemSize, length(value)));
      }
    }

    protected abstract int length(T value);

    protected abstract T newInstance(int numElements);

    @Override
    public T read(MemoryBuffer buffer) {
      if (fury.isPeerOutOfBandEnabled()) {
        MemoryBuffer buf = fury.readBufferObject(buffer);
        int size = buf.remaining();
        int numElements = size / elemSize;
        T values = newInstance(numElements);
        buf.copyToUnsafe(0, values, offset, size);
        return values;
      } else {
        int size = buffer.readPositiveVarInt();
        int numElements = size / elemSize;
        T values = newInstance(numElements);
        int readerIndex = buffer.readerIndex();
        buffer.copyToUnsafe(readerIndex, values, offset, size);
        buffer.readerIndex(readerIndex + size);
        return values;
      }
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

  public static final class BooleanArraySerializer extends PrimitiveArraySerializer<boolean[]> {

    public BooleanArraySerializer(Fury fury) {
      super(fury, boolean[].class);
    }

    @Override
    protected int length(boolean[] value) {
      return value.length;
    }

    @Override
    protected boolean[] newInstance(int numElements) {
      return new boolean[numElements];
    }
  }

  public static final class ByteArraySerializer extends PrimitiveArraySerializer<byte[]> {

    public ByteArraySerializer(Fury fury) {
      super(fury, byte[].class);
    }

    @Override
    protected int length(byte[] value) {
      return value.length;
    }

    @Override
    protected byte[] newInstance(int numElements) {
      return new byte[numElements];
    }
  }

  public static final class CharArraySerializer extends PrimitiveArraySerializer<char[]> {

    public CharArraySerializer(Fury fury) {
      super(fury, char[].class);
    }

    @Override
    protected int length(char[] value) {
      return value.length;
    }

    @Override
    protected char[] newInstance(int numElements) {
      return new char[numElements];
    }

    @Override
    public short getCrossLanguageTypeId() {
      return Fury.NOT_SUPPORT_CROSS_LANGUAGE;
    }

    @Override
    public void crossLanguageWrite(MemoryBuffer buffer, char[] value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public char[] crossLanguageRead(MemoryBuffer buffer) {
      throw new UnsupportedOperationException();
    }
  }

  public static final class ShortArraySerializer extends PrimitiveArraySerializer<short[]> {

    public ShortArraySerializer(Fury fury) {
      super(fury, short[].class);
    }

    @Override
    protected int length(short[] value) {
      return value.length;
    }

    @Override
    protected short[] newInstance(int numElements) {
      return new short[numElements];
    }
  }

  public static final class IntArraySerializer extends PrimitiveArraySerializer<int[]> {

    public IntArraySerializer(Fury fury) {
      super(fury, int[].class);
    }

    @Override
    protected int length(int[] value) {
      return value.length;
    }

    @Override
    protected int[] newInstance(int numElements) {
      return new int[numElements];
    }
  }

  public static final class LongArraySerializer extends PrimitiveArraySerializer<long[]> {

    public LongArraySerializer(Fury fury) {
      super(fury, long[].class);
    }

    @Override
    protected int length(long[] value) {
      return value.length;
    }

    @Override
    protected long[] newInstance(int numElements) {
      return new long[numElements];
    }
  }

  public static final class FloatArraySerializer extends PrimitiveArraySerializer<float[]> {

    public FloatArraySerializer(Fury fury) {
      super(fury, float[].class);
    }

    @Override
    protected int length(float[] value) {
      return value.length;
    }

    @Override
    protected float[] newInstance(int numElements) {
      return new float[numElements];
    }
  }

  public static final class DoubleArraySerializer extends PrimitiveArraySerializer<double[]> {

    public DoubleArraySerializer(Fury fury) {
      super(fury, double[].class);
    }

    @Override
    protected int length(double[] value) {
      return value.length;
    }

    @Override
    protected double[] newInstance(int numElements) {
      return new double[numElements];
    }
  }

  public static final class StringArraySerializer extends Serializer<String[]> {
    private final StringSerializer stringSerializer;

    public StringArraySerializer(Fury fury) {
      super(fury, String[].class);
      stringSerializer = new StringSerializer(fury);
    }

    @Override
    public short getCrossLanguageTypeId() {
      return (short) -Type.FURY_STRING_ARRAY.getId();
    }

    @Override
    public void write(MemoryBuffer buffer, String[] value) {
      int len = value.length;
      buffer.writeInt(len);
      for (String elem : value) {
        // TODO reference support
        if (elem != null) {
          buffer.writeByte(Fury.REF_VALUE_FLAG);
          stringSerializer.writeJavaString(buffer, elem);
        } else {
          buffer.writeByte(Fury.NULL_FLAG);
        }
      }
    }

    @Override
    public String[] read(MemoryBuffer buffer) {
      int numElements = buffer.readInt();
      String[] value = new String[numElements];
      fury.getReferenceResolver().reference(value);
      for (int i = 0; i < numElements; i++) {
        if (buffer.readByte() == Fury.REF_VALUE_FLAG) {
          value[i] = stringSerializer.readJavaString(buffer);
        } else {
          value[i] = null;
        }
      }
      return value;
    }

    @Override
    public void crossLanguageWrite(MemoryBuffer buffer, String[] value) {
      int len = value.length;
      buffer.writeInt(len);
      for (String elem : value) {
        if (elem != null) {
          buffer.writeByte(Fury.REF_VALUE_FLAG);
          stringSerializer.writeUTF8String(buffer, elem);
        } else {
          buffer.writeByte(Fury.NULL_FLAG);
        }
      }
    }

    @Override
    public String[] crossLanguageRead(MemoryBuffer buffer) {
      int numElements = buffer.readInt();
      String[] value = new String[numElements];
      for (int i = 0; i < numElements; i++) {
        if (buffer.readByte() == Fury.REF_VALUE_FLAG) {
          value[i] = stringSerializer.readUTF8String(buffer);
        } else {
          value[i] = null;
        }
      }
      return value;
    }
  }

  public static void registerDefaultSerializers(Fury fury) {
    fury.registerSerializer(Object[].class, new ObjectArraySerializer<>(fury, Object[].class));
    fury.registerSerializer(byte[].class, new ByteArraySerializer(fury));
    fury.registerSerializer(char[].class, new CharArraySerializer(fury));
    fury.registerSerializer(short[].class, new ShortArraySerializer(fury));
    fury.registerSerializer(int[].class, new IntArraySerializer(fury));
    fury.registerSerializer(long[].class, new LongArraySerializer(fury));
    fury.registerSerializer(float[].class, new FloatArraySerializer(fury));
    fury.registerSerializer(double[].class, new DoubleArraySerializer(fury));
    fury.registerSerializer(boolean[].class, new BooleanArraySerializer(fury));
    fury.registerSerializer(String[].class, new StringArraySerializer(fury));
  }

  // ########################## utils ##########################

  static void writePrimitiveArray(
      MemoryBuffer buffer, Object arr, int offset, int numElements, int elemSize) {
    int size = Math.multiplyExact(numElements, elemSize);
    buffer.writeInt(size);
    int writerIndex = buffer.writerIndex();
    int end = writerIndex + size;
    buffer.ensure(end);
    buffer.copyFromUnsafe(writerIndex, arr, offset, size);
    buffer.writerIndex(end);
  }

  public static PrimitiveArrayBufferObject byteArrayBufferObject(byte[] array) {
    return new PrimitiveArrayBufferObject(array, Platform.BYTE_ARRAY_OFFSET, 1, array.length);
  }

  static final IdentityHashMap<Class<?>, int[]> primitiveInfo = new IdentityHashMap<>();

  static {
    primitiveInfo.put(
        boolean.class,
        new int[] {Platform.BOOLEAN_ARRAY_OFFSET, 1, Type.FURY_PRIMITIVE_BOOL_ARRAY.getId()});
    primitiveInfo.put(byte.class, new int[] {Platform.BYTE_ARRAY_OFFSET, 1, Type.BINARY.getId()});
    primitiveInfo.put(
        char.class, new int[] {Platform.CHAR_ARRAY_OFFSET, 2, Fury.NOT_SUPPORT_CROSS_LANGUAGE});
    primitiveInfo.put(
        short.class,
        new int[] {Platform.SHORT_ARRAY_OFFSET, 2, Type.FURY_PRIMITIVE_SHORT_ARRAY.getId()});
    primitiveInfo.put(
        int.class, new int[] {Platform.INT_ARRAY_OFFSET, 4, Type.FURY_PRIMITIVE_INT_ARRAY.getId()});
    primitiveInfo.put(
        long.class,
        new int[] {Platform.LONG_ARRAY_OFFSET, 8, Type.FURY_PRIMITIVE_LONG_ARRAY.getId()});
    primitiveInfo.put(
        float.class,
        new int[] {Platform.FLOAT_ARRAY_OFFSET, 4, Type.FURY_PRIMITIVE_FLOAT_ARRAY.getId()});
    primitiveInfo.put(
        double.class,
        new int[] {Platform.DOUBLE_ARRAY_OFFSET, 8, Type.FURY_PRIMITIVE_DOUBLE_ARRAY.getId()});
  }
}
