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

import java.lang.reflect.Array;
import java.util.IdentityHashMap;
import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.resolver.ClassInfo;
import org.apache.fury.resolver.ClassInfoHolder;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.RefResolver;
import org.apache.fury.serializer.collection.ArrayAsList;
import org.apache.fury.serializer.collection.CollectionFlags;
import org.apache.fury.serializer.collection.FuryArrayAsListSerializer;
import org.apache.fury.type.Type;
import org.apache.fury.type.TypeUtils;
import org.apache.fury.util.Platform;
import org.apache.fury.util.Preconditions;
import org.apache.fury.util.ReflectionUtils;

/** Serializers for array types. */
public class ArraySerializers {

  /** May be multi-dimension array, or multi-dimension primitive array. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static final class ObjectArraySerializer<T> extends Serializer<T[]> {
    private final Class<T> innerType;
    private final Serializer componentTypeSerializer;
    private final ClassInfoHolder classInfoHolder;
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
      if (ReflectionUtils.isMonomorphic(componentType)) {
        this.componentTypeSerializer = fury.getClassResolver().getSerializer(componentType);
      } else {
        // TODO add ClassInfo cache for non-final component type.
        this.componentTypeSerializer = null;
      }
      this.stubDims = new int[dimension];
      classInfoHolder = fury.getClassResolver().nilClassInfoHolder();
    }

    @Override
    public short getXtypeId() {
      return (short) -Type.LIST.getId();
    }

    @Override
    public void write(MemoryBuffer buffer, T[] arr) {
      int len = arr.length;
      buffer.writeVarUint32Small7(len);
      RefResolver refResolver = fury.getRefResolver();
      Serializer componentSerializer = this.componentTypeSerializer;
      if (componentSerializer != null) {
        for (T t : arr) {
          if (!refResolver.writeRefOrNull(buffer, t)) {
            componentSerializer.write(buffer, t);
          }
        }
      } else {
        Fury fury = this.fury;
        ClassResolver classResolver = fury.getClassResolver();
        ClassInfo classInfo = null;
        Class<?> elemClass = null;
        for (T t : arr) {
          if (!refResolver.writeRefOrNull(buffer, t)) {
            Class<?> clz = t.getClass();
            if (clz != elemClass) {
              elemClass = clz;
              classInfo = classResolver.getClassInfo(clz);
            }
            fury.writeNonRef(buffer, t, classInfo);
          }
        }
      }
    }

    @Override
    public void xwrite(MemoryBuffer buffer, T[] arr) {
      int len = arr.length;
      buffer.writeVarUint32Small7(len);
      // TODO(chaokunyang) use generics by creating component serializers to multi-dimension array.
      for (T t : arr) {
        fury.xwriteRef(buffer, t);
      }
    }

    @Override
    public T[] read(MemoryBuffer buffer) {
      // Some jdk8 will crash if use varint, why?
      int numElements = buffer.readVarUint32Small7();
      Object[] value = newArray(numElements);
      RefResolver refResolver = fury.getRefResolver();
      refResolver.reference(value);
      final Serializer componentTypeSerializer = this.componentTypeSerializer;
      if (componentTypeSerializer != null) {
        for (int i = 0; i < numElements; i++) {
          Object elem;
          int nextReadRefId = refResolver.tryPreserveRefId(buffer);
          if (nextReadRefId >= Fury.NOT_NULL_VALUE_FLAG) {
            elem = componentTypeSerializer.read(buffer);
            refResolver.setReadObject(nextReadRefId, elem);
          } else {
            elem = refResolver.getReadObject();
          }
          value[i] = elem;
        }
      } else {
        Fury fury = this.fury;
        ClassInfoHolder classInfoHolder = this.classInfoHolder;
        for (int i = 0; i < numElements; i++) {
          int nextReadRefId = refResolver.tryPreserveRefId(buffer);
          Object o;
          if (nextReadRefId >= Fury.NOT_NULL_VALUE_FLAG) {
            // ref value or not-null value
            o = fury.readNonRef(buffer, classInfoHolder);
            refResolver.setReadObject(nextReadRefId, o);
          } else {
            o = refResolver.getReadObject();
          }
          value[i] = o;
        }
      }
      return (T[]) value;
    }

    @Override
    public T[] xread(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      Object[] value = newArray(numElements);
      for (int i = 0; i < numElements; i++) {
        value[i] = fury.xreadRef(buffer);
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

  // Implement all read/write methods in subclasses to avoid
  // virtual method call cost.
  public abstract static class PrimitiveArraySerializer<T>
      extends Serializers.CrossLanguageCompatibleSerializer<T> {
    protected final int offset;
    protected final int elemSize;

    public PrimitiveArraySerializer(Fury fury, Class<T> cls) {
      super(fury, cls, (short) primitiveInfo.get(TypeUtils.getArrayComponentInfo(cls).f0)[2]);
      Class<?> innerType = TypeUtils.getArrayComponentInfo(cls).f0;
      this.offset = primitiveInfo.get(innerType)[0];
      this.elemSize = primitiveInfo.get(innerType)[1];
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

  public static final class BooleanArraySerializer extends PrimitiveArraySerializer<boolean[]> {

    public BooleanArraySerializer(Fury fury) {
      super(fury, boolean[].class);
    }

    @Override
    public void write(MemoryBuffer buffer, boolean[] value) {
      if (fury.getBufferCallback() == null) {
        int size = Math.multiplyExact(value.length, elemSize);
        buffer.writePrimitiveArrayWithSize(value, offset, size);
      } else {
        fury.writeBufferObject(
            buffer, new PrimitiveArrayBufferObject(value, offset, elemSize, value.length));
      }
    }

    @Override
    public boolean[] read(MemoryBuffer buffer) {
      if (fury.isPeerOutOfBandEnabled()) {
        MemoryBuffer buf = fury.readBufferObject(buffer);
        int size = buf.remaining();
        int numElements = size / elemSize;
        boolean[] values = new boolean[numElements];
        buf.copyToUnsafe(0, values, offset, size);
        return values;
      } else {
        int size = buffer.readVarUint32Small7();
        int numElements = size / elemSize;
        boolean[] values = new boolean[numElements];
        buffer.readToUnsafe(values, offset, size);
        return values;
      }
    }
  }

  public static final class ByteArraySerializer extends PrimitiveArraySerializer<byte[]> {

    public ByteArraySerializer(Fury fury) {
      super(fury, byte[].class);
    }

    @Override
    public void write(MemoryBuffer buffer, byte[] value) {
      if (fury.getBufferCallback() == null) {
        int size = Math.multiplyExact(value.length, 1);
        buffer.writePrimitiveArrayWithSize(value, offset, size);
      } else {
        fury.writeBufferObject(
            buffer, new PrimitiveArrayBufferObject(value, offset, 1, value.length));
      }
    }

    @Override
    public byte[] read(MemoryBuffer buffer) {
      if (fury.isPeerOutOfBandEnabled()) {
        MemoryBuffer buf = fury.readBufferObject(buffer);
        int size = buf.remaining();
        byte[] values = new byte[size];
        buf.copyToUnsafe(0, values, offset, size);
        return values;
      } else {
        int size = buffer.readVarUint32Small7();
        byte[] values = new byte[size];
        buffer.readToUnsafe(values, offset, size);
        return values;
      }
    }
  }

  public static final class CharArraySerializer extends PrimitiveArraySerializer<char[]> {

    public CharArraySerializer(Fury fury) {
      super(fury, char[].class);
    }

    @Override
    public void write(MemoryBuffer buffer, char[] value) {
      if (fury.getBufferCallback() == null) {
        int size = Math.multiplyExact(value.length, elemSize);
        buffer.writePrimitiveArrayWithSize(value, offset, size);
      } else {
        fury.writeBufferObject(
            buffer, new PrimitiveArrayBufferObject(value, offset, elemSize, value.length));
      }
    }

    @Override
    public char[] read(MemoryBuffer buffer) {
      if (fury.isPeerOutOfBandEnabled()) {
        MemoryBuffer buf = fury.readBufferObject(buffer);
        int size = buf.remaining();
        int numElements = size / elemSize;
        char[] values = new char[numElements];
        buf.copyToUnsafe(0, values, offset, size);
        return values;
      } else {
        int size = buffer.readVarUint32Small7();
        int numElements = size / elemSize;
        char[] values = new char[numElements];
        buffer.readToUnsafe(values, offset, size);
        return values;
      }
    }

    @Override
    public short getXtypeId() {
      return Fury.NOT_SUPPORT_CROSS_LANGUAGE;
    }

    @Override
    public void xwrite(MemoryBuffer buffer, char[] value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public char[] xread(MemoryBuffer buffer) {
      throw new UnsupportedOperationException();
    }
  }

  public static final class ShortArraySerializer extends PrimitiveArraySerializer<short[]> {

    public ShortArraySerializer(Fury fury) {
      super(fury, short[].class);
    }

    @Override
    public void write(MemoryBuffer buffer, short[] value) {
      if (fury.getBufferCallback() == null) {
        int size = Math.multiplyExact(value.length, elemSize);
        buffer.writePrimitiveArrayWithSize(value, offset, size);
      } else {
        fury.writeBufferObject(
            buffer, new PrimitiveArrayBufferObject(value, offset, elemSize, value.length));
      }
    }

    @Override
    public short[] read(MemoryBuffer buffer) {
      if (fury.isPeerOutOfBandEnabled()) {
        MemoryBuffer buf = fury.readBufferObject(buffer);
        int size = buf.remaining();
        int numElements = size / elemSize;
        short[] values = new short[numElements];
        buf.copyToUnsafe(0, values, offset, size);
        return values;
      } else {
        int size = buffer.readVarUint32Small7();
        int numElements = size / elemSize;
        short[] values = new short[numElements];
        buffer.readToUnsafe(values, offset, size);
        return values;
      }
    }
  }

  public static final class IntArraySerializer extends PrimitiveArraySerializer<int[]> {

    public IntArraySerializer(Fury fury) {
      super(fury, int[].class);
    }

    @Override
    public void write(MemoryBuffer buffer, int[] value) {
      if (fury.getBufferCallback() == null) {
        int size = Math.multiplyExact(value.length, elemSize);
        buffer.writePrimitiveArrayWithSize(value, offset, size);
      } else {
        fury.writeBufferObject(
            buffer, new PrimitiveArrayBufferObject(value, offset, elemSize, value.length));
      }
    }

    @Override
    public int[] read(MemoryBuffer buffer) {
      if (fury.isPeerOutOfBandEnabled()) {
        MemoryBuffer buf = fury.readBufferObject(buffer);
        int size = buf.remaining();
        int numElements = size / elemSize;
        int[] values = new int[numElements];
        buf.copyToUnsafe(0, values, offset, size);
        return values;
      } else {
        int size = buffer.readVarUint32Small7();
        int numElements = size / elemSize;
        int[] values = new int[numElements];
        buffer.readToUnsafe(values, offset, size);
        return values;
      }
    }
  }

  public static final class LongArraySerializer extends PrimitiveArraySerializer<long[]> {

    public LongArraySerializer(Fury fury) {
      super(fury, long[].class);
    }

    @Override
    public void write(MemoryBuffer buffer, long[] value) {
      if (fury.getBufferCallback() == null) {
        int size = Math.multiplyExact(value.length, elemSize);
        buffer.writePrimitiveArrayWithSize(value, offset, size);
      } else {
        fury.writeBufferObject(
            buffer, new PrimitiveArrayBufferObject(value, offset, elemSize, value.length));
      }
    }

    @Override
    public long[] read(MemoryBuffer buffer) {
      if (fury.isPeerOutOfBandEnabled()) {
        MemoryBuffer buf = fury.readBufferObject(buffer);
        int size = buf.remaining();
        int numElements = size / elemSize;
        long[] values = new long[numElements];
        buf.copyToUnsafe(0, values, offset, size);
        return values;
      } else {
        int size = buffer.readVarUint32Small7();
        int numElements = size / elemSize;
        long[] values = new long[numElements];
        buffer.readToUnsafe(values, offset, size);
        return values;
      }
    }
  }

  public static final class FloatArraySerializer extends PrimitiveArraySerializer<float[]> {

    public FloatArraySerializer(Fury fury) {
      super(fury, float[].class);
    }

    @Override
    public void write(MemoryBuffer buffer, float[] value) {
      if (fury.getBufferCallback() == null) {
        int size = Math.multiplyExact(value.length, elemSize);
        buffer.writePrimitiveArrayWithSize(value, offset, size);
      } else {
        fury.writeBufferObject(
            buffer, new PrimitiveArrayBufferObject(value, offset, elemSize, value.length));
      }
    }

    @Override
    public float[] read(MemoryBuffer buffer) {
      if (fury.isPeerOutOfBandEnabled()) {
        MemoryBuffer buf = fury.readBufferObject(buffer);
        int size = buf.remaining();
        int numElements = size / elemSize;
        float[] values = new float[numElements];
        buf.copyToUnsafe(0, values, offset, size);
        return values;
      } else {
        int size = buffer.readVarUint32Small7();
        int numElements = size / elemSize;
        float[] values = new float[numElements];
        buffer.readToUnsafe(values, offset, size);
        return values;
      }
    }
  }

  public static final class DoubleArraySerializer extends PrimitiveArraySerializer<double[]> {

    public DoubleArraySerializer(Fury fury) {
      super(fury, double[].class);
    }

    @Override
    public void write(MemoryBuffer buffer, double[] value) {
      if (fury.getBufferCallback() == null) {
        int size = Math.multiplyExact(value.length, elemSize);
        buffer.writePrimitiveArrayWithSize(value, offset, size);
      } else {
        fury.writeBufferObject(
            buffer, new PrimitiveArrayBufferObject(value, offset, elemSize, value.length));
      }
    }

    @Override
    public double[] read(MemoryBuffer buffer) {
      if (fury.isPeerOutOfBandEnabled()) {
        MemoryBuffer buf = fury.readBufferObject(buffer);
        int size = buf.remaining();
        int numElements = size / elemSize;
        double[] values = new double[numElements];
        buf.copyToUnsafe(0, values, offset, size);
        return values;
      } else {
        int size = buffer.readVarUint32Small7();
        int numElements = size / elemSize;
        double[] values = new double[numElements];
        buffer.readToUnsafe(values, offset, size);
        return values;
      }
    }
  }

  public static final class StringArraySerializer extends Serializer<String[]> {
    private final StringSerializer stringSerializer;
    private final FuryArrayAsListSerializer collectionSerializer;
    private final ArrayAsList<String> list;

    public StringArraySerializer(Fury fury) {
      super(fury, String[].class);
      stringSerializer = new StringSerializer(fury);
      collectionSerializer = new FuryArrayAsListSerializer(fury);
      collectionSerializer.setElementSerializer(stringSerializer);
      list = new ArrayAsList<>(0);
    }

    @Override
    public short getXtypeId() {
      return (short) -Type.FURY_STRING_ARRAY.getId();
    }

    @Override
    public void write(MemoryBuffer buffer, String[] value) {
      int len = value.length;
      buffer.writeVarUint32Small7(len);
      if (len == 0) {
        return;
      }
      list.setArray(value);
      // TODO reference support
      // this method won't throw exception.
      int flags = collectionSerializer.writeNullabilityHeader(buffer, list);
      list.setArray(null); // clear for gc
      StringSerializer stringSerializer = this.stringSerializer;
      if ((flags & CollectionFlags.HAS_NULL) != CollectionFlags.HAS_NULL) {
        for (String elem : value) {
          stringSerializer.write(buffer, elem);
        }
      } else {
        for (String elem : value) {
          if (elem == null) {
            buffer.writeByte(Fury.NULL_FLAG);
          } else {
            buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
            stringSerializer.write(buffer, elem);
          }
        }
      }
    }

    @Override
    public String[] read(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      String[] value = new String[numElements];
      if (numElements == 0) {
        return value;
      }
      int flags = buffer.readByte();
      StringSerializer serializer = this.stringSerializer;
      if ((flags & CollectionFlags.HAS_NULL) != CollectionFlags.HAS_NULL) {
        for (int i = 0; i < numElements; i++) {
          value[i] = serializer.readJavaString(buffer);
        }
      } else {
        for (int i = 0; i < numElements; i++) {
          if (buffer.readByte() != Fury.NULL_FLAG) {
            value[i] = serializer.readJavaString(buffer);
          }
        }
      }
      return value;
    }

    @Override
    public void xwrite(MemoryBuffer buffer, String[] value) {
      int len = value.length;
      buffer.writeVarUint32Small7(len);
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
    public String[] xread(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
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
    fury.registerSerializer(Class[].class, new ObjectArraySerializer<>(fury, Class[].class));
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
    buffer.writeVarUint32Small7(size);
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
