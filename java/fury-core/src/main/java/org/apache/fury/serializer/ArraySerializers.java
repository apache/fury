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
import java.util.Arrays;
import java.util.IdentityHashMap;
import org.apache.fury.Fury;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.Platform;
import org.apache.fury.resolver.ClassInfo;
import org.apache.fury.resolver.ClassInfoHolder;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.RefResolver;
import org.apache.fury.serializer.collection.CollectionFlags;
import org.apache.fury.serializer.collection.FuryArrayAsListSerializer;
import org.apache.fury.type.GenericType;
import org.apache.fury.type.TypeUtils;
import org.apache.fury.type.Types;
import org.apache.fury.util.Preconditions;

/** Serializers for array types. */
public class ArraySerializers {

  /** May be multi-dimension array, or multi-dimension primitive array. */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static final class ObjectArraySerializer<T> extends Serializer<T[]> {
    private final Class<T> innerType;
    private final Serializer componentTypeSerializer;
    private final ClassInfoHolder classInfoHolder;
    private final int[] stubDims;
    private final GenericType componentGenericType;

    public ObjectArraySerializer(Fury fury, Class<T[]> cls) {
      super(fury, cls);
      fury.getClassResolver().setSerializer(cls, this);
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
      this.innerType = (Class<T>) innerType;
      Class<?> componentType = cls.getComponentType();
      componentGenericType = fury.getClassResolver().buildGenericType(componentType);
      if (fury.getClassResolver().isMonomorphic(componentType)) {
        if (fury.isCrossLanguage()) {
          this.componentTypeSerializer = null;
        } else {
          this.componentTypeSerializer = fury.getClassResolver().getSerializer(componentType);
        }
      } else {
        // TODO add ClassInfo cache for non-final component type.
        this.componentTypeSerializer = null;
      }
      this.stubDims = new int[dimension];
      classInfoHolder = fury.getClassResolver().nilClassInfoHolder();
    }

    @Override
    public void write(MemoryBuffer buffer, T[] arr) {
      int len = arr.length;
      RefResolver refResolver = fury.getRefResolver();
      Serializer componentSerializer = this.componentTypeSerializer;
      int header = componentSerializer != null ? 0b1 : 0b0;
      buffer.writeVarUint32Small7(len << 1 | header);
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
    public T[] copy(T[] originArray) {
      int length = originArray.length;
      Object[] newArray = newArray(length);
      if (needToCopyRef) {
        fury.reference(originArray, newArray);
      }
      Serializer componentSerializer = this.componentTypeSerializer;
      if (componentSerializer != null) {
        if (componentSerializer.isImmutable()) {
          System.arraycopy(originArray, 0, newArray, 0, length);
        } else {
          for (int i = 0; i < length; i++) {
            newArray[i] = componentSerializer.copy(originArray[i]);
          }
        }
      } else {
        for (int i = 0; i < length; i++) {
          newArray[i] = fury.copyObject(originArray[i]);
        }
      }
      return (T[]) newArray;
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
      int numElements = buffer.readVarUint32Small7();
      boolean isFinal = (numElements & 0b1) != 0;
      numElements >>>= 1;
      Object[] value = newArray(numElements);
      RefResolver refResolver = fury.getRefResolver();
      refResolver.reference(value);
      if (isFinal) {
        final Serializer componentTypeSerializer = this.componentTypeSerializer;
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
      fury.getGenerics().pushGenericType(componentGenericType);
      for (int i = 0; i < numElements; i++) {
        Object x = fury.xreadRef(buffer);
        value[i] = x;
      }
      fury.getGenerics().popGenericType();
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
      super(fury, cls);
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
    public boolean[] copy(boolean[] originArray) {
      return Arrays.copyOf(originArray, originArray.length);
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
    public byte[] copy(byte[] originArray) {
      return Arrays.copyOf(originArray, originArray.length);
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
    public char[] copy(char[] originArray) {
      return Arrays.copyOf(originArray, originArray.length);
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
    public short[] copy(short[] originArray) {
      return Arrays.copyOf(originArray, originArray.length);
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
    public int[] copy(int[] originArray) {
      return Arrays.copyOf(originArray, originArray.length);
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
    public long[] copy(long[] originArray) {
      return Arrays.copyOf(originArray, originArray.length);
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
    public float[] copy(float[] originArray) {
      return Arrays.copyOf(originArray, originArray.length);
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
    public double[] copy(double[] originArray) {
      return Arrays.copyOf(originArray, originArray.length);
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
    private final FuryArrayAsListSerializer.ArrayAsList list;

    public StringArraySerializer(Fury fury) {
      super(fury, String[].class);
      stringSerializer = new StringSerializer(fury);
      collectionSerializer = new FuryArrayAsListSerializer(fury);
      collectionSerializer.setElementSerializer(stringSerializer);
      list = new FuryArrayAsListSerializer.ArrayAsList(0);
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
      list.clearArray(); // clear for gc
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
    public String[] copy(String[] originArray) {
      String[] newArray = new String[originArray.length];
      System.arraycopy(originArray, 0, newArray, 0, originArray.length);
      return newArray;
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
          buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
          stringSerializer.writeString(buffer, elem);
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
        if (buffer.readByte() >= Fury.NOT_NULL_VALUE_FLAG) {
          value[i] = stringSerializer.readString(buffer);
        } else {
          value[i] = null;
        }
      }
      return value;
    }
  }

  public static void registerDefaultSerializers(Fury fury) {
    ClassResolver resolver = fury.getClassResolver();
    resolver.registerSerializer(Object[].class, new ObjectArraySerializer<>(fury, Object[].class));
    resolver.registerSerializer(Class[].class, new ObjectArraySerializer<>(fury, Class[].class));
    resolver.registerSerializer(byte[].class, new ByteArraySerializer(fury));
    resolver.registerSerializer(Byte[].class, new ObjectArraySerializer<>(fury, Byte[].class));
    resolver.registerSerializer(char[].class, new CharArraySerializer(fury));
    resolver.registerSerializer(
        Character[].class, new ObjectArraySerializer<>(fury, Character[].class));
    resolver.registerSerializer(short[].class, new ShortArraySerializer(fury));
    resolver.registerSerializer(Short[].class, new ObjectArraySerializer<>(fury, Short[].class));
    resolver.registerSerializer(int[].class, new IntArraySerializer(fury));
    resolver.registerSerializer(
        Integer[].class, new ObjectArraySerializer<>(fury, Integer[].class));
    resolver.registerSerializer(long[].class, new LongArraySerializer(fury));
    resolver.registerSerializer(Long[].class, new ObjectArraySerializer<>(fury, Long[].class));
    resolver.registerSerializer(float[].class, new FloatArraySerializer(fury));
    resolver.registerSerializer(Float[].class, new ObjectArraySerializer<>(fury, Float[].class));
    resolver.registerSerializer(double[].class, new DoubleArraySerializer(fury));
    resolver.registerSerializer(Double[].class, new ObjectArraySerializer<>(fury, Double[].class));
    resolver.registerSerializer(boolean[].class, new BooleanArraySerializer(fury));
    resolver.registerSerializer(
        Boolean[].class, new ObjectArraySerializer<>(fury, Boolean[].class));
    resolver.registerSerializer(String[].class, new StringArraySerializer(fury));
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
        boolean.class, new int[] {Platform.BOOLEAN_ARRAY_OFFSET, 1, Types.BOOL_ARRAY});
    primitiveInfo.put(byte.class, new int[] {Platform.BYTE_ARRAY_OFFSET, 1, Types.BINARY});
    primitiveInfo.put(
        char.class, new int[] {Platform.CHAR_ARRAY_OFFSET, 2, Fury.NOT_SUPPORT_XLANG});
    primitiveInfo.put(short.class, new int[] {Platform.SHORT_ARRAY_OFFSET, 2, Types.INT16_ARRAY});
    primitiveInfo.put(int.class, new int[] {Platform.INT_ARRAY_OFFSET, 4, Types.INT32_ARRAY});
    primitiveInfo.put(long.class, new int[] {Platform.LONG_ARRAY_OFFSET, 8, Types.INT64_ARRAY});
    primitiveInfo.put(float.class, new int[] {Platform.FLOAT_ARRAY_OFFSET, 4, Types.FLOAT32_ARRAY});
    primitiveInfo.put(
        double.class, new int[] {Platform.DOUBLE_ARRAY_OFFSET, 8, Types.FLOAT64_ARRAY});
  }

  public abstract static class AbstractedNonexistentArrayClassSerializer extends Serializer {
    protected final String className;
    private final int dims;

    public AbstractedNonexistentArrayClassSerializer(
        Fury fury, String className, Class<?> stubClass) {
      super(fury, stubClass);
      this.className = className;
      this.dims = TypeUtils.getArrayDimensions(stubClass);
    }

    @Override
    public Object[] read(MemoryBuffer buffer) {
      switch (dims) {
        case 1:
          return read1DArray(buffer);
        case 2:
          return read2DArray(buffer);
        case 3:
          return read3DArray(buffer);
        default:
          throw new UnsupportedOperationException(
              String.format("Unsupported array dimension %s for class %s", dims, className));
      }
    }

    protected abstract Object readInnerElement(MemoryBuffer buffer);

    private Object[] read1DArray(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      boolean isFinal = (numElements & 0b1) != 0;
      numElements >>>= 1;
      RefResolver refResolver = fury.getRefResolver();
      Object[] value = new Object[numElements];
      refResolver.reference(value);

      if (isFinal) {
        for (int i = 0; i < numElements; i++) {
          Object elem;
          int nextReadRefId = refResolver.tryPreserveRefId(buffer);
          if (nextReadRefId >= Fury.NOT_NULL_VALUE_FLAG) {
            elem = readInnerElement(buffer);
            refResolver.setReadObject(nextReadRefId, elem);
          } else {
            elem = refResolver.getReadObject();
          }
          value[i] = elem;
        }
      } else {
        for (int i = 0; i < numElements; i++) {
          value[i] = fury.readRef(buffer);
        }
      }
      return value;
    }

    private Object[][] read2DArray(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      boolean isFinal = (numElements & 0b1) != 0;
      numElements >>>= 1;
      RefResolver refResolver = fury.getRefResolver();
      Object[][] value = new Object[numElements][];
      refResolver.reference(value);
      if (isFinal) {
        for (int i = 0; i < numElements; i++) {
          Object[] elem;
          int nextReadRefId = refResolver.tryPreserveRefId(buffer);
          if (nextReadRefId >= Fury.NOT_NULL_VALUE_FLAG) {
            elem = read1DArray(buffer);
            refResolver.setReadObject(nextReadRefId, elem);
          } else {
            elem = (Object[]) refResolver.getReadObject();
          }
          value[i] = elem;
        }
      } else {
        for (int i = 0; i < numElements; i++) {
          value[i] = (Object[]) fury.readRef(buffer);
        }
      }
      return value;
    }

    private Object[] read3DArray(MemoryBuffer buffer) {
      int numElements = buffer.readVarUint32Small7();
      boolean isFinal = (numElements & 0b1) != 0;
      numElements >>>= 1;
      RefResolver refResolver = fury.getRefResolver();
      Object[][][] value = new Object[numElements][][];
      refResolver.reference(value);
      if (isFinal) {
        for (int i = 0; i < numElements; i++) {
          Object[][] elem;
          int nextReadRefId = refResolver.tryPreserveRefId(buffer);
          if (nextReadRefId >= Fury.NOT_NULL_VALUE_FLAG) {
            elem = read2DArray(buffer);
            refResolver.setReadObject(nextReadRefId, elem);
          } else {
            elem = (Object[][]) refResolver.getReadObject();
          }
          value[i] = elem;
        }
      } else {
        for (int i = 0; i < numElements; i++) {
          value[i] = (Object[][]) fury.readRef(buffer);
        }
      }
      return value;
    }
  }

  @SuppressWarnings("rawtypes")
  public static final class NonexistentArrayClassSerializer
      extends AbstractedNonexistentArrayClassSerializer {
    private final Serializer componentSerializer;

    public NonexistentArrayClassSerializer(Fury fury, Class<?> cls) {
      this(fury, "Unknown", cls);
    }

    public NonexistentArrayClassSerializer(Fury fury, String className, Class<?> cls) {
      super(fury, className, cls);
      if (TypeUtils.getArrayComponent(cls).isEnum()) {
        componentSerializer = new NonexistentClassSerializers.NonexistentEnumClassSerializer(fury);
      } else {
        if (fury.getConfig().getCompatibleMode() == CompatibleMode.COMPATIBLE) {
          componentSerializer =
              new CompatibleSerializer<>(fury, NonexistentClass.NonexistentSkip.class);
        } else {
          componentSerializer = null;
        }
      }
    }

    @Override
    protected Object readInnerElement(MemoryBuffer buffer) {
      if (componentSerializer == null) {
        throw new IllegalStateException(
            String.format("Class %s should serialize elements as non-morphic", className));
      }
      return componentSerializer.read(buffer);
    }
  }
}
