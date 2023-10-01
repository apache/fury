/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury;

import com.google.common.base.Preconditions;
import io.fury.builder.JITContext;
import io.fury.memory.MemoryBuffer;
import io.fury.memory.MemoryUtils;
import io.fury.pool.ThreadPoolFury;
import io.fury.resolver.ClassInfo;
import io.fury.resolver.ClassInfoCache;
import io.fury.resolver.ClassResolver;
import io.fury.resolver.EnumStringResolver;
import io.fury.resolver.MapRefResolver;
import io.fury.resolver.NoRefResolver;
import io.fury.resolver.RefResolver;
import io.fury.resolver.SerializationContext;
import io.fury.serializer.ArraySerializers;
import io.fury.serializer.BufferCallback;
import io.fury.serializer.BufferObject;
import io.fury.serializer.CompatibleMode;
import io.fury.serializer.JavaSerializer;
import io.fury.serializer.ObjectStreamSerializer;
import io.fury.serializer.OpaqueObjects;
import io.fury.serializer.Serializer;
import io.fury.serializer.SerializerFactory;
import io.fury.serializer.StringSerializer;
import io.fury.serializer.TimeSerializers;
import io.fury.type.Generics;
import io.fury.type.Type;
import io.fury.util.LoggerFactory;
import io.fury.util.Platform;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;

/**
 * Cross-Lang Data layout: 1byte mask: 1-bit null: 0->null, 1->not null 1-bit endianness: 0->le,
 * 1->be 1-bit target lang: 0->native, 1->x_lang if x_lang, will write current process language as a
 * byte into buffer. 1-bit out-of-band serialization enable flag: 0 -> not enabled, 1 -> enabled.
 * other bits reserved.
 *
 * <p>serialize/deserialize is user API for root object serialization, write/read api is for inner
 * serialization.
 *
 * @author chaokunyang
 */
@NotThreadSafe
public final class Fury {
  private static final Logger LOG = LoggerFactory.getLogger(Fury.class);

  public static final byte NULL_FLAG = -3;
  // This flag indicates that object is a not-null value.
  // We don't use another byte to indicate REF, so that we can save one byte.
  public static final byte REF_FLAG = -2;
  // this flag indicates that the object is a non-null value.
  public static final byte NOT_NULL_VALUE_FLAG = -1;
  // this flag indicates that the object is a referencable and first read.
  public static final byte REF_VALUE_FLAG = 0;
  public static final byte NOT_SUPPORT_CROSS_LANGUAGE = 0;
  public static final short FURY_TYPE_TAG_ID = Type.FURY_TYPE_TAG.getId();
  private static final byte isNilFlag = 1;
  private static final byte isLittleEndianFlag = 1 << 1;
  private static final byte isCrossLanguageFlag = 1 << 2;
  private static final byte isOutOfBandFlag = 1 << 3;
  private static final boolean isLittleEndian = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

  private final Config config;
  private final boolean refTracking;
  private final RefResolver refResolver;
  private final ClassResolver classResolver;
  private final EnumStringResolver enumStringResolver;
  private final SerializationContext serializationContext;
  private final ClassLoader classLoader;
  private final JITContext jitContext;
  private final MemoryBuffer buffer;
  private final List<Object> nativeObjects;
  private final StringSerializer stringSerializer;
  private final Language language;
  private final boolean compressInt;
  private final boolean compressLong;
  private final Generics generics;
  private Language peerLanguage;
  private BufferCallback bufferCallback;
  private Iterator<MemoryBuffer> outOfBandBuffers;
  private boolean peerOutOfBandEnabled;
  private int depth;

  private Fury(FuryBuilder builder, ClassLoader classLoader) {
    // Avoid set classLoader in `FuryBuilder`, which won't be clear when
    // `io.fury.ThreadSafeFury.clearClassLoader` is called.
    config = new Config(builder);
    this.language = builder.language;
    this.refTracking = builder.trackingRef;
    compressInt = builder.compressInt;
    compressLong = builder.compressLong;
    if (refTracking) {
      this.refResolver = new MapRefResolver();
    } else {
      this.refResolver = new NoRefResolver();
    }
    jitContext = new JITContext(this);
    enumStringResolver = new EnumStringResolver();
    classResolver = new ClassResolver(this);
    classResolver.initialize();
    serializationContext = new SerializationContext();
    this.classLoader = classLoader;
    buffer = MemoryUtils.buffer(32);
    nativeObjects = new ArrayList<>();
    generics = new Generics(this);
    stringSerializer = new StringSerializer(this);
    LOG.info("Created new fury {}", this);
  }

  /** register class. */
  public void register(Class<?> cls) {
    classResolver.register(cls);
  }

  /** register class with given id. */
  public void register(Class<?> cls, Short id) {
    classResolver.register(cls, id);
  }

  /** register class with given type tag which will be used for cross-language serialization. */
  public void register(Class<?> cls, String typeTag) {
    classResolver.register(cls, typeTag);
  }

  public <T> void registerSerializer(Class<T> type, Class<? extends Serializer> serializerClass) {
    classResolver.registerSerializer(type, serializerClass);
  }

  public void registerSerializer(Class<?> type, Serializer<?> serializer) {
    classResolver.registerSerializer(type, serializer);
  }

  public void setSerializerFactory(SerializerFactory serializerFactory) {
    classResolver.setSerializerFactory(serializerFactory);
  }

  public SerializerFactory getSerializerFactory() {
    return classResolver.getSerializerFactory();
  }

  /**
   * Serialize <code>obj</code> to a off-heap buffer specified by <code>address</code> and <code>
   * size</code>.
   */
  public MemoryBuffer serialize(Object obj, long address, int size) {
    MemoryBuffer buffer = MemoryUtils.buffer(address, size);
    serialize(buffer, obj, null);
    return buffer;
  }

  /** Return serialized <code>obj</code> as a byte array. */
  public byte[] serialize(Object obj) {
    buffer.writerIndex(0);
    serialize(buffer, obj, null);
    return buffer.getBytes(0, buffer.writerIndex());
  }

  /** Return serialized <code>obj</code> as a byte array. */
  public byte[] serialize(Object obj, BufferCallback callback) {
    buffer.writerIndex(0);
    serialize(buffer, obj, callback);
    return buffer.getBytes(0, buffer.writerIndex());
  }

  public MemoryBuffer serialize(MemoryBuffer buffer, Object obj) {
    return serialize(buffer, obj, null);
  }

  /** Serialize <code>obj</code> to a <code>buffer</code>. */
  public MemoryBuffer serialize(MemoryBuffer buffer, Object obj, BufferCallback callback) {
    try {
      jitContext.lock();
      this.bufferCallback = callback;
      int maskIndex = buffer.writerIndex();
      // 1byte used for bit mask
      buffer.ensure(maskIndex + 1);
      buffer.writerIndex(maskIndex + 1);
      byte bitmap = 0;
      if (obj == null) {
        bitmap |= isNilFlag;
        buffer.put(maskIndex, bitmap);
        return buffer;
      }
      // set endian.
      if (isLittleEndian) {
        bitmap |= isLittleEndianFlag;
      }
      if (language != Language.JAVA) {
        // set reader as x_lang.
        bitmap |= isCrossLanguageFlag;
        // set writer language.
        buffer.writeByte((byte) Language.JAVA.ordinal());
      }
      if (bufferCallback != null) {
        bitmap |= isOutOfBandFlag;
      }
      buffer.put(maskIndex, bitmap);
      if (language == Language.JAVA) {
        if (config.shareMetaContext()) {
          int startOffset = buffer.writerIndex();
          buffer.writeInt(-1); // preserve 4-byte for nativeObjects start offsets.
          writeRef(buffer, obj);
          buffer.putInt(startOffset, buffer.writerIndex());
          classResolver.writeClassDefs(buffer);
        } else {
          writeRef(buffer, obj);
        }
      } else {
        xserializeInternal(buffer, obj);
      }
      return buffer;
    } finally {
      resetWrite();
      jitContext.unlock();
    }
  }

  public void serialize(OutputStream outputStream, Object obj) {
    serialize(outputStream, obj, null);
  }

  public void serialize(OutputStream outputStream, Object obj, BufferCallback callback) {
    buffer.writerIndex(0);
    buffer.writeInt(-1);
    serialize(buffer, obj, callback);

    buffer.putInt(0, buffer.writerIndex() - 4);
    try {
      outputStream.write(buffer.getBytes(0, buffer.writerIndex()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void xserializeInternal(MemoryBuffer buffer, Object obj) {
    int startOffset = buffer.writerIndex();
    buffer.writeInt(-1); // preserve 4-byte for nativeObjects start offsets.
    buffer.writeInt(-1); // preserve 4-byte for nativeObjects size
    xwriteRef(buffer, obj);
    buffer.putInt(startOffset, buffer.writerIndex());
    buffer.putInt(startOffset + 4, nativeObjects.size());
    refResolver.resetWrite();
    // fury write opaque object classname which cause later write of classname only write an id.
    classResolver.resetWrite();
    enumStringResolver.resetWrite();
    for (Object nativeObject : nativeObjects) {
      writeRef(buffer, nativeObject);
    }
  }

  /** Serialize a nullable referencable object to <code>buffer</code>. */
  public void writeRef(MemoryBuffer buffer, Object obj) {
    if (!refResolver.writeRefOrNull(buffer, obj)) {
      ClassInfo classInfo = classResolver.getOrUpdateClassInfo(obj.getClass());
      classResolver.writeClass(buffer, classInfo);
      writeData(buffer, classInfo, obj);
    }
  }

  public void writeRef(MemoryBuffer buffer, Object obj, ClassInfoCache classInfoCache) {
    if (!refResolver.writeRefOrNull(buffer, obj)) {
      ClassInfo classInfo = classResolver.getClassInfo(obj.getClass(), classInfoCache);
      classResolver.writeClass(buffer, classInfo);
      writeData(buffer, classInfo, obj);
    }
  }

  public void writeRef(MemoryBuffer buffer, Object obj, ClassInfo classInfo) {
    Serializer<Object> serializer = classInfo.getSerializer();
    if (serializer.needToWriteRef()) {
      if (!refResolver.writeRefOrNull(buffer, obj)) {
        classResolver.writeClass(buffer, classInfo);
        depth++;
        serializer.write(buffer, obj);
        depth--;
      }
    } else {
      if (obj == null) {
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
        classResolver.writeClass(buffer, classInfo);
        depth++;
        serializer.write(buffer, obj);
        depth--;
      }
    }
  }

  public <T> void writeRef(MemoryBuffer buffer, T obj, Serializer<T> serializer) {
    if (serializer.needToWriteRef()) {
      if (!refResolver.writeRefOrNull(buffer, obj)) {
        depth++;
        serializer.write(buffer, obj);
        depth--;
      }
    } else {
      if (obj == null) {
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
        depth++;
        serializer.write(buffer, obj);
        depth--;
      }
    }
  }

  /**
   * Write object class and data without tracking ref.
   */
  public void writeNullable(MemoryBuffer buffer, Object obj) {
    if (obj == null) {
      buffer.writeByte(Fury.NULL_FLAG);
    } else {
      buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
      writeNonRef(buffer, obj);
    }
  }

  /**
   * Write object class and data without tracking ref.
   */
  public void writeNullable(MemoryBuffer buffer, Object obj, ClassInfoCache classInfoCache) {
    if (obj == null) {
      buffer.writeByte(Fury.NULL_FLAG);
    } else {
      buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
      writeNonRef(buffer, obj, classResolver.getClassInfo(obj.getClass(), classInfoCache));
    }
  }

  public void writeNullable(MemoryBuffer buffer, Object obj, ClassInfo classInfo) {
    if (obj == null) {
      buffer.writeByte(Fury.NULL_FLAG);
    } else {
      buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
      writeNonRef(buffer, obj, classInfo);
    }
  }

  /**
   * Serialize a not-null and non-reference object to <code>buffer</code>.
   *
   * <p>If reference is enabled, this method should be called only the object is first seen in the
   * object graph.
   */
  public void writeNonRef(MemoryBuffer buffer, Object obj) {
    ClassInfo classInfo = classResolver.getOrUpdateClassInfo(obj.getClass());
    classResolver.writeClass(buffer, classInfo);
    writeData(buffer, classInfo, obj);
  }

  public void writeNonRef(MemoryBuffer buffer, Object obj, ClassInfo classInfo) {
    classResolver.writeClass(buffer, classInfo);
    Serializer serializer = classInfo.getSerializer();
    depth++;
    serializer.write(buffer, obj);
    depth--;
  }

  public <T> void writeNonRef(MemoryBuffer buffer, T obj, Serializer<T> serializer) {
    depth++;
    serializer.write(buffer, obj);
    depth--;
  }

  public void xwriteRef(MemoryBuffer buffer, Object obj) {
    if (!refResolver.writeRefOrNull(buffer, obj)) {
      xwriteNonRef(buffer, obj, null);
    }
  }

  public <T> void xwriteRef(MemoryBuffer buffer, T obj, Serializer<T> serializer) {
    if (serializer.needToWriteRef()) {
      if (!refResolver.writeRefOrNull(buffer, obj)) {
        xwriteNonRef(buffer, obj, serializer);
      }
    } else {
      if (obj == null) {
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
        xwriteNonRef(buffer, obj, serializer);
      }
    }
  }

  public <T> void xwriteRefByNullableSerializer(
      MemoryBuffer buffer, T obj, Serializer<T> serializer) {
    if (serializer == null) {
      xwriteRef(buffer, obj);
    } else {
      xwriteRef(buffer, obj, serializer);
    }
  }

  public <T> void xwriteNonRef(MemoryBuffer buffer, T obj, Serializer<T> serializer) {
    depth++;
    @SuppressWarnings("unchecked")
    Class<T> cls = (Class<T>) obj.getClass();
    if (serializer == null) {
      serializer = classResolver.getSerializer(cls);
    }
    short typeId = serializer.getXtypeId();
    buffer.writeShort(typeId);
    if (typeId != NOT_SUPPORT_CROSS_LANGUAGE) {
      if (typeId == FURY_TYPE_TAG_ID) {
        classResolver.xwriteTypeTag(buffer, cls);
      }
      if (typeId < NOT_SUPPORT_CROSS_LANGUAGE) {
        classResolver.xwriteClass(buffer, cls);
      }
      serializer.xwrite(buffer, obj);
    } else {
      // Write classname so it can be used for debugging which object doesn't support
      // cross-language.
      // TODO add a config to disable this to reduce space cost.
      classResolver.xwriteClass(buffer, cls);
      // serializer may increase reference id multi times internally, thus peer cross-language later
      // fields/objects deserialization will use wrong reference id since we skip opaque objects
      // deserialization.
      // So we stash native objects and serialize all those object at the last.
      buffer.writePositiveVarInt(nativeObjects.size());
      nativeObjects.add(obj);
    }
    depth--;
  }

  /** Write not null data to buffer. */
  private void writeData(MemoryBuffer buffer, ClassInfo classInfo, Object obj) {
    switch (classInfo.getClassId()) {
      case ClassResolver.BOOLEAN_CLASS_ID:
        buffer.writeBoolean((Boolean) obj);
        break;
      case ClassResolver.BYTE_CLASS_ID:
        buffer.writeByte((Byte) obj);
        break;
      case ClassResolver.CHAR_CLASS_ID:
        buffer.writeChar((Character) obj);
        break;
      case ClassResolver.SHORT_CLASS_ID:
        buffer.writeShort((Short) obj);
        break;
      case ClassResolver.INTEGER_CLASS_ID:
        if (compressInt) {
          buffer.writeVarInt((Integer) obj);
        } else {
          buffer.writeInt((Integer) obj);
        }
        break;
      case ClassResolver.FLOAT_CLASS_ID:
        buffer.writeFloat((Float) obj);
        break;
      case ClassResolver.LONG_CLASS_ID:
        if (compressLong) {
          buffer.writeVarLong((Long) obj);
        } else {
          buffer.writeLong((Long) obj);
        }
        break;
      case ClassResolver.DOUBLE_CLASS_ID:
        buffer.writeDouble((Double) obj);
        break;
      case ClassResolver.STRING_CLASS_ID:
        stringSerializer.writeJavaString(buffer, (String) obj);
        break;
        // TODO(add fastpath for other types)
      default:
        depth++;
        classInfo.getSerializer().write(buffer, obj);
        depth--;
    }
  }

  public void writeBufferObject(MemoryBuffer buffer, BufferObject bufferObject) {
    if (bufferCallback == null || bufferCallback.apply(bufferObject)) {
      buffer.writeBoolean(true);
      // writer length.
      int totalBytes = bufferObject.totalBytes();
      // write aligned length so that later buffer copy happen on aligned offset, which will be more
      // efficient
      // TODO(chaokunyang) Remove branch when other languages support aligned varint.
      if (language == Language.JAVA) {
        buffer.writePositiveVarIntAligned(totalBytes);
      } else {
        buffer.writePositiveVarInt(totalBytes);
      }
      int writerIndex = buffer.writerIndex();
      buffer.ensure(writerIndex + bufferObject.totalBytes());
      bufferObject.writeTo(buffer);
      int size = buffer.writerIndex() - writerIndex;
      Preconditions.checkArgument(size == totalBytes);
    } else {
      buffer.writeBoolean(false);
    }
  }

  // duplicate for speed.
  public void writeBufferObject(
      MemoryBuffer buffer, ArraySerializers.PrimitiveArrayBufferObject bufferObject) {
    if (bufferCallback == null || bufferCallback.apply(bufferObject)) {
      buffer.writeBoolean(true);
      int totalBytes = bufferObject.totalBytes();
      // write aligned length so that later buffer copy happen on aligned offset, which will be very
      // efficient
      // TODO(chaokunyang) Remove branch when other languages support aligned varint.
      if (language == Language.JAVA) {
        buffer.writePositiveVarIntAligned(totalBytes);
      } else {
        buffer.writePositiveVarInt(totalBytes);
      }
      bufferObject.writeTo(buffer);
    } else {
      buffer.writeBoolean(false);
    }
  }

  public MemoryBuffer readBufferObject(MemoryBuffer buffer) {
    boolean inBand = buffer.readBoolean();
    if (inBand) {
      int size;
      // TODO(chaokunyang) Remove branch when other languages support aligned varint.
      if (language == Language.JAVA) {
        size = buffer.readPositiveAlignedVarInt();
      } else {
        size = buffer.readPositiveVarInt();
      }
      MemoryBuffer slice = buffer.slice(buffer.readerIndex(), size);
      buffer.readerIndex(buffer.readerIndex() + size);
      return slice;
    } else {
      Preconditions.checkArgument(outOfBandBuffers.hasNext());
      return outOfBandBuffers.next();
    }
  }

  public void writeString(MemoryBuffer buffer, String str) {
    stringSerializer.writeString(buffer, str);
  }

  public String readString(MemoryBuffer buffer) {
    return stringSerializer.readString(buffer);
  }

  public void writeJavaStringRef(MemoryBuffer buffer, String str) {
    if (stringSerializer.needToWriteRef()) {
      if (!refResolver.writeRefOrNull(buffer, str)) {
        stringSerializer.writeJavaString(buffer, str);
      }
    } else {
      if (str == null) {
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
        stringSerializer.write(buffer, str);
      }
    }
  }

  public String readJavaStringRef(MemoryBuffer buffer) {
    RefResolver refResolver = this.refResolver;
    if (stringSerializer.needToWriteRef()) {
      String obj;
      int nextReadRefId = refResolver.tryPreserveRefId(buffer);
      if (nextReadRefId >= NOT_NULL_VALUE_FLAG) {
        obj = stringSerializer.read(buffer);
        refResolver.setReadObject(nextReadRefId, obj);
        return obj;
      } else {
        return (String) refResolver.getReadObject();
      }
    } else {
      byte headFlag = buffer.readByte();
      if (headFlag == Fury.NULL_FLAG) {
        return null;
      } else {
        return stringSerializer.read(buffer);
      }
    }
  }

  public void writeJavaString(MemoryBuffer buffer, String str) {
    stringSerializer.writeJavaString(buffer, str);
  }

  public String readJavaString(MemoryBuffer buffer) {
    return stringSerializer.readJavaString(buffer);
  }

  /** Deserialize <code>obj</code> from a byte array. */
  public Object deserialize(byte[] bytes) {
    return deserialize(MemoryUtils.wrap(bytes), null);
  }

  public Object deserialize(byte[] bytes, Iterable<MemoryBuffer> outOfBandBuffers) {
    return deserialize(MemoryUtils.wrap(bytes), outOfBandBuffers);
  }

  /**
   * Deserialize <code>obj</code> from a off-heap buffer specified by <code>address</code> and
   * <code>size</code>.
   */
  public Object deserialize(long address, int size) {
    return deserialize(MemoryUtils.buffer(address, size), null);
  }

  /** Deserialize <code>obj</code> from a <code>buffer</code>. */
  public Object deserialize(MemoryBuffer buffer) {
    return deserialize(buffer, null);
  }

  /**
   * Deserialize <code>obj</code> from a <code>buffer</code> and <code>outOfBandBuffers</code>.
   *
   * @param buffer serialized data. If the provided buffer start address is aligned with 4 bytes,
   *     the bulk read will be more efficient.
   * @param outOfBandBuffers If <code>buffers</code> is not None, it should be an iterable of
   *     buffer-enabled objects that is consumed each time the pickle stream references an
   *     out-of-band {@link BufferObject}. Such buffers have been given in order to the
   *     `bufferCallback` of a Fury object. If <code>outOfBandBuffers</code> is null (the default),
   *     then the buffers are taken from the serialized stream, assuming they are serialized there.
   *     It is an error for <code>outOfBandBuffers</code> to be null if the serialized stream was
   *     produced with a non-null `bufferCallback`.
   */
  public Object deserialize(MemoryBuffer buffer, Iterable<MemoryBuffer> outOfBandBuffers) {
    try {
      jitContext.lock();
      byte bitmap = buffer.readByte();
      if ((bitmap & isNilFlag) == isNilFlag) {
        return null;
      }
      boolean isLittleEndian = (bitmap & isLittleEndianFlag) == isLittleEndianFlag;
      Preconditions.checkArgument(Fury.isLittleEndian, isLittleEndian);
      boolean isTargetXLang = (bitmap & isCrossLanguageFlag) == isCrossLanguageFlag;
      if (isTargetXLang) {
        peerLanguage = Language.values()[buffer.readByte()];
      } else {
        peerLanguage = Language.JAVA;
      }
      peerOutOfBandEnabled = (bitmap & isOutOfBandFlag) == isOutOfBandFlag;
      if (peerOutOfBandEnabled) {
        Preconditions.checkNotNull(
            outOfBandBuffers,
            "outOfBandBuffers shouldn't be null when the serialized stream is "
                + "produced with bufferCallback not null.");
        this.outOfBandBuffers = outOfBandBuffers.iterator();
      } else {
        Preconditions.checkArgument(
            outOfBandBuffers == null,
            "outOfBandBuffers should be null when the serialized stream is "
                + "produced with bufferCallback null.");
      }
      Object obj;
      if (isTargetXLang) {
        obj = xdeserializeInternal(buffer);
      } else {
        if (config.shareMetaContext()) {
          classResolver.readClassDefs(buffer);
        }
        obj = readRef(buffer);
      }
      return obj;
    } finally {
      resetRead();
      jitContext.unlock();
    }
  }

  public Object deserialize(InputStream inputStream) {
    return deserialize(inputStream, null);
  }

  public Object deserialize(InputStream inputStream, Iterable<MemoryBuffer> outOfBandBuffers) {
    buffer.readerIndex(0);
    try {
      int read = inputStream.read(buffer.getHeapMemory(), 0, 4);
      Preconditions.checkArgument(read == 4);
      int size = buffer.readInt();
      buffer.ensure(size + 4);
      read = inputStream.read(buffer.getHeapMemory(), 4, size);
      Preconditions.checkArgument(read == size);
      return deserialize(buffer, outOfBandBuffers);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Object xdeserializeInternal(MemoryBuffer buffer) {
    Object obj;
    int nativeObjectsStartOffset = buffer.readInt();
    int nativeObjectsSize = buffer.readInt();
    int endReaderIndex = nativeObjectsStartOffset;
    if (peerLanguage == Language.JAVA) {
      int readerIndex = buffer.readerIndex();
      buffer.readerIndex(nativeObjectsStartOffset);
      for (int i = 0; i < nativeObjectsSize; i++) {
        nativeObjects.add(readRef(buffer));
      }
      endReaderIndex = buffer.readerIndex();
      buffer.readerIndex(readerIndex);
      refResolver.resetRead();
      classResolver.resetRead();
      enumStringResolver.resetRead();
    }
    obj = xreadRef(buffer);
    buffer.readerIndex(endReaderIndex);
    return obj;
  }

  /** Deserialize nullable referencable object from <code>buffer</code>. */
  public Object readRef(MemoryBuffer buffer) {
    RefResolver refResolver = this.refResolver;
    int nextReadRefId = refResolver.tryPreserveRefId(buffer);
    if (nextReadRefId >= NOT_NULL_VALUE_FLAG) {
      // ref value or not-null value
      Object o = readDataInternal(buffer, classResolver.readAndUpdateClassInfoCache(buffer));
      refResolver.setReadObject(nextReadRefId, o);
      return o;
    } else {
      return refResolver.getReadObject();
    }
  }

  public Object readRef(MemoryBuffer buffer, ClassInfoCache classInfoCache) {
    RefResolver refResolver = this.refResolver;
    int nextReadRefId = refResolver.tryPreserveRefId(buffer);
    if (nextReadRefId >= NOT_NULL_VALUE_FLAG) {
      // ref value or not-null value
      Object o = readDataInternal(buffer, classResolver.readClassInfo(buffer, classInfoCache));
      refResolver.setReadObject(nextReadRefId, o);
      return o;
    } else {
      return refResolver.getReadObject();
    }
  }

  @SuppressWarnings("unchecked")
  public <T> T readRef(MemoryBuffer buffer, Serializer<T> serializer) {
    if (serializer.needToWriteRef()) {
      T obj;
      int nextReadRefId = refResolver.tryPreserveRefId(buffer);
      if (nextReadRefId >= NOT_NULL_VALUE_FLAG) {
        obj = serializer.read(buffer);
        refResolver.setReadObject(nextReadRefId, obj);
        return obj;
      } else {
        return (T) refResolver.getReadObject();
      }
    } else {
      byte headFlag = buffer.readByte();
      if (headFlag == Fury.NULL_FLAG) {
        return null;
      } else {
        return serializer.read(buffer);
      }
    }
  }

  /** Deserialize not-null and non-reference object from <code>buffer</code>. */
  public Object readNonRef(MemoryBuffer buffer) {
    return readDataInternal(buffer, classResolver.readAndUpdateClassInfoCache(buffer));
  }

  public Object readNonRef(MemoryBuffer buffer, ClassInfoCache classInfoCache) {
    return readDataInternal(buffer, classResolver.readClassInfo(buffer, classInfoCache));
  }

  /**
   * Read object class and data without tracking ref.
   */
  public Object readNullable(MemoryBuffer buffer) {
    byte headFlag = buffer.readByte();
    if (headFlag == Fury.NULL_FLAG) {
      return null;
    } else {
      return readNonRef(buffer);
    }
  }

  /** Class should be read already. */
  public Object readData(MemoryBuffer buffer, ClassInfo classInfo) {
    depth++;
    Serializer<?> serializer = classInfo.getSerializer();
    Object read = serializer.read(buffer);
    depth--;
    return read;
  }

  private Object readDataInternal(MemoryBuffer buffer, ClassInfo classInfo) {
    switch (classInfo.getClassId()) {
      case ClassResolver.BOOLEAN_CLASS_ID:
        return buffer.readBoolean();
      case ClassResolver.BYTE_CLASS_ID:
        return buffer.readByte();
      case ClassResolver.CHAR_CLASS_ID:
        return buffer.readChar();
      case ClassResolver.SHORT_CLASS_ID:
        return buffer.readShort();
      case ClassResolver.INTEGER_CLASS_ID:
        if (compressInt) {
          return buffer.readVarInt();
        } else {
          return buffer.readInt();
        }
      case ClassResolver.FLOAT_CLASS_ID:
        return buffer.readFloat();
      case ClassResolver.LONG_CLASS_ID:
        if (compressLong) {
          return buffer.readVarLong();
        } else {
          return buffer.readLong();
        }
      case ClassResolver.DOUBLE_CLASS_ID:
        return buffer.readDouble();
      case ClassResolver.STRING_CLASS_ID:
        return stringSerializer.readJavaString(buffer);
        // TODO(add fastpath for other types)
      default:
        depth++;
        Object read = classInfo.getSerializer().read(buffer);
        depth--;
        return read;
    }
  }

  public Object xreadRef(MemoryBuffer buffer) {
    RefResolver refResolver = this.refResolver;
    int nextReadRefId = refResolver.tryPreserveRefId(buffer);
    if (nextReadRefId >= NOT_NULL_VALUE_FLAG) {
      Object o = xreadNonRef(buffer, null);
      refResolver.setReadObject(nextReadRefId, o);
      return o;
    } else {
      return refResolver.getReadObject();
    }
  }

  public Object xreadRef(MemoryBuffer buffer, Serializer<?> serializer) {
    if (serializer.needToWriteRef()) {
      RefResolver refResolver = this.refResolver;
      int nextReadRefId = refResolver.tryPreserveRefId(buffer);
      if (nextReadRefId >= NOT_NULL_VALUE_FLAG) {
        Object o = xreadNonRef(buffer, serializer);
        refResolver.setReadObject(nextReadRefId, o);
        return o;
      } else {
        return refResolver.getReadObject();
      }
    } else {
      byte headFlag = buffer.readByte();
      if (headFlag == Fury.NULL_FLAG) {
        return null;
      } else {
        return xreadNonRef(buffer, serializer);
      }
    }
  }

  public Object xreadRefByNullableSerializer(MemoryBuffer buffer, Serializer<?> serializer) {
    if (serializer == null) {
      return xreadRef(buffer);
    } else {
      return xreadRef(buffer, serializer);
    }
  }

  public Object xreadNonRef(MemoryBuffer buffer, Serializer<?> serializer) {
    depth++;
    short typeId = buffer.readShort();
    ClassResolver classResolver = this.classResolver;
    if (typeId != NOT_SUPPORT_CROSS_LANGUAGE) {
      Class<?> cls = null;
      if (typeId == FURY_TYPE_TAG_ID) {
        cls = classResolver.readClassByTypeTag(buffer);
      }
      if (typeId < NOT_SUPPORT_CROSS_LANGUAGE) {
        if (peerLanguage != Language.JAVA) {
          classResolver.xreadClassName(buffer);
          cls = classResolver.getClassByTypeId((short) -typeId);
        } else {
          cls = classResolver.xreadClass(buffer);
        }
      } else {
        if (typeId != FURY_TYPE_TAG_ID) {
          cls = classResolver.getClassByTypeId(typeId);
        }
      }
      Preconditions.checkNotNull(cls);
      if (serializer == null) {
        serializer = classResolver.getSerializer(cls);
      }
      // TODO check serializer consistent with `classResolver.getSerializer(cls)` when serializer
      // not null;
      Object o = serializer.xread(buffer);
      depth--;
      return o;
    } else {
      String className = classResolver.xreadClassName(buffer);
      int ordinal = buffer.readPositiveVarInt();
      if (peerLanguage != Language.JAVA) {
        return OpaqueObjects.of(peerLanguage, className, ordinal);
      } else {
        return nativeObjects.get(ordinal);
      }
    }
  }

  /**
   * Serialize java object without class info, deserialization should use {@link
   * #deserializeJavaObject}.
   */
  public byte[] serializeJavaObject(Object obj) {
    buffer.writerIndex(0);
    serializeJavaObject(buffer, obj);
    return buffer.getBytes(0, buffer.writerIndex());
  }

  /**
   * Serialize java object without class info, deserialization should use {@link
   * #deserializeJavaObject}.
   */
  public void serializeJavaObject(MemoryBuffer buffer, Object obj) {
    try {
      jitContext.lock();
      if (config.shareMetaContext()) {
        int startOffset = buffer.writerIndex();
        buffer.writeInt(-1); // preserve 4-byte for nativeObjects start offsets.
        if (!refResolver.writeRefOrNull(buffer, obj)) {
          ClassInfo classInfo = classResolver.getOrUpdateClassInfo(obj.getClass());
          writeData(buffer, classInfo, obj);
        }
        buffer.putInt(startOffset, buffer.writerIndex());
        classResolver.writeClassDefs(buffer);
      } else {
        if (!refResolver.writeRefOrNull(buffer, obj)) {
          ClassInfo classInfo = classResolver.getOrUpdateClassInfo(obj.getClass());
          writeData(buffer, classInfo, obj);
        }
      }
    } finally {
      resetWrite();
      jitContext.unlock();
    }
  }

  /**
   * Serialize java object without class info, deserialization should use {@link
   * #deserializeJavaObject}.
   */
  public void serializeJavaObject(OutputStream outputStream, Object obj) {
    serializeToStream(outputStream, buf -> serializeJavaObject(buf, obj));
  }

  /**
   * Deserialize java object from binary without class info, serialization should use {@link
   * #serializeJavaObject}.
   */
  public <T> T deserializeJavaObject(byte[] data, Class<T> cls) {
    return deserializeJavaObject(MemoryBuffer.fromByteArray(data), cls);
  }

  /**
   * Deserialize java object from binary by passing class info, serialization should use {@link
   * #serializeJavaObject}.
   */
  @SuppressWarnings("unchecked")
  public <T> T deserializeJavaObject(MemoryBuffer buffer, Class<T> cls) {
    try {
      if (config.shareMetaContext()) {
        classResolver.readClassDefs(buffer);
      }
      T obj;
      int nextReadRefId = refResolver.tryPreserveRefId(buffer);
      if (nextReadRefId >= NOT_NULL_VALUE_FLAG) {
        obj = (T) readDataInternal(buffer, classResolver.getClassInfo(cls));
        return obj;
      } else {
        return null;
      }
    } finally {
      resetRead();
    }
  }

  /**
   * Deserialize java object from binary by passing class info, serialization should use {@link
   * #serializeJavaObject}.
   */
  @SuppressWarnings("unchecked")
  public <T> T deserializeJavaObject(InputStream inputStream, Class<T> cls) {
    return (T) deserializeFromStream(inputStream, buf -> this.deserializeJavaObject(buf, cls));
  }

  /**
   * Deserialize java object from binary by passing class info, serialization should use {@link
   * #deserializeJavaObjectAndClass}.
   */
  public byte[] serializeJavaObjectAndClass(Object obj) {
    buffer.writerIndex(0);
    serializeJavaObjectAndClass(buffer, obj);
    return buffer.getBytes(0, buffer.writerIndex());
  }

  /**
   * Serialize java object with class info, deserialization should use {@link
   * #deserializeJavaObjectAndClass}.
   */
  public void serializeJavaObjectAndClass(MemoryBuffer buffer, Object obj) {
    try {
      jitContext.lock();
      if (config.shareMetaContext()) {
        int startOffset = buffer.writerIndex();
        buffer.writeInt(-1); // preserve 4-byte for nativeObjects start offsets.
        writeRef(buffer, obj);
        buffer.putInt(startOffset, buffer.writerIndex());
        classResolver.writeClassDefs(buffer);
      } else {
        writeRef(buffer, obj);
      }
    } finally {
      resetWrite();
      jitContext.unlock();
    }
  }

  /**
   * Serialize java object with class info, deserialization should use {@link
   * #deserializeJavaObjectAndClass}.
   */
  public void serializeJavaObjectAndClass(OutputStream outputStream, Object obj) {
    serializeToStream(outputStream, buf -> serializeJavaObjectAndClass(buf, obj));
  }

  /**
   * Deserialize class info and java object from binary, serialization should use {@link
   * #serializeJavaObjectAndClass}.
   */
  public Object deserializeJavaObjectAndClass(byte[] data) {
    return deserializeJavaObjectAndClass(MemoryBuffer.fromByteArray(data));
  }

  /**
   * Deserialize class info and java object from binary, serialization should use {@link
   * #serializeJavaObjectAndClass}.
   */
  public Object deserializeJavaObjectAndClass(MemoryBuffer buffer) {
    try {
      jitContext.lock();
      if (config.shareMetaContext()) {
        classResolver.readClassDefs(buffer);
      }
      return readRef(buffer);
    } finally {
      resetRead();
      jitContext.unlock();
    }
  }

  /**
   * Deserialize class info and java object from binary, serialization should use {@link
   * #serializeJavaObjectAndClass}.
   */
  public Object deserializeJavaObjectAndClass(InputStream inputStream) {
    return deserializeFromStream(inputStream, this::deserializeJavaObjectAndClass);
  }

  private void serializeToStream(OutputStream outputStream, Consumer<MemoryBuffer> function) {
    if (outputStream.getClass() == ByteArrayOutputStream.class) {
      byte[] oldBytes = buffer.getHeapMemory(); // Note: This should not be null.
      MemoryUtils.wrap((ByteArrayOutputStream) outputStream, buffer);
      int writerIndex = buffer.writerIndex();
      buffer.writeInt(-1);
      function.accept(buffer);
      buffer.putInt(writerIndex, buffer.writerIndex() - writerIndex);
      MemoryUtils.wrap(buffer, (ByteArrayOutputStream) outputStream);
      buffer.pointTo(oldBytes, 0, oldBytes.length);
    } else {
      buffer.writerIndex(0);
      buffer.writeInt(-1);
      function.accept(buffer);
      buffer.putInt(0, buffer.writerIndex() - 4);
      try {
        byte[] bytes = buffer.getHeapMemory();
        if (bytes != null) {
          outputStream.write(bytes, 0, buffer.writerIndex());
        } else {
          outputStream.write(buffer.getBytes(0, buffer.writerIndex()));
        }
        outputStream.flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private Object deserializeFromStream(
      InputStream inputStream, Function<MemoryBuffer, Object> function) {
    buffer.readerIndex(0);
    try {
      boolean isBis = inputStream.getClass() == ByteArrayInputStream.class;
      byte[] oldBytes = null;
      if (isBis) {
        oldBytes = buffer.getHeapMemory(); // Note: This should not be null.
        MemoryUtils.wrap((ByteArrayInputStream) inputStream, buffer);
        buffer.increaseReaderIndex(4); // skip size.
      } else {
        int read = inputStream.read(buffer.getHeapMemory(), 0, 4);
        Preconditions.checkArgument(read == 4);
        int size = buffer.readInt();
        buffer.ensure(4 + size);
        read = inputStream.read(buffer.getHeapMemory(), 4, size);
        Preconditions.checkArgument(read == size);
      }
      Object o = function.apply(buffer);
      if (isBis) {
        inputStream.skip(buffer.readerIndex());
        buffer.pointTo(oldBytes, 0, oldBytes.length);
      }
      return o;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void reset() {
    refResolver.reset();
    classResolver.reset();
    enumStringResolver.reset();
    serializationContext.reset();
    nativeObjects.clear();
    peerOutOfBandEnabled = false;
    bufferCallback = null;
    depth = 0;
  }

  public void resetWrite() {
    refResolver.resetWrite();
    classResolver.resetWrite();
    enumStringResolver.resetWrite();
    serializationContext.reset();
    nativeObjects.clear();
    bufferCallback = null;
    depth = 0;
  }

  public void resetRead() {
    refResolver.resetRead();
    classResolver.resetRead();
    enumStringResolver.resetRead();
    serializationContext.reset();
    nativeObjects.clear();
    peerOutOfBandEnabled = false;
    depth = 0;
  }

  public JITContext getJITContext() {
    return jitContext;
  }

  public BufferCallback getBufferCallback() {
    return bufferCallback;
  }

  public boolean isPeerOutOfBandEnabled() {
    return peerOutOfBandEnabled;
  }

  public RefResolver getRefResolver() {
    return refResolver;
  }

  public ClassResolver getClassResolver() {
    return classResolver;
  }

  public EnumStringResolver getEnumStringResolver() {
    return enumStringResolver;
  }

  public SerializationContext getSerializationContext() {
    return serializationContext;
  }

  public Generics getGenerics() {
    return generics;
  }

  public int getDepth() {
    return depth;
  }

  public void setDepth(int depth) {
    this.depth = depth;
  }

  // Invoked by jit
  public StringSerializer getStringSerializer() {
    return stringSerializer;
  }

  public ClassLoader getClassLoader() {
    return classLoader;
  }

  public Language getLanguage() {
    return language;
  }

  public boolean trackingRef() {
    return refTracking;
  }

  public boolean isStringRefIgnored() {
    return config.isStringRefIgnored();
  }

  public boolean isBasicTypesRefIgnored() {
    return config.isBasicTypesRefIgnored();
  }

  public boolean checkClassVersion() {
    return config.checkClassVersion();
  }

  public CompatibleMode getCompatibleMode() {
    return config.getCompatibleMode();
  }

  public Config getConfig() {
    return config;
  }

  public Class<? extends Serializer> getDefaultJDKStreamSerializerType() {
    return config.getDefaultJDKStreamSerializerType();
  }

  public boolean compressString() {
    return config.compressString();
  }

  public boolean compressInt() {
    return compressInt;
  }

  public boolean compressLong() {
    return compressLong;
  }

  public static FuryBuilder builder() {
    return new FuryBuilder();
  }

  public static final class FuryBuilder {
    private static final boolean ENABLE_CLASS_REGISTRATION_FORCIBLY;

    static {
      String flagValue =
          System.getProperty(
              "fury.enable_fury_security_mode_forcibly",
              System.getenv("ENABLE_CLASS_REGISTRATION_FORCIBLY"));
      if (flagValue == null) {
        flagValue = "false";
      }
      ENABLE_CLASS_REGISTRATION_FORCIBLY = "true".equals(flagValue) || "1".equals(flagValue);
    }

    boolean checkClassVersion = false;
    Language language = Language.JAVA;
    boolean trackingRef = false;
    boolean basicTypesRefIgnored = true;
    boolean stringRefIgnored = true;
    boolean timeRefIgnored = true;
    ClassLoader classLoader;
    boolean compressInt = false;
    boolean compressLong = false;
    boolean compressString = true;
    CompatibleMode compatibleMode = CompatibleMode.SCHEMA_CONSISTENT;
    boolean checkJdkClassSerializable = true;
    Class<? extends Serializer> defaultJDKStreamSerializerType = ObjectStreamSerializer.class;
    boolean requireClassRegistration = true;
    boolean shareMetaContext = false;
    boolean codeGenEnabled = true;
    public boolean deserializeUnexistedClass = false;
    public boolean asyncCompilationEnabled = false;
    public boolean registerGuavaTypes = true;

    private FuryBuilder() {}

    /**
     * Whether cross-language serialize the object. If you used fury for java only, please set
     * language to {@link Language#JAVA}, which will have much better performance.
     */
    public FuryBuilder withLanguage(Language language) {
      this.language = language;
      return this;
    }

    /** Whether track shared or circular references. */
    public FuryBuilder withRefTracking(boolean trackingRef) {
      this.trackingRef = trackingRef;
      return this;
    }

    /** Whether ignore basic types shared reference. */
    public FuryBuilder ignoreBasicTypesRef(boolean ignoreBasicTypesRef) {
      this.basicTypesRefIgnored = ignoreBasicTypesRef;
      return this;
    }

    /** Whether ignore string shared reference. */
    public FuryBuilder ignoreStringRef(boolean ignoreStringRef) {
      this.stringRefIgnored = ignoreStringRef;
      return this;
    }

    /**
     * Whether ignore reference tracking of all time types registered in {@link TimeSerializers}
     * when ref tracking is enabled.
     *
     * @see Config#isTimeRefIgnored
     */
    public FuryBuilder ignoreTimeRef(boolean ignoreTimeRef) {
      this.timeRefIgnored = ignoreTimeRef;
      return this;
    }

    /** Use variable length encoding for int/long. */
    public FuryBuilder withNumberCompressed(boolean numberCompressed) {
      this.compressInt = numberCompressed;
      this.compressLong = numberCompressed;
      return this;
    }

    /** Use variable length encoding for int. */
    public FuryBuilder withIntCompressed(boolean intCompressed) {
      this.compressInt = intCompressed;
      return this;
    }

    /** Use variable length encoding for long. */
    public FuryBuilder withLongCompressed(boolean longCompressed) {
      this.compressLong = longCompressed;
      return this;
    }

    /** Whether compress string for small size. */
    public FuryBuilder withStringCompressed(boolean stringCompressed) {
      this.compressString = stringCompressed;
      return this;
    }

    /**
     * Set classloader for fury to load classes, this classloader can't up updated. Fury will cache
     * the class meta data, if classloader can be updated, there may be class meta collision if
     * different classloaders have classes with same name.
     *
     * <p>If you want to change classloader, please use {@link io.fury.util.LoaderBinding} or {@link
     * ThreadSafeFury} to setup mapping between classloaders and fury instances.
     */
    public FuryBuilder withClassLoader(ClassLoader classLoader) {
      this.classLoader = classLoader;
      return this;
    }

    /**
     * Set class schema compatible mode.
     *
     * @see CompatibleMode
     */
    public FuryBuilder withCompatibleMode(CompatibleMode compatibleMode) {
      this.compatibleMode = compatibleMode;
      return this;
    }

    /**
     * Whether check class schema consistency, will be disabled automatically when {@link
     * CompatibleMode#COMPATIBLE} is enabled. Do not disable this option unless you can ensure the
     * class won't evolve.
     */
    public FuryBuilder withClassVersionCheck(boolean checkClassVersion) {
      this.checkClassVersion = checkClassVersion;
      return this;
    }

    /** Whether check classes under `java.*` implement {@link java.io.Serializable}. */
    public FuryBuilder withJdkClassSerializableCheck(boolean checkJdkClassSerializable) {
      this.checkJdkClassSerializable = checkJdkClassSerializable;
      return this;
    }

    /**
     * Whether pre-register guava types such as `RegularImmutableMap`/`RegularImmutableList`. Those
     * types are not public API, but seems pretty stable.
     *
     * @see io.fury.serializer.GuavaSerializers
     */
    public FuryBuilder registerGuavaTypes(boolean register) {
      this.registerGuavaTypes = register;
      return this;
    }

    /**
     * Whether to require registering classes for serialization, enabled by default. If disabled,
     * unknown classes can be deserialized, which may be insecure and cause remote code execution
     * attack if the classes `constructor`/`equals`/`hashCode` method contain malicious code. Do not
     * disable class registration if you can't ensure your environment are *indeed secure*. We are
     * not responsible for security risks if you disable this option. If you disable this option,
     * you can configure {@link io.fury.resolver.ClassChecker} by {@link
     * ClassResolver#setClassChecker} to control which classes are allowed being serialized.
     */
    public FuryBuilder requireClassRegistration(boolean requireClassRegistration) {
      this.requireClassRegistration = requireClassRegistration;
      return this;
    }

    /** Whether to enable meta share mode. */
    public FuryBuilder withMetaContextShare(boolean shareMetaContext) {
      this.shareMetaContext = shareMetaContext;
      return this;
    }

    /**
     * Whether deserialize/skip data of un-existed class.
     *
     * @see Config#deserializeUnexistedClass()
     */
    public FuryBuilder withDeserializeUnexistedClass(boolean deserializeUnexistedClass) {
      this.deserializeUnexistedClass = deserializeUnexistedClass;
      return this;
    }

    /**
     * Whether enable jit for serialization. When disabled, the first serialization will be faster
     * since no need to generate code, but later will be much slower compared jit mode.
     */
    public FuryBuilder withCodegen(boolean codeGen) {
      this.codeGenEnabled = codeGen;
      return this;
    }

    /**
     * Whether enable async compilation. If enabled, serialization will use interpreter mode
     * serialization first and switch to jit serialization after async serializer jit for a class \
     * is finished.
     *
     * @see Config#isAsyncCompilationEnabled()
     */
    public FuryBuilder withAsyncCompilation(boolean asyncCompilation) {
      this.asyncCompilationEnabled = asyncCompilation;
      return this;
    }

    private void finish() {
      if (classLoader == null) {
        classLoader = Thread.currentThread().getContextClassLoader();
      }
      if (language != Language.JAVA) {
        stringRefIgnored = false;
      }
      if (ENABLE_CLASS_REGISTRATION_FORCIBLY) {
        if (!requireClassRegistration) {
          LOG.warn("Class registration is enabled forcibly.");
          requireClassRegistration = true;
        }
      }
      if (defaultJDKStreamSerializerType == JavaSerializer.class) {
        LOG.warn(
            "JDK serialization is used for types which customized java serialization by "
                + "implementing methods such as writeObject/readObject. This is not secure, try to "
                + "use {} instead, or implement a custom {}.",
            ObjectStreamSerializer.class,
            Serializer.class);
      }
      if (compatibleMode == CompatibleMode.COMPATIBLE) {
        checkClassVersion = false;
      }
      if (!requireClassRegistration) {
        LOG.warn(
            "Class registration isn't forced, unknown classes can be deserialized. "
                + "If the environment isn't secure, please enable class registration by "
                + "`FuryBuilder#requireClassRegistration(true)` or configure ClassChecker by "
                + "`ClassResolver#setClassChecker`");
      }
    }

    public Fury build() {
      finish();
      ClassLoader loader = this.classLoader;
      // clear classLoader to avoid `LoaderBinding#furyFactory` lambda capture classLoader by
      // capturing `FuryBuilder`, which make `classLoader` not able to be gc.
      this.classLoader = null;
      return newFury(this, loader);
    }

    /** Build thread safe fury. */
    public ThreadSafeFury buildThreadSafeFury() {
      return buildThreadLocalFury();
    }

    /** Build thread safe fury backed by {@link ThreadLocalFury}. */
    public ThreadLocalFury buildThreadLocalFury() {
      finish();
      ClassLoader loader = this.classLoader;
      // clear classLoader to avoid `LoaderBinding#furyFactory` lambda capture classLoader by
      // capturing `FuryBuilder`,  which make `classLoader` not able to be gc.
      this.classLoader = null;
      ThreadLocalFury threadSafeFury =
          new ThreadLocalFury(classLoader -> newFury(FuryBuilder.this, classLoader));
      threadSafeFury.setClassLoader(loader);
      return threadSafeFury;
    }

    /**
     * Build pooled ThreadSafeFury.
     *
     * @param minPoolSize min pool size
     * @param maxPoolSize max pool size
     * @return ThreadSafeFuryPool
     */
    public ThreadSafeFury buildThreadSafeFuryPool(int minPoolSize, int maxPoolSize) {
      return buildThreadSafeFuryPool(minPoolSize, maxPoolSize, 30L, TimeUnit.SECONDS);
    }

    /**
     * Build pooled ThreadSafeFury.
     *
     * @param minPoolSize min pool size
     * @param maxPoolSize max pool size
     * @param expireTime cache expire time, default 5's
     * @param timeUnit TimeUnit, default SECONDS
     * @return ThreadSafeFuryPool
     */
    public ThreadSafeFury buildThreadSafeFuryPool(
        int minPoolSize, int maxPoolSize, long expireTime, TimeUnit timeUnit) {
      if (minPoolSize < 0 || maxPoolSize < 0 || minPoolSize > maxPoolSize) {
        throw new IllegalArgumentException(
            String.format(
                "thread safe fury pool's init pool size error, please check it, min:[%s], max:[%s]",
                minPoolSize, maxPoolSize));
      }
      finish();
      ClassLoader loader = this.classLoader;
      this.classLoader = null;
      ThreadSafeFury threadSafeFury =
          new ThreadPoolFury(
              classLoader -> newFury(FuryBuilder.this, classLoader),
              minPoolSize,
              maxPoolSize,
              expireTime,
              timeUnit);
      threadSafeFury.setClassLoader(loader);
      return threadSafeFury;
    }
  }

  /**
   * Create Fury and print exception when failed. Many application will create fury as a static
   * variable, Fury creation exception will be swallowed by {@link NoClassDefFoundError}. We print
   * exception explicitly for better debugging.
   */
  private static Fury newFury(FuryBuilder builder, ClassLoader classLoader) {
    try {
      return new Fury(builder, classLoader);
    } catch (Throwable t) {
      t.printStackTrace();
      LOG.error("Fury creation failed with classloader {}", classLoader);
      Platform.throwException(t);
      throw new RuntimeException(t);
    }
  }
}
