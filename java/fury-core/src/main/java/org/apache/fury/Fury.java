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

package org.apache.fury;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.fury.builder.JITContext;
import org.apache.fury.collection.ObjectArray;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.Config;
import org.apache.fury.config.FuryBuilder;
import org.apache.fury.config.Language;
import org.apache.fury.config.LongEncoding;
import org.apache.fury.exception.DeserializationException;
import org.apache.fury.io.FuryInputStream;
import org.apache.fury.io.FuryReadableChannel;
import org.apache.fury.logging.Logger;
import org.apache.fury.logging.LoggerFactory;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.resolver.ClassInfo;
import org.apache.fury.resolver.ClassInfoHolder;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.MapRefResolver;
import org.apache.fury.resolver.MetaStringResolver;
import org.apache.fury.resolver.NoRefResolver;
import org.apache.fury.resolver.RefResolver;
import org.apache.fury.resolver.SerializationContext;
import org.apache.fury.serializer.ArraySerializers;
import org.apache.fury.serializer.BufferCallback;
import org.apache.fury.serializer.BufferObject;
import org.apache.fury.serializer.OpaqueObjects;
import org.apache.fury.serializer.PrimitiveSerializers.LongSerializer;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.serializer.SerializerFactory;
import org.apache.fury.serializer.StringSerializer;
import org.apache.fury.type.Generics;
import org.apache.fury.type.Type;
import org.apache.fury.util.ExceptionUtils;
import org.apache.fury.util.Platform;
import org.apache.fury.util.Preconditions;
import org.apache.fury.util.StringUtils;

/**
 * Cross-Lang Data layout: 1byte mask: 1-bit null: 0->null, 1->not null 1-bit endianness: 0->le,
 * 1->be 1-bit target lang: 0->native, 1->x_lang if x_lang, will write current process language as a
 * byte into buffer. 1-bit out-of-band serialization enable flag: 0 -> not enabled, 1 -> enabled.
 * other bits reserved.
 *
 * <p>serialize/deserialize is user API for root object serialization, write/read api is for inner
 * serialization.
 */
@NotThreadSafe
public final class Fury implements BaseFury {
  private static final Logger LOG = LoggerFactory.getLogger(Fury.class);

  public static final byte NULL_FLAG = -3;
  // This flag indicates that object is a not-null value.
  // We don't use another byte to indicate REF, so that we can save one byte.
  public static final byte REF_FLAG = -2;
  // this flag indicates that the object is a non-null value.
  public static final byte NOT_NULL_VALUE_FLAG = -1;
  // this flag indicates that the object is a referencable and first write.
  public static final byte REF_VALUE_FLAG = 0;
  public static final byte NOT_SUPPORT_CROSS_LANGUAGE = 0;
  public static final short FURY_TYPE_TAG_ID = Type.FURY_TYPE_TAG.getId();
  private static final byte isNilFlag = 1;
  private static final byte isLittleEndianFlag = 1 << 1;
  private static final byte isCrossLanguageFlag = 1 << 2;
  private static final byte isOutOfBandFlag = 1 << 3;
  private static final boolean isLittleEndian = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
  private static final byte BITMAP = isLittleEndian ? isLittleEndianFlag : 0;
  private static final int BUFFER_SIZE_LIMIT = 128 * 1024;
  private static final short MAGIC_NUMBER = 0x62D4;

  private final Config config;
  private final boolean refTracking;
  private final RefResolver refResolver;
  private final ClassResolver classResolver;
  private final MetaStringResolver metaStringResolver;
  private final SerializationContext serializationContext;
  private final ClassLoader classLoader;
  private final JITContext jitContext;
  private MemoryBuffer buffer;
  private final List<Object> nativeObjects;
  private final StringSerializer stringSerializer;
  private final Language language;
  private final boolean compressInt;
  private final LongEncoding longEncoding;
  private final Generics generics;
  private Language peerLanguage;
  private BufferCallback bufferCallback;
  private Iterator<MemoryBuffer> outOfBandBuffers;
  private boolean peerOutOfBandEnabled;
  private int depth;

  public Fury(FuryBuilder builder, ClassLoader classLoader) {
    // Avoid set classLoader in `FuryBuilder`, which won't be clear when
    // `org.apache.fury.ThreadSafeFury.clearClassLoader` is called.
    config = new Config(builder);
    this.language = config.getLanguage();
    this.refTracking = config.trackingRef();
    compressInt = config.compressInt();
    longEncoding = config.longEncoding();
    if (refTracking) {
      this.refResolver = new MapRefResolver();
    } else {
      this.refResolver = new NoRefResolver();
    }
    jitContext = new JITContext(this);
    metaStringResolver = new MetaStringResolver();
    classResolver = new ClassResolver(this);
    classResolver.initialize();
    serializationContext = new SerializationContext();
    this.classLoader = classLoader;
    nativeObjects = new ArrayList<>();
    generics = new Generics(this);
    stringSerializer = new StringSerializer(this);
    LOG.info("Created new fury {}", this);
  }

  @Override
  public void register(Class<?> cls) {
    classResolver.register(cls);
  }

  @Override
  public void register(Class<?> cls, boolean createSerializer) {
    classResolver.register(cls, createSerializer);
  }

  @Override
  public void register(Class<?> cls, Short id) {
    classResolver.register(cls, id);
  }

  @Override
  public void register(Class<?> cls, Short id, boolean createSerializer) {
    classResolver.register(cls, id, createSerializer);
  }

  /** register class with given type tag which will be used for cross-language serialization. */
  public void register(Class<?> cls, String typeTag) {
    classResolver.register(cls, typeTag);
  }

  @Override
  public <T> void registerSerializer(Class<T> type, Class<? extends Serializer> serializerClass) {
    classResolver.registerSerializer(type, serializerClass);
  }

  @Override
  public void registerSerializer(Class<?> type, Serializer<?> serializer) {
    classResolver.registerSerializer(type, serializer);
  }

  @Override
  public void setSerializerFactory(SerializerFactory serializerFactory) {
    classResolver.setSerializerFactory(serializerFactory);
  }

  public SerializerFactory getSerializerFactory() {
    return classResolver.getSerializerFactory();
  }

  @Override
  public MemoryBuffer serialize(Object obj, long address, int size) {
    MemoryBuffer buffer = MemoryUtils.buffer(address, size);
    serialize(buffer, obj, null);
    return buffer;
  }

  @Override
  public byte[] serialize(Object obj) {
    MemoryBuffer buf = getBuffer();
    buf.writerIndex(0);
    serialize(buf, obj, null);
    byte[] bytes = buf.getBytes(0, buf.writerIndex());
    resetBuffer();
    return bytes;
  }

  @Override
  public byte[] serialize(Object obj, BufferCallback callback) {
    MemoryBuffer buf = getBuffer();
    buf.writerIndex(0);
    serialize(buf, obj, callback);
    byte[] bytes = buf.getBytes(0, buf.writerIndex());
    resetBuffer();
    return bytes;
  }

  @Override
  public MemoryBuffer serialize(MemoryBuffer buffer, Object obj) {
    return serialize(buffer, obj, null);
  }

  @Override
  public MemoryBuffer serialize(MemoryBuffer buffer, Object obj, BufferCallback callback) {
    if (language == Language.XLANG) {
      buffer.writeInt16(MAGIC_NUMBER);
    }
    byte bitmap = BITMAP;
    if (language != Language.JAVA) {
      bitmap |= isCrossLanguageFlag;
    }
    if (obj == null) {
      bitmap |= isNilFlag;
      buffer.writeByte(bitmap);
      return buffer;
    }
    if (callback != null) {
      bitmap |= isOutOfBandFlag;
      bufferCallback = callback;
    }
    buffer.writeByte(bitmap);
    try {
      jitContext.lock();
      if (depth != 0) {
        throwDepthSerializationException();
      }
      if (language == Language.JAVA) {
        write(buffer, obj);
      } else {
        buffer.writeByte((byte) Language.JAVA.ordinal());
        xserializeInternal(buffer, obj);
      }
      return buffer;
    } catch (StackOverflowError t) {
      throw processStackOverflowError(t);
    } finally {
      resetWrite();
      jitContext.unlock();
    }
  }

  @Override
  public void serialize(OutputStream outputStream, Object obj) {
    serializeToStream(outputStream, buf -> serialize(buf, obj, null));
  }

  @Override
  public void serialize(OutputStream outputStream, Object obj, BufferCallback callback) {
    serializeToStream(outputStream, buf -> serialize(buf, obj, callback));
  }

  private StackOverflowError processStackOverflowError(StackOverflowError e) {
    if (!refTracking) {
      String msg =
          "Object may contain circular references, please enable ref tracking "
              + "by `FuryBuilder#withRefTracking(true)`";
      String rawMessage = e.getMessage();
      if (StringUtils.isNotBlank(rawMessage)) {
        msg += ": " + rawMessage;
      }
      StackOverflowError t1 = ExceptionUtils.trySetStackOverflowErrorMessage(e, msg);
      if (t1 != null) {
        return t1;
      }
    }
    throw e;
  }

  private MemoryBuffer getBuffer() {
    MemoryBuffer buf = buffer;
    if (buf == null) {
      buf = buffer = MemoryBuffer.newHeapBuffer(64);
    }
    return buf;
  }

  private void resetBuffer() {
    MemoryBuffer buf = buffer;
    if (buf != null && buf.size() > BUFFER_SIZE_LIMIT) {
      buffer = MemoryBuffer.newHeapBuffer(BUFFER_SIZE_LIMIT);
    }
  }

  private void write(MemoryBuffer buffer, Object obj) {
    int startOffset = buffer.writerIndex();
    boolean shareMetaContext = config.shareMetaContext();
    if (shareMetaContext) {
      buffer.writeInt32(-1); // preserve 4-byte for nativeObjects start offsets.
    }
    // reduce caller stack
    if (!refResolver.writeRefOrNull(buffer, obj)) {
      ClassInfo classInfo = classResolver.getOrUpdateClassInfo(obj.getClass());
      classResolver.writeClass(buffer, classInfo);
      writeData(buffer, classInfo, obj);
    }
    if (shareMetaContext) {
      buffer.putInt32(startOffset, buffer.writerIndex());
      classResolver.writeClassDefs(buffer);
    }
  }

  private void xserializeInternal(MemoryBuffer buffer, Object obj) {
    int startOffset = buffer.writerIndex();
    buffer.writeInt32(-1); // preserve 4-byte for nativeObjects start offsets.
    buffer.writeInt32(-1); // preserve 4-byte for nativeObjects size
    xwriteRef(buffer, obj);
    buffer.putInt32(startOffset, buffer.writerIndex());
    buffer.putInt32(startOffset + 4, nativeObjects.size());
    refResolver.resetWrite();
    // fury write opaque object classname which cause later write of classname only write an id.
    classResolver.resetWrite();
    metaStringResolver.resetWrite();
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

  public void writeRef(MemoryBuffer buffer, Object obj, ClassInfoHolder classInfoHolder) {
    if (!refResolver.writeRefOrNull(buffer, obj)) {
      ClassInfo classInfo = classResolver.getClassInfo(obj.getClass(), classInfoHolder);
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

  /** Write object class and data without tracking ref. */
  public void writeNullable(MemoryBuffer buffer, Object obj) {
    if (obj == null) {
      buffer.writeByte(Fury.NULL_FLAG);
    } else {
      buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
      writeNonRef(buffer, obj);
    }
  }

  /** Write object class and data without tracking ref. */
  public void writeNullable(MemoryBuffer buffer, Object obj, ClassInfoHolder classInfoHolder) {
    if (obj == null) {
      buffer.writeByte(Fury.NULL_FLAG);
    } else {
      buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
      writeNonRef(buffer, obj, classResolver.getClassInfo(obj.getClass(), classInfoHolder));
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
    buffer.writeInt16(typeId);
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
      buffer.writeVarUint32(nativeObjects.size());
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
        buffer.writeInt16((Short) obj);
        break;
      case ClassResolver.INTEGER_CLASS_ID:
        if (compressInt) {
          buffer.writeVarInt32((Integer) obj);
        } else {
          buffer.writeInt32((Integer) obj);
        }
        break;
      case ClassResolver.FLOAT_CLASS_ID:
        buffer.writeFloat32((Float) obj);
        break;
      case ClassResolver.LONG_CLASS_ID:
        LongSerializer.writeInt64(buffer, (Long) obj, longEncoding);
        break;
      case ClassResolver.DOUBLE_CLASS_ID:
        buffer.writeFloat64((Double) obj);
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
        buffer.writeVarUint32Aligned(totalBytes);
      } else {
        buffer.writeVarUint32(totalBytes);
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
        buffer.writeVarUint32Aligned(totalBytes);
      } else {
        buffer.writeVarUint32(totalBytes);
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
        size = buffer.readAlignedVarUint();
      } else {
        size = buffer.readVarUint32();
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

  public void writeInt64(MemoryBuffer buffer, long value) {
    LongSerializer.writeInt64(buffer, value, longEncoding);
  }

  public long readInt64(MemoryBuffer buffer) {
    return LongSerializer.readInt64(buffer, longEncoding);
  }

  @Override
  public Object deserialize(byte[] bytes) {
    return deserialize(MemoryUtils.wrap(bytes), null);
  }

  @Override
  public Object deserialize(byte[] bytes, Iterable<MemoryBuffer> outOfBandBuffers) {
    return deserialize(MemoryUtils.wrap(bytes), outOfBandBuffers);
  }

  @Override
  public Object deserialize(long address, int size) {
    return deserialize(MemoryUtils.buffer(address, size), null);
  }

  @Override
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
  @Override
  public Object deserialize(MemoryBuffer buffer, Iterable<MemoryBuffer> outOfBandBuffers) {
    try {
      jitContext.lock();
      if (depth != 0) {
        throwDepthDeserializationException();
      }
      if (language == Language.XLANG) {
        short magicNumber = buffer.readInt16();
        assert magicNumber == MAGIC_NUMBER
            : String.format(
                "The fury xlang serialization must start with magic number 0x%x. Please "
                    + "check whether the serialization is based on the xlang protocol and the data didn't corrupt.",
                MAGIC_NUMBER);
      }
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
    } catch (Throwable t) {
      throw handleReadFailed(t);
    } finally {
      resetRead();
      jitContext.unlock();
    }
  }

  @Override
  public Object deserialize(FuryInputStream inputStream) {
    return deserialize(inputStream, null);
  }

  @Override
  public Object deserialize(FuryInputStream inputStream, Iterable<MemoryBuffer> outOfBandBuffers) {
    try {
      MemoryBuffer buf = inputStream.getBuffer();
      return deserialize(buf, outOfBandBuffers);
    } finally {
      inputStream.shrinkBuffer();
    }
  }

  @Override
  public Object deserialize(FuryReadableChannel channel) {
    return deserialize(channel, null);
  }

  @Override
  public Object deserialize(FuryReadableChannel channel, Iterable<MemoryBuffer> outOfBandBuffers) {
    MemoryBuffer buf = channel.getBuffer();
    return deserialize(buf, outOfBandBuffers);
  }

  private RuntimeException handleReadFailed(Throwable t) {
    if (refResolver instanceof MapRefResolver) {
      ObjectArray readObjects = ((MapRefResolver) refResolver).getReadObjects();
      // carry with read objects for better trouble shooting.
      List<Object> objects = Arrays.asList(readObjects.objects).subList(0, readObjects.size);
      throw new DeserializationException(objects, t);
    } else {
      Platform.throwException(t);
      throw new IllegalStateException("unreachable");
    }
  }

  private Object xdeserializeInternal(MemoryBuffer buffer) {
    Object obj;
    int nativeObjectsStartOffset = buffer.readInt32();
    int nativeObjectsSize = buffer.readInt32();
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
      metaStringResolver.resetRead();
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
      Object o = readDataInternal(buffer, classResolver.readClassInfo(buffer));
      refResolver.setReadObject(nextReadRefId, o);
      return o;
    } else {
      return refResolver.getReadObject();
    }
  }

  public Object readRef(MemoryBuffer buffer, ClassInfoHolder classInfoHolder) {
    RefResolver refResolver = this.refResolver;
    int nextReadRefId = refResolver.tryPreserveRefId(buffer);
    if (nextReadRefId >= NOT_NULL_VALUE_FLAG) {
      // ref value or not-null value
      Object o = readDataInternal(buffer, classResolver.readClassInfo(buffer, classInfoHolder));
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
    return readDataInternal(buffer, classResolver.readClassInfo(buffer));
  }

  public Object readNonRef(MemoryBuffer buffer, ClassInfoHolder classInfoHolder) {
    return readDataInternal(buffer, classResolver.readClassInfo(buffer, classInfoHolder));
  }

  /** Read object class and data without tracking ref. */
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
        return buffer.readInt16();
      case ClassResolver.INTEGER_CLASS_ID:
        if (compressInt) {
          return buffer.readVarInt32();
        } else {
          return buffer.readInt32();
        }
      case ClassResolver.FLOAT_CLASS_ID:
        return buffer.readFloat32();
      case ClassResolver.LONG_CLASS_ID:
        return LongSerializer.readInt64(buffer, longEncoding);
      case ClassResolver.DOUBLE_CLASS_ID:
        return buffer.readFloat64();
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
    short typeId = buffer.readInt16();
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
      int ordinal = buffer.readVarUint32();
      if (peerLanguage != Language.JAVA) {
        return OpaqueObjects.of(peerLanguage, className, ordinal);
      } else {
        return nativeObjects.get(ordinal);
      }
    }
  }

  @Override
  public byte[] serializeJavaObject(Object obj) {
    MemoryBuffer buf = getBuffer();
    buf.writerIndex(0);
    serializeJavaObject(buf, obj);
    byte[] bytes = buf.getBytes(0, buf.writerIndex());
    resetBuffer();
    return bytes;
  }

  @Override
  public void serializeJavaObject(MemoryBuffer buffer, Object obj) {
    try {
      jitContext.lock();
      if (depth != 0) {
        throwDepthSerializationException();
      }
      if (config.shareMetaContext()) {
        int startOffset = buffer.writerIndex();
        buffer.writeInt32(-1); // preserve 4-byte for nativeObjects start offsets.
        if (!refResolver.writeRefOrNull(buffer, obj)) {
          ClassInfo classInfo = classResolver.getOrUpdateClassInfo(obj.getClass());
          writeData(buffer, classInfo, obj);
        }
        buffer.putInt32(startOffset, buffer.writerIndex());
        classResolver.writeClassDefs(buffer);
      } else {
        if (!refResolver.writeRefOrNull(buffer, obj)) {
          ClassInfo classInfo = classResolver.getOrUpdateClassInfo(obj.getClass());
          writeData(buffer, classInfo, obj);
        }
      }
    } catch (StackOverflowError t) {
      throw processStackOverflowError(t);
    } finally {
      resetWrite();
      jitContext.unlock();
    }
  }

  /**
   * Serialize java object without class info, deserialization should use {@link
   * #deserializeJavaObject}.
   */
  @Override
  public void serializeJavaObject(OutputStream outputStream, Object obj) {
    serializeToStream(outputStream, buf -> serializeJavaObject(buf, obj));
  }

  @Override
  public <T> T deserializeJavaObject(byte[] data, Class<T> cls) {
    return deserializeJavaObject(MemoryBuffer.fromByteArray(data), cls);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T deserializeJavaObject(MemoryBuffer buffer, Class<T> cls) {
    try {
      jitContext.lock();
      if (depth != 0) {
        throwDepthDeserializationException();
      }
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
    } catch (Throwable t) {
      throw handleReadFailed(t);
    } finally {
      resetRead();
      jitContext.unlock();
    }
  }

  /**
   * Deserialize java object from binary by passing class info, serialization should use {@link
   * #serializeJavaObject}.
   */
  @Override
  public <T> T deserializeJavaObject(FuryInputStream inputStream, Class<T> cls) {
    try {
      MemoryBuffer buf = inputStream.getBuffer();
      return deserializeJavaObject(buf, cls);
    } finally {
      inputStream.shrinkBuffer();
    }
  }

  /**
   * Deserialize java object from binary channel by passing class info, serialization should use
   * {@link #serializeJavaObject}.
   */
  @Override
  public <T> T deserializeJavaObject(FuryReadableChannel channel, Class<T> cls) {
    MemoryBuffer buf = channel.getBuffer();
    return deserializeJavaObject(buf, cls);
  }

  /**
   * Deserialize java object from binary by passing class info, serialization should use {@link
   * #deserializeJavaObjectAndClass}.
   */
  @Override
  public byte[] serializeJavaObjectAndClass(Object obj) {
    MemoryBuffer buf = getBuffer();
    buf.writerIndex(0);
    serializeJavaObjectAndClass(buf, obj);
    byte[] bytes = buf.getBytes(0, buf.writerIndex());
    resetBuffer();
    return bytes;
  }

  /**
   * Serialize java object with class info, deserialization should use {@link
   * #deserializeJavaObjectAndClass}.
   */
  @Override
  public void serializeJavaObjectAndClass(MemoryBuffer buffer, Object obj) {
    try {
      jitContext.lock();
      if (depth != 0) {
        throwDepthSerializationException();
      }
      write(buffer, obj);
    } catch (StackOverflowError t) {
      throw processStackOverflowError(t);
    } finally {
      resetWrite();
      jitContext.unlock();
    }
  }

  /**
   * Serialize java object with class info, deserialization should use {@link
   * #deserializeJavaObjectAndClass}.
   */
  @Override
  public void serializeJavaObjectAndClass(OutputStream outputStream, Object obj) {
    serializeToStream(outputStream, buf -> serializeJavaObjectAndClass(buf, obj));
  }

  /**
   * Deserialize class info and java object from binary, serialization should use {@link
   * #serializeJavaObjectAndClass}.
   */
  @Override
  public Object deserializeJavaObjectAndClass(byte[] data) {
    return deserializeJavaObjectAndClass(MemoryBuffer.fromByteArray(data));
  }

  /**
   * Deserialize class info and java object from binary, serialization should use {@link
   * #serializeJavaObjectAndClass}.
   */
  @Override
  public Object deserializeJavaObjectAndClass(MemoryBuffer buffer) {
    try {
      jitContext.lock();
      if (depth != 0) {
        throwDepthDeserializationException();
      }
      if (config.shareMetaContext()) {
        classResolver.readClassDefs(buffer);
      }
      return readRef(buffer);
    } catch (Throwable t) {
      throw handleReadFailed(t);
    } finally {
      resetRead();
      jitContext.unlock();
    }
  }

  /**
   * Deserialize class info and java object from binary, serialization should use {@link
   * #serializeJavaObjectAndClass}.
   */
  @Override
  public Object deserializeJavaObjectAndClass(FuryInputStream inputStream) {
    try {
      MemoryBuffer buf = inputStream.getBuffer();
      return deserializeJavaObjectAndClass(buf);
    } finally {
      inputStream.shrinkBuffer();
    }
  }

  /**
   * Deserialize class info and java object from binary channel, serialization should use {@link
   * #serializeJavaObjectAndClass}.
   */
  @Override
  public Object deserializeJavaObjectAndClass(FuryReadableChannel channel) {
    MemoryBuffer buf = channel.getBuffer();
    return deserializeJavaObjectAndClass(buf);
  }

  private void serializeToStream(OutputStream outputStream, Consumer<MemoryBuffer> function) {
    MemoryBuffer buf = getBuffer();
    if (outputStream.getClass() == ByteArrayOutputStream.class) {
      byte[] oldBytes = buf.getHeapMemory(); // Note: This should not be null.
      assert oldBytes != null;
      MemoryUtils.wrap((ByteArrayOutputStream) outputStream, buf);
      function.accept(buf);
      MemoryUtils.wrap(buf, (ByteArrayOutputStream) outputStream);
      buf.pointTo(oldBytes, 0, oldBytes.length);
    } else {
      buf.writerIndex(0);
      function.accept(buf);
      try {
        byte[] bytes = buf.getHeapMemory();
        if (bytes != null) {
          outputStream.write(bytes, 0, buf.writerIndex());
        } else {
          outputStream.write(buf.getBytes(0, buf.writerIndex()));
        }
        outputStream.flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        resetBuffer();
      }
    }
  }

  public void reset() {
    refResolver.reset();
    classResolver.reset();
    metaStringResolver.reset();
    serializationContext.reset();
    nativeObjects.clear();
    peerOutOfBandEnabled = false;
    bufferCallback = null;
    depth = 0;
  }

  public void resetWrite() {
    refResolver.resetWrite();
    classResolver.resetWrite();
    metaStringResolver.resetWrite();
    serializationContext.reset();
    nativeObjects.clear();
    bufferCallback = null;
    depth = 0;
  }

  public void resetRead() {
    refResolver.resetRead();
    classResolver.resetRead();
    metaStringResolver.resetRead();
    serializationContext.reset();
    nativeObjects.clear();
    peerOutOfBandEnabled = false;
    depth = 0;
  }

  private void throwDepthSerializationException() {
    String method = "Fury#" + (language != Language.JAVA ? "x" : "") + "writeXXX";
    throw new IllegalStateException(
        String.format(
            "Nested call Fury.serializeXXX is not allowed when serializing, Please use %s instead",
            method));
  }

  private void throwDepthDeserializationException() {
    String method = "Fury#" + (language != Language.JAVA ? "x" : "") + "readXXX";
    throw new IllegalStateException(
        String.format(
            "Nested call Fury.deserializeXXX is not allowed when deserializing, Please use %s instead",
            method));
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

  public MetaStringResolver getMetaStringResolver() {
    return metaStringResolver;
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

  public void incDepth(int diff) {
    this.depth += diff;
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

  public LongEncoding longEncoding() {
    return longEncoding;
  }

  public boolean compressLong() {
    return config.compressLong();
  }

  public static FuryBuilder builder() {
    return new FuryBuilder();
  }
}
