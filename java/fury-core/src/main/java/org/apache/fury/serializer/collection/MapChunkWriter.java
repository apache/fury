package org.apache.fury.serializer.collection;

import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.resolver.ClassInfo;
import org.apache.fury.resolver.ClassInfoHolder;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.RefResolver;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.util.Preconditions;

public class MapChunkWriter {

  private static final int MAX_CHUNK_SIZE = 127;

  public MapChunkWriter(Fury fury) {
    this.fury = fury;
  }

  private int header = 0;
  private int startOffset;
  private int chunkSize;
  private Class<?> keyClass = null;
  private Class<?> valueClass = null;
  private final Fury fury;
  private boolean writeKeyClassInfo = false;
  private boolean writeValueClassInfo = false;
  private boolean keyIsNotSameType = false;
  private boolean valueIsNotSameType = false;
  private boolean prevKeyIsNull = false;
  private Serializer keySerializer;
  private Serializer valueSerializer;

  /** mark chunk write finish */
  private boolean markChunkWriteFinish = false;

  /**
   * preserve two byte for header and chunk size and record the write index so that we can write key
   * value at first, write header and chunk size when the chunk is finish at correct position
   */
  private boolean hasPreservedByte = false;

  private void preserveByteForHeaderAndChunkSize(MemoryBuffer memoryBuffer) {
    if (hasPreservedByte) {
      return;
    }
    int writerIndex = memoryBuffer.writerIndex();
    // preserve two byte for header and chunk size
    memoryBuffer.writerIndex(writerIndex + 2);
    this.startOffset = writerIndex;
    hasPreservedByte = true;
  }

  public MapChunkWriter next(Object key, Object value, MemoryBuffer buffer) {
    if (!markChunkWriteFinish) {
      if (key == null) {
        prevKeyIsNull = true;
        if (chunkSize > 0) {
          reset(buffer);
        }
      }
      if (prevKeyIsNull && key != null) {
        reset(buffer);
      }
      if (value == null && chunkSize > 0 && !valueHasNull()) {
        // if value has null before, no need to reset chunk
        reset(buffer);
      }
      if (!keyIsNotSameType) {
        this.keyIsNotSameType = judgeKeyIsNotSameType(key);
        if (keyIsNotSameType) {
          if (valueIsNotSameType) {
            markChunkWriteFinish(buffer);
          } else {
            reset(buffer);
          }
        }
      }
      if (!valueIsNotSameType) {
        this.valueIsNotSameType = judgeValueIsNotSameType(value);
        if (valueIsNotSameType) {
          if (keyIsNotSameType) {
            markChunkWriteFinish(buffer);
          } else {
            reset(buffer);
          }
        }
      }
      if (chunkSize >= MAX_CHUNK_SIZE) {
        reset(buffer);
      }
    }
    return this;
  }

  public void increaseChunkSize() {
    chunkSize++;
  }

  public void generalChunkWrite(
      Object key,
      Object value,
      MemoryBuffer memoryBuffer,
      ClassResolver classResolver,
      RefResolver refResolver,
      ClassInfoHolder keyClassInfoWriteCache,
      ClassInfoHolder valueClassInfoWriteCache) {
    final boolean trackingRef = fury.trackingRef();
    writeKey(key, memoryBuffer, classResolver, refResolver, trackingRef, keyClassInfoWriteCache);
    writeValue(
        value, memoryBuffer, classResolver, refResolver, trackingRef, valueClassInfoWriteCache);
    increaseChunkSize();
  }

  public void writeFinalKey(Object key, MemoryBuffer buffer, Serializer keySerializer) {
    preserveByteForHeaderAndChunkSize(buffer);
    boolean trackingKeyRef = keySerializer.needToWriteRef();
    if (!trackingKeyRef) {
      // map key has one null at most, use one chunk to write
      if (key == null) {
        header |= MapFlags.KEY_HAS_NULL;
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        keySerializer.write(buffer, key);
      }
    } else {
      updateFinalKeyHeader(key, true);
      RefResolver refResolver = fury.getRefResolver();
      if (!refResolver.writeRefOrNull(buffer, key)) {
        keySerializer.write(buffer, key);
      }
    }
  }

  public Object readFinalKey(MemoryBuffer buffer, int header, Serializer keySerializer) {
    this.header = header;
    boolean trackingKeyRef = keySerializer.needToWriteRef();
    if (!trackingKeyRef) {
      if (keyHasNull()) {
        byte nullFlag = buffer.readByte();
        Preconditions.checkArgument(nullFlag == Fury.NULL_FLAG, "unexpected NULL_FLAG");
        return null;
      } else {
        return keySerializer.read(buffer);
      }
    } else {
      return fury.readRef(buffer, keySerializer);
    }
  }

  public void writeFinalValue(Object value, MemoryBuffer buffer, Serializer valueSerializer) {
    preserveByteForHeaderAndChunkSize(buffer);
    boolean trackingValueRef = valueSerializer.needToWriteRef();
    if (!trackingValueRef) {
      if (value == null) {
        header |= MapFlags.VALUE_HAS_NULL;
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        if (valueHasNull()) {
          buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
          valueSerializer.write(buffer, value);
        } else {
          valueSerializer.write(buffer, value);
        }
      }
    } else {
      updateFinalValueHeader(value, true);
      RefResolver refResolver = fury.getRefResolver();
      if (!refResolver.writeRefOrNull(buffer, value)) {
        valueSerializer.write(buffer, value);
      }
    }
  }

  public Object readFinalValue(MemoryBuffer buffer, int header, Serializer valueSerializer) {
    this.header = header;
    boolean trackingValueRef = valueSerializer.needToWriteRef();
    if (!trackingValueRef) {
      if (valueHasNull()) {
        byte flag = buffer.readByte();
        if (flag == Fury.NOT_NULL_VALUE_FLAG) {
          return valueSerializer.read(buffer);
        } else {
          return null;
        }
      } else {
        return valueSerializer.read(buffer);
      }
    } else {
      return fury.readRef(buffer, valueSerializer);
    }
  }

  private boolean judgeKeyIsNotSameType(Object key) {
    if (key == null) {
      return false;
    }
    if (keyClass == null) {
      keyClass = key.getClass();
    }
    return keyClass != key.getClass();
  }

  private boolean judgeValueIsNotSameType(Object value) {
    if (value == null) {
      return false;
    }
    if (valueClass == null) {
      valueClass = value.getClass();
    }
    return valueClass != value.getClass();
  }

  public void writeKey(
      Object key,
      MemoryBuffer buffer,
      ClassResolver classResolver,
      RefResolver refResolver,
      boolean trackingKeyRef,
      ClassInfoHolder keyClassInfoWriteCache) {
    preserveByteForHeaderAndChunkSize(buffer);
    updateKeyHeader(key, trackingKeyRef, keyIsNotSameType);
    // todo hening key == null提到外面？
    if (!trackingKeyRef) {
      if (key == null) {
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        if (!keyIsNotSameType) {
          writeKeyClass(key, buffer, keyClassInfoWriteCache);
          keyClassInfoWriteCache.getSerializer().write(buffer, key);
        } else {
          fury.writeNonRef(
              buffer, key, classResolver.getClassInfo(key.getClass(), keyClassInfoWriteCache));
        }
      }
    } else {
      // todo 提到外面
      if (key == null) {
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        if (!keyIsNotSameType) {
          // todo key is not null, no need to write no null flag
          writeKeyClass(key, buffer, keyClassInfoWriteCache);
          fury.writeRef(buffer, key, keyClassInfoWriteCache.getSerializer());
        } else {
          if (!refResolver.writeNullFlag(buffer, key)) {
            fury.writeRef(
                buffer, key, classResolver.getClassInfo(key.getClass(), keyClassInfoWriteCache));
          }
        }
      }
    }
  }

  public Object readKey(
      int header,
      MemoryBuffer memoryBuffer,
      ClassResolver classResolver,
      boolean trackingKeyRef,
      ClassInfoHolder keyClassInfoReadCache) {
    this.header = header;
    if (!trackingKeyRef) {
      if (keyHasNull()) {
        byte nullFlag = memoryBuffer.readByte();
        Preconditions.checkArgument(nullFlag == Fury.NULL_FLAG, "unexpected error");
        return null;
      } else {
        if (!keyIsNotSameType()) {
          if (keySerializer == null) {
            keySerializer =
                classResolver.readClassInfo(memoryBuffer, keyClassInfoReadCache).getSerializer();
          }
          return keySerializer.read(memoryBuffer);
        } else {
          return fury.readNonRef(memoryBuffer, keyClassInfoReadCache);
        }
      }
    } else {
      if (keyHasNull()) {
        byte nullFlag = memoryBuffer.readByte();
        Preconditions.checkArgument(nullFlag == Fury.NULL_FLAG, "unexpected error");
        return null;
      } else {
        if (!keyIsNotSameType()) {
          if (keySerializer == null) {
            keySerializer =
                classResolver.readClassInfo(memoryBuffer, keyClassInfoReadCache).getSerializer();
          }
          return fury.readRef(memoryBuffer, keySerializer);
        } else {
          return fury.readRef(memoryBuffer, keyClassInfoReadCache);
        }
      }
    }
  }

  public void writeValue(
      Object value,
      MemoryBuffer buffer,
      ClassResolver classResolver,
      RefResolver refResolver,
      boolean trackingValueRef,
      ClassInfoHolder valueClassInfoWriteCache) {
    preserveByteForHeaderAndChunkSize(buffer);
    updateValueHeader(value, trackingValueRef, valueIsNotSameType);
    if (!trackingValueRef) {
      if (value == null) {
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        if (!valueIsNotSameType) {
          if (!valueHasNull()) {
            writeValueClass(value, buffer, valueClassInfoWriteCache);
            valueClassInfoWriteCache.getSerializer().write(buffer, value);
          } else {
            buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
            writeValueClass(value, buffer, valueClassInfoWriteCache);
            valueClassInfoWriteCache.getSerializer().write(buffer, value);
          }
        } else {
          fury.writeNullable(
              buffer,
              value,
              classResolver.getClassInfo(value.getClass(), valueClassInfoWriteCache));
        }
      }
    } else {
      if (value == null) {
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        if (!valueIsNotSameType) {
          writeValueClass(value, buffer, valueClassInfoWriteCache);
          if (!valueHasNull()) {
            fury.writeRef(buffer, value, valueClassInfoWriteCache.getSerializer());
          } else {
            buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
            fury.writeRef(buffer, value, valueClassInfoWriteCache.getSerializer());
          }
        } else {
          if (!refResolver.writeNullFlag(buffer, value)) {
            fury.writeRef(
                buffer,
                value,
                classResolver.getClassInfo(value.getClass(), valueClassInfoWriteCache));
          }
        }
      }
    }
  }

  public Object readValue(
      int header,
      MemoryBuffer buffer,
      ClassResolver classResolver,
      boolean trackingValueRef,
      ClassInfoHolder valueClassInfoReadCache) {
    this.header = header;
    if (!trackingValueRef) {
      if (!valueIsNotSameType()) {
        if (valueHasNull()) {
          byte flag = buffer.readByte();
          if (flag == Fury.NOT_NULL_VALUE_FLAG) {
            if (valueSerializer == null) {
              valueSerializer =
                  classResolver.readClassInfo(buffer, valueClassInfoReadCache).getSerializer();
            }
            return valueSerializer.read(buffer);
          } else {
            return null;
          }
        } else {
          if (valueSerializer == null) {
            valueSerializer =
                classResolver.readClassInfo(buffer, valueClassInfoReadCache).getSerializer();
          }
          return valueSerializer.read(buffer);
        }
      } else {
        return fury.readNullable(buffer, valueClassInfoReadCache);
      }

    } else {
      if (!valueIsNotSameType()) {
        if (valueHasNull()) {
          byte flag = buffer.readByte();
          if (flag == Fury.NOT_NULL_VALUE_FLAG) {
            if (valueSerializer == null) {
              valueSerializer =
                  classResolver.readClassInfo(buffer, valueClassInfoReadCache).getSerializer();
            }
            return fury.readRef(buffer, valueSerializer);
          } else {
            return null;
          }
        } else {
          if (valueSerializer == null) {
            valueSerializer =
                classResolver.readClassInfo(buffer, valueClassInfoReadCache).getSerializer();
          }
          return fury.readRef(buffer, valueSerializer);
        }
      } else {
        return fury.readRef(buffer, valueClassInfoReadCache);
      }
    }
  }

  private void updateKeyHeader(Object key, boolean trackingKeyRef, boolean keyIsNotSameType) {
    if (key == null) {
      header |= MapFlags.KEY_HAS_NULL;
    } else {
      if (keyIsNotSameType) {
        header |= MapFlags.KEY_NOT_SAME_TYPE;
      }
      if (trackingKeyRef) {
        header |= MapFlags.TRACKING_KEY_REF;
      }
    }
  }

  private void writeKeyClass(
      Object key, MemoryBuffer memoryBuffer, ClassInfoHolder keyClassInfoWriteCache) {
    ClassResolver classResolver = fury.getClassResolver();
    ClassInfo classInfo = classResolver.getClassInfo(key.getClass(), keyClassInfoWriteCache);
    if (!writeKeyClassInfo) {
      classResolver.writeClass(memoryBuffer, classInfo);
      writeKeyClassInfo = true;
    }
  }

  private void writeValueClass(
      Object value, MemoryBuffer memoryBuffer, ClassInfoHolder valueClassInfoWriteCache) {
    ClassResolver classResolver = fury.getClassResolver();
    ClassInfo classInfo = classResolver.getClassInfo(value.getClass(), valueClassInfoWriteCache);
    if (!writeValueClassInfo) {
      classResolver.writeClass(memoryBuffer, classInfo);
      writeValueClassInfo = true;
    }
  }

  private void updateFinalKeyHeader(Object key, boolean trackingKeyRef) {
    if (trackingKeyRef) {
      header |= MapFlags.TRACKING_KEY_REF;
    } else {
      if (key == null) {
        header |= MapFlags.KEY_HAS_NULL;
      }
    }
  }

  private void updateFinalValueHeader(Object value, boolean trackingValueRef) {
    if (trackingValueRef) {
      header |= MapFlags.TRACKING_VALUE_REF;
    } else {
      if (value == null) {
        header |= MapFlags.VALUE_HAS_NULL;
      }
    }
  }

  private void updateValueHeader(
      Object value, boolean trackingValueRef, boolean valueIsNotSameType) {
    if (value == null) {
      header |= MapFlags.VALUE_HAS_NULL;
    } else {
      if (valueIsNotSameType) {
        header |= MapFlags.VALUE_NOT_SAME_TYPE;
      }
      if (trackingValueRef) {
        header |= MapFlags.TRACKING_VALUE_REF;
      }
    }
  }

  /**
   * update chunk size and header, if chunk size == 0, do nothing
   *
   * @param memoryBuffer memoryBuffer which is written
   */
  public void writeHeader(MemoryBuffer memoryBuffer) {
    if (chunkSize > 0) {
      int currentWriteIndex = memoryBuffer.writerIndex();
      memoryBuffer.writerIndex(startOffset);
      memoryBuffer.writeByte(chunkSize);
      memoryBuffer.writeByte(header);
      memoryBuffer.writerIndex(currentWriteIndex);
      chunkSize = 0;
    }
  }

  /**
   * use chunk size = 0 to mark chunk write finish, if mark chunk write finish which means predict
   * failed, chunk write is finish, rest of map will be written by generalJavaWrite
   *
   * @param memoryBuffer memoryBuffer which is written
   */
  public void markChunkWriteFinish(MemoryBuffer memoryBuffer) {
    if (!markChunkWriteFinish) {
      writeHeader(memoryBuffer);
      // set chunk size = 0
      memoryBuffer.writeByte(0);
      markChunkWriteFinish = true;
    }
  }

  /**
   * chunk size reach max size, start new chunk, no need reset keyClass and value Class
   *
   * @param memoryBuffer memoryBuffer which is written
   */
  public void reset(MemoryBuffer memoryBuffer) {
    writeHeader(memoryBuffer);
    header = 0;
    chunkSize = 0;
    hasPreservedByte = false;
    writeKeyClassInfo = false;
    writeValueClassInfo = false;
    prevKeyIsNull = false;
    keyClass = null;
    valueClass = null;
    keySerializer = null;
    valueSerializer = null;
  }

  private boolean keyHasNull() {
    return (header & MapFlags.KEY_HAS_NULL) == MapFlags.KEY_HAS_NULL;
  }

  private boolean valueHasNull() {
    return (header & MapFlags.VALUE_HAS_NULL) == MapFlags.VALUE_HAS_NULL;
  }

  private boolean valueIsNotSameType() {
    return (header & MapFlags.VALUE_NOT_SAME_TYPE) == MapFlags.VALUE_NOT_SAME_TYPE;
  }

  private boolean keyIsNotSameType() {
    return (header & MapFlags.KEY_NOT_SAME_TYPE) == MapFlags.KEY_NOT_SAME_TYPE;
  }

  public boolean isMarkChunkWriteFinish() {
    return markChunkWriteFinish;
  }

  public void setKeySerializer(Serializer keySerializer) {
    this.keySerializer = keySerializer;
  }

  public void setValueSerializer(Serializer valueSerializer) {
    this.valueSerializer = valueSerializer;
  }
}
