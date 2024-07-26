package org.apache.fury.serializer.collection;

import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.resolver.ClassInfo;
import org.apache.fury.resolver.ClassInfoHolder;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.RefResolver;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.type.GenericType;
import org.apache.fury.util.Preconditions;

import java.util.Map;

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

    /**
     * mark chunk write finish
     */
    private boolean markChunkWriteFinish = false;
    /**
     * preserve two byte for header and chunk size and record the write index
     * so that we can write key value at first, write header and chunk size when the chunk is finish at correct position
     */
    private boolean preserveByteForHeaderAndChunkSize = false;

    private void preserveByteForHeaderAndChunkSize(MemoryBuffer memoryBuffer) {
        if (!preserveByteForHeaderAndChunkSize) {
            return;
        }
        int writerIndex = memoryBuffer.writerIndex();
        // preserve two byte for header and chunk size
        memoryBuffer.writerIndex(writerIndex + 2);
        this.startOffset = writerIndex;
        preserveByteForHeaderAndChunkSize = true;
    }

    public void increaseChunkSize() {
        chunkSize++;
    }

    public void resetIfNeed(Object key, MemoryBuffer memoryBuffer) {
        // if chunkSize reach max chunk size or key is null, start a new chunk
        if (chunkSize >= MAX_CHUNK_SIZE || key == null) {
            reset(memoryBuffer);
        }
    }


    public void generalChunkWrite(Object key, Object value, MemoryBuffer memoryBuffer, ClassResolver classResolver, RefResolver refResolver, ClassInfoHolder keyClassInfoWriteCache, ClassInfoHolder valueClassInfoWriteCache) {
        if (!markChunkWriteFinish) {
            writeKey(key, memoryBuffer, classResolver, refResolver, keyClassInfoWriteCache);
            writeValue(value, memoryBuffer, classResolver, refResolver, valueClassInfoWriteCache);
            increaseChunkSize();
            resetIfNeed(key, memoryBuffer);
        }
        writeJavaRefOptimized(fury, classResolver, refResolver, memoryBuffer, key, keyClassInfoWriteCache);
        writeJavaRefOptimized(fury, classResolver, refResolver, memoryBuffer, value, valueClassInfoWriteCache);
    }

    public void generalChunkRead(MemoryBuffer memoryBuffer, ClassResolver classResolver, RefResolver refResolver, Map map, int size, ClassInfoHolder keyClassInfoReadCache, ClassInfoHolder valueClassInfoReadCache) {
        while(size > 0) {
            Object key;
            Object value;
            if (!markChunkWriteFinish) {
                byte headerSize = memoryBuffer.readByte();
                Preconditions.checkArgument(headerSize >= 0, "unexpected header size");
                if (headerSize == 0) {
                    key = fury.readRef(memoryBuffer, keyClassInfoReadCache);
                    value = fury.readRef(memoryBuffer, valueClassInfoReadCache);
                    markChunkWriteFinish = true;
                } else {
                    this.header = memoryBuffer.readByte();
                    Serializer keySerializer = null;
                    Serializer valueSerializer = null;
                    while (headerSize > 0) {
                        if (keyHasNull()) {
                            byte nullFlag = memoryBuffer.readByte();
                            Preconditions.checkArgument(nullFlag == Fury.NULL_FLAG, "unexpected NULL_FLAG");
                            key = null;
                        } else {
                            if (keySerializer == null) {
                                keySerializer = classResolver.readClassInfo(memoryBuffer, keyClassInfoReadCache).getSerializer();
                            }
                            boolean trackingKeyRef = keySerializer.needToWriteRef();
                            if (!trackingKeyRef) {
                                if (!keyIsNotSameType()) {
                                    key = keySerializer.read(memoryBuffer);
                                } else {
                                    key = fury.readNonRef(memoryBuffer, keyClassInfoReadCache);;
                                }
                            } else {
                                if (!keyIsNotSameType()) {
                                    key = keySerializer.read(memoryBuffer);
                                } else {
                                    key = fury.readRef(memoryBuffer, keyClassInfoReadCache);
                                }
                            }
                        }
                        if (valueHasNull()) {

                        } else {

                        }
                        headerSize--;
                    }
                }
            } else {
                key = fury.readRef(memoryBuffer, keyClassInfoReadCache);
                value = fury.readRef(memoryBuffer, valueClassInfoReadCache);
            }
            map.put(key, value);
            size--;
        }

        Object key = fury.readRef(memoryBuffer, keyClassInfoReadCache);
        Object value = fury.readRef(memoryBuffer, valueClassInfoReadCache);
        map.put(key, value);
    }

    public void writeFinalKey(Object key, MemoryBuffer buffer, Serializer keySerializer) {
        preserveByteForHeaderAndChunkSize(buffer);
        boolean trackingKeyRef = keySerializer.needToWriteRef();
        if (!trackingKeyRef) {
            // map key has one null at most, use one chunk to write
            if (key == null) {
                if (chunkSize > 0) {
                    reset(buffer);
                }
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
        if ((header & MapFlags.KEY_HAS_NULL) == MapFlags.KEY_HAS_NULL) {
            byte nullFlag = buffer.readByte();
            Preconditions.checkArgument(nullFlag == Fury.NULL_FLAG, "unexpected NULL_FLAG");
            return null;
        } else {
            boolean trackingKeyRef = keySerializer.needToWriteRef();
            if (trackingKeyRef) {
                return fury.readRef(buffer, keySerializer);
            } else {
                return keySerializer.read(buffer);
            }
        }
    }

    public void writeFinalValue(Object value, MemoryBuffer buffer, Serializer valueSerializer) {
        preserveByteForHeaderAndChunkSize(buffer);
        boolean trackingValueRef = valueSerializer.needToWriteRef();
        if (!trackingValueRef) {
            if (value == null) {
                //if value has null before, no need to reset chunk
                if (chunkSize > 0 && !valueHasNull()) {
                    reset(buffer);
                }
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
        if ((header & MapFlags.VALUE_HAS_NULL) == MapFlags.VALUE_HAS_NULL) {
            byte nullFlag = buffer.readByte();
            if (nullFlag == Fury.NULL_FLAG) {
                return null;
            } else {
                return valueSerializer.read(buffer);
            }
        } else {
            boolean trackingValueRef = valueSerializer.needToWriteRef();
            if (trackingValueRef) {
                return fury.readRef(buffer, valueSerializer);
            } else {
                return valueSerializer.read(buffer);
            }
        }
    }

    public void writeKeyWithGenericType(Object key, MemoryBuffer buffer, ClassResolver classResolver, RefResolver refResolver, GenericType keyGenericType, ClassInfoHolder keyClassInfoWriteCache) {
        boolean trackingKeyRef = fury.getClassResolver().needToWriteRef(keyGenericType.getCls());
        preserveByteForHeaderAndChunkSize(buffer);
        if (key == null) {
            if ((header & MapFlags.KEY_HAS_NULL) != MapFlags.KEY_HAS_NULL) {
                reset(buffer);
            }
            updateKeyHeader(null, buffer, keyClassInfoWriteCache, trackingKeyRef);
            buffer.writeByte(Fury.NULL_FLAG);
        } else {
            writeKeyNonNull(key, buffer, classResolver, refResolver, keyClassInfoWriteCache, trackingKeyRef);
        }
    }

    private void writeKeyNonNull(Object key, MemoryBuffer buffer, ClassResolver classResolver, RefResolver refResolver, ClassInfoHolder keyClassInfoWriteCache, boolean trackingKeyRef) {
        updateKeyHeader(key, buffer, keyClassInfoWriteCache, trackingKeyRef);
        if (!trackingKeyRef) {
            if (!keyIsNotSameType()) {
                keyClassInfoWriteCache.getSerializer().write(buffer, key);
            } else {
                if (valueNotSameType()) {
                    markChunkWriteFinish(buffer);
                }
                writeJavaRefOptimized(fury, classResolver, refResolver, true, buffer, key, keyClassInfoWriteCache);
            }
        } else {
            if (!keyIsNotSameType()) {
                if (!refResolver.writeRefOrNull(buffer, key)) {
                    keyClassInfoWriteCache.getSerializer().write(buffer, key);
                }
            } else {
                if (valueNotSameType()) {
                    markChunkWriteFinish(buffer);
                }
                writeJavaRefOptimized(fury, classResolver, refResolver, true, buffer, key, keyClassInfoWriteCache);
            }
        }

    }

    public void writeValueWithGenericType(Object value, MemoryBuffer buffer, ClassResolver classResolver, RefResolver refResolver, GenericType valueGenericType, ClassInfoHolder valueClassInfoWriteCache) {
        boolean trackingValueRef = fury.getClassResolver().needToWriteRef(valueGenericType.getCls());
        preserveByteForHeaderAndChunkSize(buffer);
        if (value == null) {
            if (chunkSize > 0 && (header & MapFlags.VALUE_HAS_NULL) != MapFlags.VALUE_HAS_NULL) {
                reset(buffer);
            }
            updateValueHeader(null, buffer, valueClassInfoWriteCache, trackingValueRef);
            buffer.writeByte(Fury.NULL_FLAG);
        } else {
            writeValueNonNull(value, buffer, classResolver, refResolver, valueClassInfoWriteCache, trackingValueRef);
        }
    }


    private void writeValueNonNull(Object value, MemoryBuffer buffer, ClassResolver classResolver, RefResolver refResolver, ClassInfoHolder valueClassInfoWriteCache, boolean trackingValueRef) {
        updateValueHeader(value, buffer, valueClassInfoWriteCache, trackingValueRef);
        if (!trackingValueRef) {
            if (!valueNotSameType()) {
                Serializer valueSerializer = valueClassInfoWriteCache.getSerializer();
                if (valueHasNull()) {
                    buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
                    valueSerializer.write(buffer, value);
                } else {
                    valueSerializer.write(buffer, value);
                }
            } else {
                if (keyIsNotSameType()) {
                    markChunkWriteFinish(buffer);
                }
                writeJavaRefOptimized(fury, classResolver, refResolver, false, buffer, value, valueClassInfoWriteCache);
            }
        } else {
            if (!valueNotSameType()) {
                if (!refResolver.writeRefOrNull(buffer, value)) {
                    valueClassInfoWriteCache.getSerializer().write(buffer, value);
                }
            } else {
                if (keyIsNotSameType()) {
                    markChunkWriteFinish(buffer);
                }
                writeJavaRefOptimized(fury, classResolver, refResolver, true, buffer, value, valueClassInfoWriteCache);
            }
        }
    }


    public void writeKey(Object key, MemoryBuffer buffer, ClassResolver classResolver, RefResolver refResolver, ClassInfoHolder keyClassInfoWriteCache) {
        preserveByteForHeaderAndChunkSize(buffer);
        if (key == null) {
            //If chunk size > 0 means there are non null keys in the chunk,
            // then the chunk needs to be reset to store the null.
            // Otherwise, it means encountering null for the first time and not performing a reset operation
            if (chunkSize > 0) {
                reset(buffer);
            }
            header |= MapFlags.KEY_HAS_NULL;
            buffer.writeByte(Fury.NULL_FLAG);
        } else {
            ClassInfo classInfo = classResolver.getClassInfo(key.getClass(), keyClassInfoWriteCache);
            boolean trackingKeyRef = classInfo.getSerializer().needToWriteRef();
            writeKeyNonNull(key, buffer, classResolver, refResolver, keyClassInfoWriteCache, trackingKeyRef);
        }
    }


    public void writeValue(Object value
            , MemoryBuffer buffer
            , ClassResolver classResolver
            , RefResolver refResolver
            , ClassInfoHolder valueClassInfoWriteCache) {
        preserveByteForHeaderAndChunkSize(buffer);
        if (value == null) {
            if (chunkSize > 0 && !valueHasNull()) {
                reset(buffer);
            }
            header |= MapFlags.VALUE_HAS_NULL;
            buffer.writeByte(Fury.NULL_FLAG);
        } else {
            ClassInfo classInfo = classResolver.getClassInfo(value.getClass(), valueClassInfoWriteCache);
            boolean trackingValueRef = classInfo.getSerializer().needToWriteRef();
            writeValueNonNull(value, buffer, classResolver, refResolver, valueClassInfoWriteCache, trackingValueRef);
        }
    }

    private void updateKeyHeader(Object key, MemoryBuffer memoryBuffer, ClassInfoHolder keyClassInfoWriteCache, boolean trackingKeyRef) {
        if (key == null) {
            header |= MapFlags.KEY_HAS_NULL;
        } else {
            if (keyClass == null) {
                keyClass = key.getClass();
            } else if (keyClass != key.getClass()) {
                header |= MapFlags.KEY_NOT_SAME_TYPE;
            }
            if (trackingKeyRef) {
                header |= MapFlags.TRACKING_KEY_REF;
            }
            ClassResolver classResolver = fury.getClassResolver();
            ClassInfo classInfo = classResolver.getClassInfo(key.getClass(), keyClassInfoWriteCache);
            if (!writeKeyClassInfo) {
                classResolver.writeClass(memoryBuffer, classInfo);
                writeKeyClassInfo = true;
            }
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

    private void updateValueHeader(Object value, MemoryBuffer memoryBuffer, ClassInfoHolder valueClassInfoWriteCache, boolean trackingValueRef) {
        if (value == null) {
            header |= MapFlags.VALUE_HAS_NULL;
        } else {
            if (valueClass == null) {
                valueClass = value.getClass();
            } else if (valueClass != value.getClass()) {
                header |= MapFlags.VALUE_NOT_SAME_TYPE;
            }
            if (trackingValueRef) {
                header |= MapFlags.TRACKING_VALUE_REF;
            }
            ClassResolver classResolver = fury.getClassResolver();
            ClassInfo classInfo = classResolver.getClassInfo(value.getClass(), valueClassInfoWriteCache);
            if (!writeValueClassInfo) {
                classResolver.writeClass(memoryBuffer, classInfo);
                writeValueClassInfo = true;
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
     * use chunk size = 0 to mark chunk write finish,
     * if mark chunk write finish which means predict failed, chunk write is finish,
     * rest of map will be written by generalJavaWrite
     *
     * @param memoryBuffer memoryBuffer which is written
     */
    public void markChunkWriteFinish(MemoryBuffer memoryBuffer) {
        if (!markChunkWriteFinish) {
            writeHeader(memoryBuffer);
            //set chunk size = 0
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
        preserveByteForHeaderAndChunkSize = false;
    }

    //todo 这两个方法的区别是什么
    private void writeJavaRefOptimized(
            Fury fury,
            ClassResolver classResolver,
            RefResolver refResolver,
            boolean trackingRef,
            MemoryBuffer buffer,
            Object obj,
            ClassInfoHolder classInfoHolder) {
        if (trackingRef) {
            if (!refResolver.writeNullFlag(buffer, obj)) {
                fury.writeRef(buffer, obj, classResolver.getClassInfo(obj.getClass(), classInfoHolder));
            }
        } else {
            if (obj == null) {
                buffer.writeByte(Fury.NULL_FLAG);
            } else {
                buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
                fury.writeNonRef(buffer, obj, classResolver.getClassInfo(obj.getClass(), classInfoHolder));
            }
        }
    }

    private void writeJavaRefOptimized(
            Fury fury,
            ClassResolver classResolver,
            RefResolver refResolver,
            MemoryBuffer buffer,
            Object obj,
            ClassInfoHolder classInfoHolder) {
        if (!refResolver.writeNullFlag(buffer, obj)) {
            fury.writeRef(buffer, obj, classResolver.getClassInfo(obj.getClass(), classInfoHolder));
        }
    }

    private Object readJavaRefOptimized(
            Fury fury,
            RefResolver refResolver,
            boolean trackingRef,
            MemoryBuffer buffer,
            ClassInfoHolder classInfoHolder) {
        if (trackingRef) {
            int nextReadRefId = refResolver.tryPreserveRefId(buffer);
            if (nextReadRefId >= Fury.NOT_NULL_VALUE_FLAG) {
                Object obj = fury.readNonRef(buffer, classInfoHolder);
                refResolver.setReadObject(nextReadRefId, obj);
                return obj;
            } else {
                return refResolver.getReadObject();
            }
        } else {
            byte headFlag = buffer.readByte();
            if (headFlag == Fury.NULL_FLAG) {
                return null;
            } else {
                return fury.readNonRef(buffer, classInfoHolder);
            }
        }
    }


    private boolean keyHasNull() {
        return (header & MapFlags.KEY_HAS_NULL) == MapFlags.KEY_HAS_NULL;
    }

    private boolean valueHasNull() {
        return (header & MapFlags.VALUE_HAS_NULL) == MapFlags.VALUE_HAS_NULL;
    }

    private boolean valueNotSameType() {
        return (header & MapFlags.VALUE_NOT_SAME_TYPE) == MapFlags.VALUE_NOT_SAME_TYPE;
    }

    private boolean keyIsNotSameType() {
        return (header & MapFlags.KEY_NOT_SAME_TYPE) == MapFlags.KEY_NOT_SAME_TYPE;
    }

}
