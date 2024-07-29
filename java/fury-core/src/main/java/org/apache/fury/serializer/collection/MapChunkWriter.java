package org.apache.fury.serializer.collection;

import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.resolver.ClassInfo;
import org.apache.fury.resolver.ClassInfoHolder;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.RefResolver;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.util.Preconditions;

import java.util.Map;

/**
 * todo 1 value如果有空的情况，value可能有多个空，写class信息，在哪里写，2 写class当前可能写重复了
 */
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

    public MapChunkWriter next(Object key, Object value, MemoryBuffer buffer) {
        if (!markChunkWriteFinish) {
            if (key == null && chunkSize > 0) {
                prevKeyIsNull = true;
                reset(buffer);
            }
            if (prevKeyIsNull && key != null) {
                reset(buffer);
            }
            if (value == null && chunkSize > 0 && !valueHasNull()) {
                //if value has null before, no need to reset chunk
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



    public void generalChunkWrite(Object key, Object value, MemoryBuffer memoryBuffer, ClassResolver classResolver, RefResolver refResolver, ClassInfoHolder keyClassInfoWriteCache, ClassInfoHolder valueClassInfoWriteCache) {
        final boolean trackingRef = fury.trackingRef();
        writeKey(key, memoryBuffer, classResolver, refResolver, trackingRef, keyClassInfoWriteCache);
        writeValue(value, memoryBuffer, classResolver, refResolver, trackingRef, valueClassInfoWriteCache);
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

    public void writeKey(Object key, MemoryBuffer buffer, ClassResolver classResolver, RefResolver refResolver, boolean trackingKeyRef, ClassInfoHolder keyClassInfoWriteCache) {
        preserveByteForHeaderAndChunkSize(buffer);
        //todo hening key == null提到外面？
        if (!trackingKeyRef) {
            if (key == null) {
                updateKeyHeader(null, buffer, keyClassInfoWriteCache, false, false);
                buffer.writeByte(Fury.NULL_FLAG);
            } else {
                if (!keyIsNotSameType) {
                    updateKeyHeader(key, buffer, keyClassInfoWriteCache, false, false);
                    keyClassInfoWriteCache.getSerializer().write(buffer, key);
                } else {
                    updateKeyHeader(key, buffer, keyClassInfoWriteCache, false, true);
                    fury.writeNonRef(buffer, key, classResolver.getClassInfo(key.getClass(), keyClassInfoWriteCache));
                }
            }
        } else {
            //todo 提到外面
            if (key == null) {
                updateKeyHeader(null, buffer, keyClassInfoWriteCache, true, false);
                buffer.writeByte(Fury.NULL_FLAG);
            } else {
                if (!keyIsNotSameType) {
                    updateKeyHeader(key, buffer, keyClassInfoWriteCache, true, false);
                    //todo key is not null, no need to write no null flag
                    fury.writeRef(buffer, key, keyClassInfoWriteCache.getSerializer());
                } else {
                    // todo hening remove write class
                    updateKeyHeader(key, buffer, keyClassInfoWriteCache, true, true);
                    if (!refResolver.writeNullFlag(buffer, key)) {
                        fury.writeRef(buffer, key, classResolver.getClassInfo(key.getClass(), keyClassInfoWriteCache));
                    }
                }
            }
        }

    }

    public Object readKey(int header, MemoryBuffer memoryBuffer, ClassResolver classResolver, boolean trackingKeyRef, ClassInfoHolder keyClassInfoReadCache) {
        this.header = header;
        if (!trackingKeyRef) {
            if (keyHasNull()) {
                byte nullFlag = memoryBuffer.readByte();
                Preconditions.checkArgument(nullFlag != Fury.NULL_FLAG, "unexpected error");
                return null;
            } else {
                if (!keyIsNotSameType()) {
                    if (keyClassInfoReadCache.getSerializer() == null) {
                        classResolver.readClassInfo(memoryBuffer, keyClassInfoReadCache);
                    }
                    return keyClassInfoReadCache.getSerializer().read(memoryBuffer);
                } else {
                    return fury.readNonRef(memoryBuffer, keyClassInfoReadCache);
                }
            }
        } else {
            if (keyHasNull()) {
                byte nullFlag = memoryBuffer.readByte();
                Preconditions.checkArgument(nullFlag != Fury.NULL_FLAG, "unexpected error");
                return null;
            } else {
                if (!keyIsNotSameType()) {
                    if (keyClassInfoReadCache.getSerializer() == null) {
                        classResolver.readClassInfo(memoryBuffer, keyClassInfoReadCache);
                    }
                    return fury.readRef(memoryBuffer, keyClassInfoReadCache.getSerializer());
                } else {
                    return fury.readRef(memoryBuffer, keyClassInfoReadCache);
                }
            }
        }
    }


    public void writeValue(Object value, MemoryBuffer buffer, ClassResolver classResolver, RefResolver refResolver, boolean trackingValueRef, ClassInfoHolder valueClassInfoWriteCache) {
        preserveByteForHeaderAndChunkSize(buffer);
        if (!trackingValueRef) {
            if (value == null) {
                updateValueHeader(null, buffer, valueClassInfoWriteCache, false, false);
                buffer.writeByte(Fury.NULL_FLAG);
            } else {
                updateValueHeader(value, buffer, valueClassInfoWriteCache, false, valueIsNotSameType);
                if (!valueIsNotSameType) {
                    writeValueClass(value, buffer, valueClassInfoWriteCache);
                    if (!valueHasNull()) {
                        valueClassInfoWriteCache.getSerializer().write(buffer, value);
                    } else {
                        buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
                        valueClassInfoWriteCache.getSerializer().write(buffer, value);
                    }
                } else {
                    fury.writeNonRef(buffer, value, classResolver.getClassInfo(value.getClass(), valueClassInfoWriteCache));
                }
            }
        } else {
            if (value == null) {
                updateValueHeader(null, buffer, valueClassInfoWriteCache, true, false);
                buffer.writeByte(Fury.NULL_FLAG);
            } else {
                updateKeyHeader(value, buffer, valueClassInfoWriteCache, true, valueIsNotSameType);
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
                        fury.writeRef(buffer, value, classResolver.getClassInfo(value.getClass(), valueClassInfoWriteCache));
                    }
                }
            }
        }

    }

    public Object readValue(int header, MemoryBuffer buffer, ClassResolver classResolver, boolean trackingValueRef, ClassInfoHolder valueClassInfoReadCache) {
        this.header = header;
        if (!trackingValueRef) {
            if (valueHasNull()) {
                byte flag = buffer.readByte();
                if (flag == Fury.NOT_NULL_VALUE_FLAG) {
                    if (valueClassInfoReadCache.getSerializer() == null) {
                        classResolver.readClassInfo(buffer, valueClassInfoReadCache);
                    }
                    return valueClassInfoReadCache.getSerializer().read(buffer);
                } else {
                    return null;
                }
            } else {
                return valueClassInfoReadCache.getSerializer().read(buffer);
            }
        } else {
            if (valueClassInfoReadCache.getSerializer() == null) {
                classResolver.readClassInfo(buffer, valueClassInfoReadCache);
            }
            if (!valueIsNotSameType) {
                if (valueHasNull()) {
                    byte flag = buffer.readByte();
                    if (flag == Fury.NOT_NULL_VALUE_FLAG) {
                        if (valueClassInfoReadCache.getSerializer() == null) {
                            classResolver.readClassInfo(buffer, valueClassInfoReadCache);
                        }
                        return fury.readRef(buffer, valueClassInfoReadCache.getSerializer());
                    } else {
                        return null;
                    }
                } else {
                    return fury.readRef(buffer, valueClassInfoReadCache.getSerializer());
                }
            } else {
                return fury.readRef(buffer, valueClassInfoReadCache);
            }
        }
    }


//    private void writeValueNonNull(Object value, MemoryBuffer buffer, ClassResolver classResolver, RefResolver refResolver, ClassInfoHolder valueClassInfoWriteCache, boolean trackingValueRef) {
//        updateValueHeader(value, buffer, valueClassInfoWriteCache, trackingValueRef);
//        if (!trackingValueRef) {
//            if (!valueNotSameType()) {
//                Serializer valueSerializer = valueClassInfoWriteCache.getSerializer();
//                if (valueHasNull()) {
//                    buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
//                    valueSerializer.write(buffer, value);
//                } else {
//                    valueSerializer.write(buffer, value);
//                }
//            } else {
//                if (keyIsNotSameType()) {
//                    markChunkWriteFinish(buffer);
//                }
//                writeJavaRefOptimized(fury, classResolver, refResolver, false, buffer, value, valueClassInfoWriteCache);
//            }
//        } else {
//            if (!valueNotSameType()) {
//                if (!refResolver.writeRefOrNull(buffer, value)) {
//                    valueClassInfoWriteCache.getSerializer().write(buffer, value);
//                }
//            } else {
//                if (keyIsNotSameType()) {
//                    markChunkWriteFinish(buffer);
//                }
//                writeJavaRefOptimized(fury, classResolver, refResolver, true, buffer, value, valueClassInfoWriteCache);
//            }
//        }
//    }


//    public void writeKey(Object key, MemoryBuffer buffer, ClassResolver classResolver, RefResolver refResolver, ClassInfoHolder keyClassInfoWriteCache) {
//        preserveByteForHeaderAndChunkSize(buffer);
//        final boolean trackingRef = fury.trackingRef();
//        if (!trackingRef) {
//            if (key == null) {
//                header |= MapFlags.KEY_HAS_NULL;
//                buffer.writeByte(Fury.NULL_FLAG);
//            } else {
//                updateKeyHeader(key, buffer, keyClassInfoWriteCache, false, keyIsNotSameType);
//                if (!keyIsNotSameType) {
//                    keyClassInfoWriteCache.getSerializer().write(buffer, key);
//                } else {
//                    fury.writeNonRef(buffer, key, classResolver.getClassInfo(key.getClass(), keyClassInfoWriteCache));
//                }
//            }
//        } else {
//            if (key == null) {
//                //todo remove writeClass
//                updateKeyHeader(null, buffer, keyClassInfoWriteCache, true, false);
//                buffer.writeByte(Fury.NULL_FLAG);
//            } else {
//                if (!keyIsNotSameType) {
//                    ClassInfo classInfo = classResolver.getClassInfo(key.getClass(), keyClassInfoWriteCache);
//                    boolean trackingKeyRef = classInfo.getSerializer().needToWriteRef();
//                    updateKeyHeader(key, buffer, keyClassInfoWriteCache, trackingKeyRef, false);
//                    fury.writeRef(buffer, key, keyClassInfoWriteCache.getSerializer());
//                } else {
//                    writeJavaRefOptimized(fury, classResolver, refResolver, buffer, key, keyClassInfoWriteCache);
//                }
//            }
//        }
//    }
//
//
//    public void writeValue(Object value
//            , MemoryBuffer buffer
//            , ClassResolver classResolver
//            , RefResolver refResolver
//            , ClassInfoHolder valueClassInfoWriteCache) {
//        preserveByteForHeaderAndChunkSize(buffer);
//        boolean trackingRef = fury.trackingRef();
//        if (!trackingRef) {
//            if (value == null) {
//                header |= MapFlags.VALUE_HAS_NULL;
//                buffer.writeByte(Fury.NULL_FLAG);
//            } else {
//                updateValueHeader(value, buffer, valueClassInfoWriteCache, false, valueIsNotSameType);
//                if (!valueIsNotSameType) {
//                    if (!valueHasNull()) {
//                        valueClassInfoWriteCache.getSerializer().write(buffer, value);
//                    } else {
//                        buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
//                        valueClassInfoWriteCache.getSerializer().write(buffer, value);
//                    }
//                } else {
//                    fury.writeNonRef(buffer, value, classResolver.getClassInfo(value.getClass(), valueClassInfoWriteCache));
//                }
//            }
//        } else {
//            ClassInfo classInfo = classResolver.getClassInfo(value.getClass(), valueClassInfoWriteCache);
//            boolean trackingValueRef = classInfo.getSerializer().needToWriteRef();
//            writeValueNonNull(value, buffer, classResolver, refResolver, valueClassInfoWriteCache, trackingValueRef);
//        }
//
//    }

    private void updateKeyHeader(Object key, MemoryBuffer memoryBuffer, ClassInfoHolder keyClassInfoWriteCache, boolean trackingKeyRef, boolean keyIsNotSameType) {
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

    private void writeKeyClass(Object key, MemoryBuffer memoryBuffer, ClassInfoHolder keyClassInfoWriteCache) {
        ClassResolver classResolver = fury.getClassResolver();
        ClassInfo classInfo = classResolver.getClassInfo(key.getClass(), keyClassInfoWriteCache);
        if (!writeKeyClassInfo) {
            classResolver.writeClass(memoryBuffer, classInfo);
            writeKeyClassInfo = true;
        }
    }

    private void writeValueClass(Object value, MemoryBuffer memoryBuffer, ClassInfoHolder valueClassInfoWriteCache) {
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

    private void updateValueHeader(Object value, MemoryBuffer memoryBuffer, ClassInfoHolder valueClassInfoWriteCache, boolean trackingValueRef, boolean valueIsNotSameType) {
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
        writeKeyClassInfo = false;
        writeValueClassInfo = false;
        prevKeyIsNull = false;
        keyClass = null;
        valueClass = null;
    }

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

    public boolean isMarkChunkWriteFinish() {
        return markChunkWriteFinish;
    }



}
