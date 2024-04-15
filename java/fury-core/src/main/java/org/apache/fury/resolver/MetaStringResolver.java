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

package org.apache.fury.resolver;

import java.nio.charset.StandardCharsets;
import org.apache.fury.collection.LongMap;
import org.apache.fury.collection.ObjectMap;
import org.apache.fury.memory.MemoryBuffer;

/**
 * A resolver for limited string value writing. Currently, we only support classname dynamic
 * writing. In the future, we may profile string field value dynamically and writing by this
 * resolver to reduce string cost. TODO add common inner package names and classnames here. TODO
 * share common immutable datastructure globally across multiple fury.
 */
public final class MetaStringResolver {
  public static final byte USE_STRING_VALUE = 0;
  public static final byte USE_STRING_ID = 1;
  private static final int initialCapacity = 8;
  // use a lower load factor to minimize hash collision
  private static final float furyMapLoadFactor = 0.25f;

  // Every deserialization for unregistered string will query it, performance is important.
  private final ObjectMap<MetaStringBytes, String> enumStringBytes2StringMap =
      new ObjectMap<>(initialCapacity, furyMapLoadFactor);
  private final LongMap<MetaStringBytes> hash2MetaStringBytesMap =
      new LongMap<>(initialCapacity, furyMapLoadFactor);
  // Every enum bytes should be singleton at every fury, since we keep state in it.
  private final ObjectMap<String, MetaStringBytes> enumString2BytesMap =
      new ObjectMap<>(initialCapacity, furyMapLoadFactor);
  private MetaStringBytes[] dynamicWrittenString = new MetaStringBytes[32];
  private MetaStringBytes[] dynamicReadStringIds = new MetaStringBytes[32];
  private short dynamicWriteStringId;
  private short dynamicReadStringId;

  public MetaStringResolver() {
    dynamicWriteStringId = 0;
    dynamicReadStringId = 0;
  }

  MetaStringBytes getOrCreateMetaStringBytes(String str) {
    MetaStringBytes metaStringBytes = enumString2BytesMap.get(str);
    if (metaStringBytes == null) {
      metaStringBytes = new MetaStringBytes(str);
      enumString2BytesMap.put(str, metaStringBytes);
    }
    return metaStringBytes;
  }

  public void writeMetaString(MemoryBuffer buffer, String str) {
    writeMetaStringBytes(buffer, getOrCreateMetaStringBytes(str));
  }

  public String readMetaString(MemoryBuffer buffer) {
    MetaStringBytes byteString = readMetaStringBytes(buffer);
    String str = enumStringBytes2StringMap.get(byteString);
    if (str == null) { // TODO use org.apache.fury.resolver.ObjectMap
      str = new String(byteString.bytes, StandardCharsets.UTF_8);
      enumStringBytes2StringMap.put(byteString, str);
    }
    return str;
  }

  public void writeMetaStringBytes(MemoryBuffer buffer, MetaStringBytes byteString) {
    short id = byteString.dynamicWriteStringId;
    int writerIndex = buffer.writerIndex();
    if (id == MetaStringBytes.DEFAULT_DYNAMIC_WRITE_STRING_ID) {
      id = dynamicWriteStringId++;
      byteString.dynamicWriteStringId = id;
      MetaStringBytes[] dynamicWrittenMetaString = this.dynamicWrittenString;
      if (dynamicWrittenMetaString.length <= id) {
        MetaStringBytes[] tmp = new MetaStringBytes[id * 2];
        System.arraycopy(dynamicWrittenMetaString, 0, tmp, 0, dynamicWrittenMetaString.length);
        dynamicWrittenMetaString = tmp;
        this.dynamicWrittenString = tmp;
      }
      dynamicWrittenMetaString[id] = byteString;
      int bytesLen = byteString.bytes.length;
      buffer.increaseWriterIndex(11 + bytesLen);
      buffer._unsafePutByte(writerIndex, USE_STRING_VALUE);
      // Since duplicate enum string writing are avoided by dynamic id,
      // use 8-byte hash won't increase too much space.
      buffer._unsafePutInt64(writerIndex + 1, byteString.hashCode);
      buffer._unsafePutInt16(writerIndex + 9, (short) bytesLen);
      buffer.put(writerIndex + 11, byteString.bytes, 0, bytesLen);
    } else {
      buffer.increaseWriterIndex(3);
      buffer._unsafePutByte(writerIndex, USE_STRING_ID);
      buffer._unsafePutInt16(writerIndex + 1, id);
    }
  }

  MetaStringBytes readMetaStringBytes(MemoryBuffer buffer) {
    if (buffer.readByte() == USE_STRING_VALUE) {
      long hashCode = buffer.readInt64();
      MetaStringBytes byteString = trySkipMetaStringBytes(buffer, hashCode);
      updateDynamicString(byteString);
      return byteString;
    } else {
      return dynamicReadStringIds[buffer.readInt16()];
    }
  }

  MetaStringBytes readMetaStringBytes(MemoryBuffer buffer, MetaStringBytes cache) {
    if (buffer.readByte() == USE_STRING_VALUE) {
      long hashCode = buffer.readInt64();
      if (cache.hashCode == hashCode) {
        // skip byteString data
        buffer.increaseReaderIndex(2 + cache.bytes.length);
        updateDynamicString(cache);
        return cache;
      } else {
        MetaStringBytes byteString = trySkipMetaStringBytes(buffer, hashCode);
        updateDynamicString(byteString);
        return byteString;
      }
    } else {
      return dynamicReadStringIds[buffer.readInt16()];
    }
  }

  /** Read enum string by try to reuse previous read {@link MetaStringBytes} object. */
  private MetaStringBytes trySkipMetaStringBytes(MemoryBuffer buffer, long hashCode) {
    MetaStringBytes byteString = hash2MetaStringBytesMap.get(hashCode);
    if (byteString == null) {
      int strBytesLength = buffer.readInt16();
      byte[] strBytes = buffer.readBytes(strBytesLength);
      byteString = new MetaStringBytes(strBytes, hashCode);
      hash2MetaStringBytesMap.put(hashCode, byteString);
    } else {
      // skip byteString data
      buffer.increaseReaderIndex(2 + byteString.bytes.length);
    }
    return byteString;
  }

  private void updateDynamicString(MetaStringBytes byteString) {
    short currentDynamicReadId = dynamicReadStringId++;
    MetaStringBytes[] dynamicReadStringIds = this.dynamicReadStringIds;
    if (dynamicReadStringIds.length <= currentDynamicReadId) {
      MetaStringBytes[] tmp = new MetaStringBytes[currentDynamicReadId * 2];
      System.arraycopy(dynamicReadStringIds, 0, tmp, 0, dynamicReadStringIds.length);
      dynamicReadStringIds = tmp;
      this.dynamicReadStringIds = tmp;
    }
    dynamicReadStringIds[currentDynamicReadId] = byteString;
  }

  public void reset() {
    resetRead();
    resetWrite();
  }

  public void resetRead() {
    int dynamicReadId = this.dynamicReadStringId;
    if (dynamicReadId != 0) {
      for (int i = 0; i < dynamicReadId; i++) {
        dynamicReadStringIds[i] = null;
      }
      this.dynamicReadStringId = 0;
    }
  }

  public void resetWrite() {
    int dynamicWriteStringId = this.dynamicWriteStringId;
    if (dynamicWriteStringId != 0) {
      for (int i = 0; i < dynamicWriteStringId; i++) {
        dynamicWrittenString[i].dynamicWriteStringId =
            MetaStringBytes.DEFAULT_DYNAMIC_WRITE_STRING_ID;
        dynamicWrittenString[i] = null;
      }
      this.dynamicWriteStringId = 0;
    }
  }
}
