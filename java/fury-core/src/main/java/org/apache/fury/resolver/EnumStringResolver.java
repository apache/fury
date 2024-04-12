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
public final class EnumStringResolver {
  public static final byte USE_STRING_VALUE = 0;
  public static final byte USE_STRING_ID = 1;
  private static final int initialCapacity = 8;
  // use a lower load factor to minimize hash collision
  private static final float furyMapLoadFactor = 0.25f;

  // Every deserialization for unregistered string will query it, performance is important.
  private final ObjectMap<EnumStringBytes, String> enumStringBytes2StringMap =
      new ObjectMap<>(initialCapacity, furyMapLoadFactor);
  private final LongMap<EnumStringBytes> hash2EnumStringBytesMap =
      new LongMap<>(initialCapacity, furyMapLoadFactor);
  // Every enum bytes should be singleton at every fury, since we keep state in it.
  private final ObjectMap<String, EnumStringBytes> enumString2BytesMap =
      new ObjectMap<>(initialCapacity, furyMapLoadFactor);
  private EnumStringBytes[] dynamicWrittenString = new EnumStringBytes[32];
  private EnumStringBytes[] dynamicReadStringIds = new EnumStringBytes[32];
  private short dynamicWriteStringId;
  private short dynamicReadStringId;

  public EnumStringResolver() {
    dynamicWriteStringId = 0;
    dynamicReadStringId = 0;
  }

  EnumStringBytes getOrCreateEnumStringBytes(String str) {
    EnumStringBytes enumStringBytes = enumString2BytesMap.get(str);
    if (enumStringBytes == null) {
      enumStringBytes = new EnumStringBytes(str);
      enumString2BytesMap.put(str, enumStringBytes);
    }
    return enumStringBytes;
  }

  public void writeEnumString(MemoryBuffer buffer, String str) {
    writeEnumStringBytes(buffer, getOrCreateEnumStringBytes(str));
  }

  public String readEnumString(MemoryBuffer buffer) {
    EnumStringBytes byteString = readEnumStringBytes(buffer);
    String str = enumStringBytes2StringMap.get(byteString);
    if (str == null) { // TODO use org.apache.fury.resolver.ObjectMap
      str = new String(byteString.bytes, StandardCharsets.UTF_8);
      enumStringBytes2StringMap.put(byteString, str);
    }
    return str;
  }

  public void writeEnumStringBytes(MemoryBuffer buffer, EnumStringBytes byteString) {
    short id = byteString.dynamicWriteStringId;
    int writerIndex = buffer.writerIndex();
    if (id == EnumStringBytes.DEFAULT_DYNAMIC_WRITE_STRING_ID) {
      id = dynamicWriteStringId++;
      byteString.dynamicWriteStringId = id;
      EnumStringBytes[] dynamicWrittenEnumString = this.dynamicWrittenString;
      if (dynamicWrittenEnumString.length <= id) {
        EnumStringBytes[] tmp = new EnumStringBytes[id * 2];
        System.arraycopy(dynamicWrittenEnumString, 0, tmp, 0, dynamicWrittenEnumString.length);
        dynamicWrittenEnumString = tmp;
        this.dynamicWrittenString = tmp;
      }
      dynamicWrittenEnumString[id] = byteString;
      int bytesLen = byteString.bytes.length;
      buffer.increaseWriterIndex(11 + bytesLen);
      buffer._unsafePut(writerIndex, USE_STRING_VALUE);
      // Since duplicate enum string writing are avoided by dynamic id,
      // use 8-byte hash won't increase too much space.
      buffer._unsafePutLong(writerIndex + 1, byteString.hashCode);
      buffer._unsafePutShort(writerIndex + 9, (short) bytesLen);
      buffer.put(writerIndex + 11, byteString.bytes, 0, bytesLen);
    } else {
      buffer.increaseWriterIndex(3);
      buffer._unsafePut(writerIndex, USE_STRING_ID);
      buffer._unsafePutShort(writerIndex + 1, id);
    }
  }

  EnumStringBytes readEnumStringBytes(MemoryBuffer buffer) {
    if (buffer.readByte() == USE_STRING_VALUE) {
      long hashCode = buffer.readInt64();
      EnumStringBytes byteString = trySkipEnumStringBytes(buffer, hashCode);
      updateDynamicString(byteString);
      return byteString;
    } else {
      return dynamicReadStringIds[buffer.readInt16()];
    }
  }

  EnumStringBytes readEnumStringBytes(MemoryBuffer buffer, EnumStringBytes cache) {
    if (buffer.readByte() == USE_STRING_VALUE) {
      long hashCode = buffer.readInt64();
      if (cache.hashCode == hashCode) {
        // skip byteString data
        buffer.increaseReaderIndex(2 + cache.bytes.length);
        updateDynamicString(cache);
        return cache;
      } else {
        EnumStringBytes byteString = trySkipEnumStringBytes(buffer, hashCode);
        updateDynamicString(byteString);
        return byteString;
      }
    } else {
      return dynamicReadStringIds[buffer.readInt16()];
    }
  }

  /** Read enum string by try to reuse previous read {@link EnumStringBytes} object. */
  private EnumStringBytes trySkipEnumStringBytes(MemoryBuffer buffer, long hashCode) {
    EnumStringBytes byteString = hash2EnumStringBytesMap.get(hashCode);
    if (byteString == null) {
      int strBytesLength = buffer.readInt16();
      byte[] strBytes = buffer.readBytes(strBytesLength);
      byteString = new EnumStringBytes(strBytes, hashCode);
      hash2EnumStringBytesMap.put(hashCode, byteString);
    } else {
      // skip byteString data
      buffer.increaseReaderIndex(2 + byteString.bytes.length);
    }
    return byteString;
  }

  private void updateDynamicString(EnumStringBytes byteString) {
    short currentDynamicReadId = dynamicReadStringId++;
    EnumStringBytes[] dynamicReadStringIds = this.dynamicReadStringIds;
    if (dynamicReadStringIds.length <= currentDynamicReadId) {
      EnumStringBytes[] tmp = new EnumStringBytes[currentDynamicReadId * 2];
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
            EnumStringBytes.DEFAULT_DYNAMIC_WRITE_STRING_ID;
        dynamicWrittenString[i] = null;
      }
      this.dynamicWriteStringId = 0;
    }
  }
}
