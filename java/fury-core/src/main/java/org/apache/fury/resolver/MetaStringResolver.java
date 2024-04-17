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

import org.apache.fury.collection.LongMap;
import org.apache.fury.collection.ObjectMap;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.meta.MetaString;

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
  private final ObjectMap<MetaStringBytes, String> metaStringBytes2StringMap =
      new ObjectMap<>(initialCapacity, furyMapLoadFactor);
  private final LongMap<MetaStringBytes> hash2MetaStringBytesMap =
      new LongMap<>(initialCapacity, furyMapLoadFactor);
  // Every enum bytes should be singleton at every fury, since we keep state in it.
  private final ObjectMap<MetaString, MetaStringBytes> metaString2BytesMap =
      new ObjectMap<>(initialCapacity, furyMapLoadFactor);
  private MetaStringBytes[] dynamicWrittenString = new MetaStringBytes[32];
  private MetaStringBytes[] dynamicReadStringIds = new MetaStringBytes[32];
  private short dynamicWriteStringId;
  private short dynamicReadStringId;

  public MetaStringResolver() {
    dynamicWriteStringId = 0;
    dynamicReadStringId = 0;
  }

  MetaStringBytes getOrCreateMetaStringBytes(MetaString str) {
    MetaStringBytes metaStringBytes = metaString2BytesMap.get(str);
    if (metaStringBytes == null) {
      metaStringBytes = new MetaStringBytes(str);
      metaString2BytesMap.put(str, metaStringBytes);
    }
    return metaStringBytes;
  }

  public void writeMetaStringBytesWithFlag(MemoryBuffer buffer, MetaStringBytes byteString) {
    short id = byteString.dynamicWriteStringId;
    if (id == MetaStringBytes.DEFAULT_DYNAMIC_WRITE_STRING_ID) {
      id = dynamicWriteStringId++;
      byteString.dynamicWriteStringId = id;
      MetaStringBytes[] dynamicWrittenMetaString = this.dynamicWrittenString;
      if (dynamicWrittenMetaString.length <= id) {
        dynamicWrittenMetaString = growWrite(id);
      }
      dynamicWrittenMetaString[id] = byteString;
      buffer.writeVarUint32Small7(byteString.bytes.length << 2 | 0b1);
      buffer.writeInt64(byteString.hashCode);
      buffer.writeBytes(byteString.bytes);
    } else {
      buffer.writeVarUint32Small7(((id + 1) << 2) | 0b11);
    }
  }

  public void writeMetaStringBytes(MemoryBuffer buffer, MetaStringBytes byteString) {
    short id = byteString.dynamicWriteStringId;
    if (id == MetaStringBytes.DEFAULT_DYNAMIC_WRITE_STRING_ID) {
      id = dynamicWriteStringId++;
      byteString.dynamicWriteStringId = id;
      MetaStringBytes[] dynamicWrittenMetaString = this.dynamicWrittenString;
      if (dynamicWrittenMetaString.length <= id) {
        dynamicWrittenMetaString = growWrite(id);
      }
      dynamicWrittenMetaString[id] = byteString;
      buffer.writeVarUint32Small7(byteString.bytes.length << 1);
      buffer.writeInt64(byteString.hashCode);
      buffer.writeBytes(byteString.bytes);
    } else {
      buffer.writeVarUint32Small7(((id + 1) << 1) | 1);
    }
  }

  private MetaStringBytes[] growWrite(int id) {
    MetaStringBytes[] tmp = new MetaStringBytes[id * 2];
    System.arraycopy(dynamicWrittenString, 0, tmp, 0, dynamicWrittenString.length);
    return this.dynamicWrittenString = tmp;
  }

  public String readMetaString(MemoryBuffer buffer) {
    MetaStringBytes byteString = readMetaStringBytes(buffer);
    String str = metaStringBytes2StringMap.get(byteString);
    if (str == null) {
      // TODO support meta string in other languages.
      str = byteString.decode('.', '_');
      metaStringBytes2StringMap.put(byteString, str);
    }
    return str;
  }

  public MetaStringBytes readMetaStringBytesWithFlag(MemoryBuffer buffer, int header) {
    int len = header >>> 2;
    if ((header & 0b10) == 0) {
      long hashCode = buffer.readInt64();
      MetaStringBytes byteString = trySkipMetaStringBytes(buffer, len, hashCode);
      updateDynamicString(byteString);
      return byteString;
    } else {
      return dynamicReadStringIds[len - 1];
    }
  }

  public MetaStringBytes readMetaStringBytesWithFlag(
      MemoryBuffer buffer, MetaStringBytes cache, int header) {
    int len = header >>> 2;
    if ((header & 0b10) == 0) {
      long hashCode = buffer.readInt64();
      if (cache.hashCode == hashCode) {
        // skip byteString data
        buffer.increaseReaderIndex(len);
        updateDynamicString(cache);
        return cache;
      }
      MetaStringBytes byteString = trySkipMetaStringBytes(buffer, len, hashCode);
      updateDynamicString(byteString);
      return byteString;
    } else {
      return dynamicReadStringIds[len - 1];
    }
  }

  MetaStringBytes readMetaStringBytes(MemoryBuffer buffer) {
    int header = buffer.readVarUint32Small7();
    int len = header >>> 1;
    if ((header & 0b1) == 0) {
      long hashCode = buffer.readInt64();
      MetaStringBytes byteString = trySkipMetaStringBytes(buffer, len, hashCode);
      updateDynamicString(byteString);
      return byteString;
    } else {
      return dynamicReadStringIds[len - 1];
    }
  }

  MetaStringBytes readMetaStringBytes(MemoryBuffer buffer, MetaStringBytes cache) {
    int header = buffer.readVarUint32Small7();
    int len = header >>> 1;
    if ((header & 0b1) == 0) {
      long hashCode = buffer.readInt64();
      if (cache.hashCode == hashCode) {
        // skip byteString data
        buffer.increaseReaderIndex(len);
        updateDynamicString(cache);
        return cache;
      } else {
        MetaStringBytes byteString = trySkipMetaStringBytes(buffer, len, hashCode);
        updateDynamicString(byteString);
        return byteString;
      }
    } else {
      return dynamicReadStringIds[len - 1];
    }
  }

  /** Read enum string by try to reuse previous read {@link MetaStringBytes} object. */
  private MetaStringBytes trySkipMetaStringBytes(MemoryBuffer buffer, int len, long hashCode) {
    MetaStringBytes byteString = hash2MetaStringBytesMap.get(hashCode);
    if (byteString == null) {
      byteString = new MetaStringBytes(buffer.readBytes(len), hashCode);
      hash2MetaStringBytesMap.put(hashCode, byteString);
    } else {
      // skip byteString data
      buffer.increaseReaderIndex(len);
    }
    return byteString;
  }

  private void updateDynamicString(MetaStringBytes byteString) {
    short currentDynamicReadId = dynamicReadStringId++;
    MetaStringBytes[] dynamicReadStringIds = this.dynamicReadStringIds;
    if (dynamicReadStringIds.length <= currentDynamicReadId) {
      dynamicReadStringIds = growRead(currentDynamicReadId);
    }
    dynamicReadStringIds[currentDynamicReadId] = byteString;
  }

  private MetaStringBytes[] growRead(int id) {
    MetaStringBytes[] tmp = new MetaStringBytes[id * 2];
    System.arraycopy(dynamicReadStringIds, 0, tmp, 0, dynamicReadStringIds.length);
    return this.dynamicReadStringIds = tmp;
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
