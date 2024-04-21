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

package org.apache.fury.format.row.binary;

import java.nio.ByteBuffer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.fury.format.row.MapData;
import org.apache.fury.format.type.DataTypes;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.util.Platform;

/**
 * An BinaryMap implementation of Map which is backed by two BinaryArray./FuryObjectOutput
 *
 * <p>[8 byte(key array size in bytes)] + [Key BinaryArray] + [Value BinaryArray].
 *
 * <p>Note that, user is responsible to guarantee that the key array does not have duplicated
 * elements otherwise the behavior is undefined.
 */
public class BinaryMap implements MapData {
  private final BinaryArray keys;
  private final BinaryArray values;
  private final Field field;
  private MemoryBuffer buf;
  private int baseOffset;
  private int sizeInBytes;

  public BinaryMap(Field field) {
    this.field = field;
    this.keys = new BinaryArray(DataTypes.keyArrayFieldForMap(field));
    this.values = new BinaryArray(DataTypes.itemArrayFieldForMap(field));
  }

  public BinaryMap(BinaryArray keys, BinaryArray values, Field field) {
    this.keys = keys;
    this.values = values;
    this.field = field;
    this.baseOffset = 0;
    this.sizeInBytes = keys.getSizeInBytes() + values.getSizeInBytes() + 8;

    MemoryBuffer copyBuf = MemoryUtils.buffer(sizeInBytes);
    copyBuf.putInt32(0, keys.getSizeInBytes());
    copyBuf.putInt32(4, 0);
    keys.getBuffer().copyTo(baseOffset, copyBuf, 8, keys.getSizeInBytes());
    values
        .getBuffer()
        .copyTo(baseOffset, copyBuf, keys.getSizeInBytes() + 8, values.getSizeInBytes());
    this.buf = copyBuf;
  }

  public void pointTo(MemoryBuffer buf, int offset, int sizeInBytes) {
    this.buf = buf;
    this.baseOffset = offset;
    this.sizeInBytes = sizeInBytes;
    // Read the numBytes of key array from the aligned first 8 bytes as int.
    final int keyArrayBytes = buf.getInt32(offset);
    assert keyArrayBytes >= 0 : "keyArrayBytes (" + keyArrayBytes + ") should >= 0";
    final int valueArrayBytes = sizeInBytes - keyArrayBytes - 8;
    assert valueArrayBytes >= 0 : "valueArraySize (" + valueArrayBytes + ") should >= 0";

    keys.pointTo(buf, offset + 8, keyArrayBytes);
    values.pointTo(buf, offset + 8 + keyArrayBytes, valueArrayBytes);
    assert keys.numElements() == values.numElements();
  }

  public MemoryBuffer getBuf() {
    return buf;
  }

  public int getBaseOffset() {
    return baseOffset;
  }

  public int getSizeInBytes() {
    return sizeInBytes;
  }

  public Field getField() {
    return field;
  }

  @Override
  public int numElements() {
    return keys.numElements();
  }

  @Override
  public BinaryArray keyArray() {
    return keys;
  }

  @Override
  public BinaryArray valueArray() {
    return values;
  }

  @Override
  public MapData copy() {
    MemoryBuffer copyBuf = MemoryUtils.buffer(sizeInBytes);
    buf.copyTo(baseOffset, copyBuf, 0, sizeInBytes);
    BinaryMap mapCopy = new BinaryMap(field);
    mapCopy.pointTo(copyBuf, 0, sizeInBytes);
    return mapCopy;
  }

  public void writeToMemory(Object target, long targetOffset) {
    buf.copyToUnsafe(baseOffset, target, targetOffset, sizeInBytes);
  }

  public void writeTo(ByteBuffer buffer) {
    assert (buffer.hasArray());
    byte[] target = buffer.array();
    int offset = buffer.arrayOffset();
    int pos = buffer.position();
    writeToMemory(target, Platform.BYTE_ARRAY_OFFSET + offset + pos);
    buffer.position(pos + sizeInBytes);
  }

  @Override
  public String toString() {
    return "BinaryMap{"
        + "keys="
        + keys
        + ", values="
        + values
        + ", sizeInBytes="
        + sizeInBytes
        + '}';
  }
}
