/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.format.row.binary.writer;

import io.fury.format.row.binary.BinaryArray;
import io.fury.format.row.binary.BinaryMap;
import io.fury.format.row.binary.BinaryRow;
import io.fury.format.vectorized.ArrowUtils;
import io.fury.memory.BitUtils;
import io.fury.memory.MemoryBuffer;
import io.fury.util.DecimalUtils;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.DecimalUtility;

/** Base class for writing row-format structures. */
public abstract class BinaryWriter {
  public static int THRESHOLD_LEN = 8;

  public static int roundNumberOfBytesToNearestWord(int numBytes) {
    int remainder = numBytes & 0x07;
    if (remainder == 0) {
      return numBytes;
    } else {
      return numBytes + (8 - remainder);
    }
  }

  // MemoryBuffer should be word-aligned since binary is word-aligned
  protected MemoryBuffer buffer;
  // The writerIndex of the buffer where the writing starts.
  protected int startIndex;

  // avoid polymorphic setNullAt/setNotNullAt to inline for performance.
  // array use 8 byte for numElements
  private final int bytesBeforeBitMap;
  protected final List<BinaryWriter> children;

  protected BinaryWriter(MemoryBuffer buffer, int bytesBeforeBitMap) {
    this.buffer = buffer;
    this.bytesBeforeBitMap = bytesBeforeBitMap;
    children = new ArrayList<>();
  }

  public final MemoryBuffer getBuffer() {
    return buffer;
  }

  public final int writerIndex() {
    return buffer.writerIndex();
  }

  public final int size() {
    return buffer.writerIndex() - startIndex;
  }

  public final int getStartIndex() {
    return startIndex;
  }

  public final void increaseWriterIndex(int val) {
    buffer.increaseWriterIndex(val);
  }

  public final void increaseWriterIndexToAligned(int val) {
    int writerIndex = buffer.writerIndex();
    int maybeEnd = writerIndex + val;
    int remainder = maybeEnd & 0x07;
    if (remainder == 0) {
      buffer.increaseWriterIndex(val);
    } else {
      int end = maybeEnd - remainder + 8;
      int newVal = end - writerIndex;
      buffer.grow(newVal);
      for (int i = maybeEnd; i < end; i++) {
        buffer.put(i, (byte) 0);
      }
      buffer.increaseWriterIndex(newVal);
    }
  }

  protected final void grow(int neededSize) {
    buffer.grow(neededSize);
  }

  public final void setOffsetAndSize(int ordinal, int size) {
    setOffsetAndSize(ordinal, buffer.writerIndex(), size);
  }

  public final void setOffsetAndSize(int ordinal, int absoluteOffset, int size) {
    final long relativeOffset = absoluteOffset - startIndex;
    final long offsetAndSize = (relativeOffset << 32) | (long) size;
    write(ordinal, offsetAndSize);
  }

  /** if numBytes is not multiple of 8, zero 8 byte until multiple of 8. */
  protected final void zeroOutPaddingBytes(int numBytes) {
    if ((numBytes & 0x07) > 0) {
      buffer.putLong(buffer.writerIndex() + ((numBytes >> 3) << 3), 0L);
    }
  }

  /**
   * Since writer is used for one-pass writer, same field won't be writer twice. There is no need to
   * put zero into the corresponding field when set null.
   */
  public final void setNullAt(int ordinal) {
    BitUtils.set(buffer, startIndex + bytesBeforeBitMap, ordinal);
  }

  public final void setNotNullAt(int ordinal) {
    BitUtils.unset(buffer, startIndex + bytesBeforeBitMap, ordinal);
  }

  public final boolean isNullAt(int ordinal) {
    return BitUtils.isSet(buffer, startIndex + bytesBeforeBitMap, ordinal);
  }

  public abstract int getOffset(int ordinal);

  public abstract void write(int ordinal, byte value);

  public abstract void write(int ordinal, boolean value);

  public abstract void write(int ordinal, short value);

  public abstract void write(int ordinal, int value);

  public abstract void write(int ordinal, float value);

  public abstract void write(int ordinal, BigDecimal input);

  public final void write(int ordinal, long value) {
    buffer.putLong(getOffset(ordinal), value);
  }

  public final void write(int ordinal, double value) {
    buffer.putDouble(getOffset(ordinal), value);
  }

  // String is not 8-byte aligned
  public final void write(int ordinal, String input) {
    write(ordinal, input, StandardCharsets.UTF_8);
  }

  public final void write(int ordinal, String input, Charset charset) {
    byte[] bytes = input.getBytes(charset);
    int numBytes = bytes.length;
    if (numBytes < THRESHOLD_LEN) {
      writeDirectly(ordinal, bytes);
    } else {
      write(ordinal, bytes);
    }
  }

  // byte[] is not 8-byte aligned
  public final void write(int ordinal, byte[] input) {
    writeUnaligned(ordinal, input, 0, input.length);
  }

  // BinaryRow is aligned
  public final void write(int ordinal, BinaryRow row) {
    writeAlignedBytes(ordinal, row.getBuffer(), row.getBaseOffset(), row.getSizeInBytes());
  }

  // BinaryMap is aligned
  public final void write(int ordinal, BinaryMap map) {
    writeAlignedBytes(ordinal, map.getBuf(), map.getBaseOffset(), map.getSizeInBytes());
  }

  // BinaryArray is aligned
  public final void write(int ordinal, BinaryArray array) {
    writeAlignedBytes(ordinal, array.getBuffer(), array.getBaseOffset(), array.getSizeInBytes());
  }

  /** This operation will increase writerIndex by aligned 8-byte. */
  public final void writeUnaligned(int ordinal, byte[] input, int offset, int numBytes) {
    final int roundedSize = roundNumberOfBytesToNearestWord(numBytes);
    buffer.grow(roundedSize);
    zeroOutPaddingBytes(numBytes);
    buffer.put(buffer.writerIndex(), input, offset, numBytes);
    setOffsetAndSize(ordinal, numBytes);
    buffer.increaseWriterIndexUnsafe(roundedSize);
  }

  /** This operation will increase writerIndex by aligned 8-byte. */
  public final void writeUnaligned(int ordinal, MemoryBuffer input, int offset, int numBytes) {
    final int roundedSize = roundNumberOfBytesToNearestWord(numBytes);
    buffer.grow(roundedSize);
    zeroOutPaddingBytes(numBytes);
    buffer.copyFrom(buffer.writerIndex(), input, offset, numBytes);
    setOffsetAndSize(ordinal, numBytes);
    buffer.increaseWriterIndexUnsafe(roundedSize);
  }

  public final void writeAlignedBytes(
      int ordinal, MemoryBuffer input, int baseOffset, int numBytes) {
    buffer.grow(numBytes);
    buffer.copyFrom(buffer.writerIndex(), input, baseOffset, numBytes);
    setOffsetAndSize(ordinal, numBytes);
    buffer.increaseWriterIndex(numBytes);
  }

  protected final void writeDecimal(int ordinal, BigDecimal value, ArrowType.Decimal type) {
    if (value != null) {
      DecimalUtility.checkPrecisionAndScale(value, type.getPrecision(), type.getScale());
      grow(DecimalUtils.DECIMAL_BYTE_LENGTH);
      ArrowBuf arrowBuf = ArrowUtils.buffer(DecimalUtils.DECIMAL_BYTE_LENGTH);
      DecimalUtility.writeBigDecimalToArrowBuf(
          value, arrowBuf, 0, DecimalUtils.DECIMAL_BYTE_LENGTH);
      buffer.copyFromUnsafe(
          writerIndex(), null, arrowBuf.memoryAddress(), DecimalUtils.DECIMAL_BYTE_LENGTH);
      arrowBuf.getReferenceManager().release();
      setOffsetAndSize(ordinal, writerIndex(), DecimalUtils.DECIMAL_BYTE_LENGTH);
      increaseWriterIndex(DecimalUtils.DECIMAL_BYTE_LENGTH);
    } else {
      setNullAt(ordinal);
    }
  }

  final void writeDirectly(int ordinal, byte[] bytes) {
    buffer.grow(8);
    long value = bytesToLong(bytes);
    buffer.putLong(getOffset(ordinal), value);
    buffer.increaseWriterIndexUnsafe(8);
  }

  /** write long value to position pointed by current writerIndex. */
  public final void writeDirectly(long value) {
    buffer.grow(8);
    buffer.putLong(writerIndex(), value);
    buffer.increaseWriterIndexUnsafe(8);
  }

  /** write long value to position pointed by offset. */
  public final void writeDirectly(int offset, long value) {
    buffer.putLong(offset, value);
  }

  public final void copyTo(BinaryWriter writer, int ordinal) {
    writer.writeAlignedBytes(ordinal, buffer, startIndex, buffer.writerIndex());
  }

  public final void setBuffer(MemoryBuffer buffer) {
    this.buffer = buffer;
    for (BinaryWriter child : children) {
      child.setBuffer(buffer);
    }
  }

  final long bytesToLong(byte[] bytes) {
    long value = 0;
    // Encode byte array to long type value.
    for (byte b : bytes) {
      value <<= 8;
      long byteValue = b & 0xFF;
      value ^= byteValue;
    }
    // Record flag and array length in one byte.
    value = ((128L + bytes.length) << 56) | value;
    return value;
  }
}
