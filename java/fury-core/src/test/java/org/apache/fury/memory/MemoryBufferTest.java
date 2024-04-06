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

package org.apache.fury.memory;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Random;
import org.apache.fury.util.Platform;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MemoryBufferTest {

  @Test
  public void testBufferWrite() {
    MemoryBuffer buffer = MemoryUtils.buffer(8);
    buffer.writeBoolean(true);
    buffer.writeByte(Byte.MIN_VALUE);
    buffer.writeChar('a');
    buffer.writeShort(Short.MAX_VALUE);
    buffer.writeInt(Integer.MAX_VALUE);
    buffer.writeLong(Long.MAX_VALUE);
    buffer.writeFloat(Float.MAX_VALUE);
    buffer.writeDouble(Double.MAX_VALUE);
    byte[] bytes = new byte[] {1, 2, 3, 4};
    buffer.writeBytes(bytes);

    assertTrue(buffer.readBoolean());
    assertEquals(buffer.readByte(), Byte.MIN_VALUE);
    assertEquals(buffer.readChar(), 'a');
    assertEquals(buffer.readShort(), Short.MAX_VALUE);
    assertEquals(buffer.readInt(), Integer.MAX_VALUE);
    assertEquals(buffer.readLong(), Long.MAX_VALUE);
    assertEquals(buffer.readFloat(), Float.MAX_VALUE, 0.1);
    assertEquals(buffer.readDouble(), Double.MAX_VALUE, 0.1);
    assertEquals(buffer.readBytes(bytes.length), bytes);
    assertEquals(buffer.readerIndex(), buffer.writerIndex());
  }

  @Test
  public void testBufferUnsafeWrite() {
    {
      MemoryBuffer buffer = MemoryUtils.buffer(1024);
      byte[] heapMemory = buffer.getHeapMemory();
      long pos = buffer.getUnsafeAddress();
      assertEquals(buffer.getUnsafeWriterAddress(), pos);
      assertEquals(buffer.getUnsafeReaderAddress(), pos);
      MemoryBuffer.unsafePut(heapMemory, pos, Byte.MIN_VALUE);
      pos += 1;
      MemoryBuffer.unsafePutShort(heapMemory, pos, Short.MAX_VALUE);
      pos += 2;
      MemoryBuffer.unsafePutInt(heapMemory, pos, Integer.MIN_VALUE);
      pos += 4;
      MemoryBuffer.unsafePutLong(heapMemory, pos, Long.MAX_VALUE);
      pos += 8;
      MemoryBuffer.unsafePutDouble(heapMemory, pos, -1);
      pos += 8;
      MemoryBuffer.unsafePutFloat(heapMemory, pos, -1);
      assertEquals(buffer.unsafeGetFloat((int) (pos - Platform.BYTE_ARRAY_OFFSET)), -1);
      pos -= 8;
      assertEquals(buffer.unsafeGetDouble((int) (pos - Platform.BYTE_ARRAY_OFFSET)), -1);
      pos -= 8;
      assertEquals(MemoryBuffer.unsafeGetLong(heapMemory, pos), Long.MAX_VALUE);
      pos -= 4;
      assertEquals(MemoryBuffer.unsafeGetInt(heapMemory, pos), Integer.MIN_VALUE);
      pos -= 2;
      assertEquals(buffer.getShort((int) (pos - Platform.BYTE_ARRAY_OFFSET)), Short.MAX_VALUE);
      pos -= 1;
      assertEquals(buffer.get((int) (pos - Platform.BYTE_ARRAY_OFFSET)), Byte.MIN_VALUE);
    }
    {
      MemoryBuffer buffer = MemoryUtils.buffer(1024);
      int index = 0;
      buffer.unsafePut(index, Byte.MIN_VALUE);
      index += 1;
      buffer.unsafePutShort(index, Short.MAX_VALUE);
      index += 2;
      buffer.unsafePutInt(index, Integer.MIN_VALUE);
      index += 4;
      buffer.unsafePutLong(index, Long.MAX_VALUE);
      index += 8;
      buffer.unsafePutDouble(index, -1);
      index += 8;
      buffer.unsafePutFloat(index, -1);
      assertEquals(buffer.unsafeGetFloat(index), -1);
      index -= 8;
      assertEquals(buffer.unsafeGetDouble(index), -1);
      index -= 8;
      assertEquals(buffer.unsafeGetLong(index), Long.MAX_VALUE);
      index -= 4;
      assertEquals(buffer.unsafeGetInt(index), Integer.MIN_VALUE);
      index -= 2;
      assertEquals(buffer.unsafeGetShort(index), Short.MAX_VALUE);
      index -= 1;
      assertEquals(buffer.unsafeGet(index), Byte.MIN_VALUE);
    }
  }

  @Test
  public void testWrapBuffer() {
    {
      byte[] bytes = new byte[8];
      int offset = 2;
      bytes[offset] = 1;
      MemoryBuffer buffer = MemoryUtils.wrap(bytes, offset, 2);
      assertEquals(buffer.readByte(), bytes[offset]);
    }
    {
      byte[] bytes = new byte[8];
      int offset = 2;
      MemoryBuffer buffer = MemoryUtils.wrap(ByteBuffer.wrap(bytes, offset, 2));
      assertEquals(buffer.readByte(), bytes[offset]);
    }
    {
      ByteBuffer direct = ByteBuffer.allocateDirect(8);
      int offset = 2;
      direct.put(offset, (byte) 1);
      direct.position(offset);
      MemoryBuffer buffer = MemoryUtils.wrap(direct);
      assertEquals(buffer.readByte(), direct.get(offset));
    }
  }

  @Test
  public void testSliceAsByteBuffer() {
    byte[] data = new byte[10];
    new Random().nextBytes(data);
    {
      MemoryBuffer buffer = MemoryUtils.wrap(data, 5, 5);
      assertEquals(buffer.sliceAsByteBuffer(), ByteBuffer.wrap(data, 5, 5));
    }
    {
      ByteBuffer direct = ByteBuffer.allocateDirect(10);
      direct.put(data);
      direct.flip();
      direct.position(5);
      MemoryBuffer buffer = MemoryUtils.wrap(direct);
      assertEquals(buffer.sliceAsByteBuffer(), direct);
      Assert.assertEquals(
          Platform.getAddress(buffer.sliceAsByteBuffer()), Platform.getAddress(direct) + 5);
    }
    {
      long address = 0;
      try {
        address = Platform.allocateMemory(10);
        ByteBuffer direct = Platform.wrapDirectBuffer(address, 10);
        direct.put(data);
        direct.flip();
        direct.position(5);
        MemoryBuffer buffer = MemoryUtils.wrap(direct);
        assertEquals(buffer.sliceAsByteBuffer(), direct);
        assertEquals(Platform.getAddress(buffer.sliceAsByteBuffer()), address + 5);
      } finally {
        Platform.freeMemory(address);
      }
    }
  }

  @Test
  public void testCompare() {
    MemoryBuffer buf1 = MemoryUtils.buffer(16);
    MemoryBuffer buf2 = MemoryUtils.buffer(16);
    buf1.putLongB(0, 10);
    buf2.putLongB(0, 10);
    buf1.put(9, (byte) 1);
    buf2.put(9, (byte) 2);
    Assert.assertTrue(buf1.compare(buf2, 0, 0, buf1.size()) < 0);
    buf1.put(9, (byte) 3);
    Assert.assertFalse(buf1.compare(buf2, 0, 0, buf1.size()) < 0);
  }

  @Test
  public void testEqualTo() {
    MemoryBuffer buf1 = MemoryUtils.buffer(16);
    MemoryBuffer buf2 = MemoryUtils.buffer(16);
    buf1.putLongB(0, 10);
    buf2.putLongB(0, 10);
    buf1.put(9, (byte) 1);
    buf2.put(9, (byte) 1);
    Assert.assertTrue(buf1.equalTo(buf2, 0, 0, buf1.size()));
    buf1.put(9, (byte) 2);
    Assert.assertFalse(buf1.equalTo(buf2, 0, 0, buf1.size()));
  }

  @Test
  public void testWritePrimitiveArrayWithSizeEmbedded() {
    MemoryBuffer buf = MemoryUtils.buffer(16);
    Random random = new Random(0);
    byte[] bytes = new byte[100];
    random.nextBytes(bytes);
    char[] chars = new char[100];
    for (int i = 0; i < chars.length; i++) {
      chars[i] = (char) random.nextInt();
    }
    buf.writePrimitiveArrayWithSizeEmbedded(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length);
    buf.writePrimitiveArrayWithSizeEmbedded(chars, Platform.CHAR_ARRAY_OFFSET, chars.length * 2);
    assertEquals(bytes, buf.readBytesWithSizeEmbedded());
    assertEquals(chars, buf.readCharsWithSizeEmbedded());
    buf.writePrimitiveArrayAlignedSizeEmbedded(bytes, Platform.BYTE_ARRAY_OFFSET, bytes.length);
    buf.writePrimitiveArrayAlignedSizeEmbedded(chars, Platform.CHAR_ARRAY_OFFSET, chars.length * 2);
    assertEquals(bytes, buf.readBytesAlignedSizeEmbedded());
    assertEquals(chars, buf.readCharsAlignedSizeEmbedded());
  }

  @Test
  public void testWritePositiveVarInt() {
    for (int i = 0; i < 32; i++) {
      MemoryBuffer buf = MemoryUtils.buffer(8);
      for (int j = 0; j < i; j++) {
        buf.writeByte((byte) 1); // make address unaligned.
        buf.readByte();
      }
      checkPositiveVarInt(buf, 1, 1);
      checkPositiveVarInt(buf, 1 << 6, 1);
      checkPositiveVarInt(buf, 1 << 7, 2);
      checkPositiveVarInt(buf, 1 << 13, 2);
      checkPositiveVarInt(buf, 1 << 14, 3);
      checkPositiveVarInt(buf, 1 << 20, 3);
      checkPositiveVarInt(buf, 1 << 21, 4);
      checkPositiveVarInt(buf, 1 << 27, 4);
      checkPositiveVarInt(buf, 1 << 28, 5);
      checkPositiveVarInt(buf, Integer.MAX_VALUE, 5);

      checkPositiveVarInt(buf, -1);
      checkPositiveVarInt(buf, -1 << 6);
      checkPositiveVarInt(buf, -1 << 7);
      checkPositiveVarInt(buf, -1 << 13);
      checkPositiveVarInt(buf, -1 << 14);
      checkPositiveVarInt(buf, -1 << 20);
      checkPositiveVarInt(buf, -1 << 21);
      checkPositiveVarInt(buf, -1 << 27);
      checkPositiveVarInt(buf, -1 << 28);
      checkPositiveVarInt(buf, Byte.MIN_VALUE);
      checkPositiveVarInt(buf, Short.MIN_VALUE);
      checkPositiveVarInt(buf, Integer.MIN_VALUE);
    }
  }

  private void checkPositiveVarInt(MemoryBuffer buf, int value, int bytesWritten) {
    assertEquals(buf.writerIndex(), buf.readerIndex());
    int actualBytesWritten = buf.writePositiveVarInt(value);
    assertEquals(actualBytesWritten, bytesWritten);
    int varInt = buf.readPositiveVarInt();
    assertEquals(buf.writerIndex(), buf.readerIndex());
    assertEquals(value, varInt);
  }

  private void checkPositiveVarInt(MemoryBuffer buf, int value) {
    int readerIndex = buf.readerIndex();
    assertEquals(buf.writerIndex(), readerIndex);
    buf.writePositiveVarInt(value);
    int varInt = buf.readPositiveVarInt();
    assertEquals(buf.writerIndex(), buf.readerIndex());
    assertEquals(value, varInt);
    // test slow read branch in `readPositiveVarInt`
    assertEquals(
        buf.slice(readerIndex, buf.readerIndex() - readerIndex).readPositiveVarInt(), value);
  }

  @Test
  public void testWriteVarInt() {
    for (int i = 0; i < 5; i++) {
      checkVarInt(buf(i), 1, 1);
      checkVarInt(buf(i), 1 << 5, 1);
      checkVarInt(buf(i), 1 << 6, 2);
      checkVarInt(buf(i), 1 << 7, 2);
      checkVarInt(buf(i), 1 << 12, 2);
      checkVarInt(buf(i), 1 << 13, 3);
      checkVarInt(buf(i), 1 << 14, 3);
      checkVarInt(buf(i), 1 << 19, 3);
      checkVarInt(buf(i), 1 << 20, 4);
      checkVarInt(buf(i), 1 << 26, 4);
      checkVarInt(buf(i), 1 << 27, 5);
      checkVarInt(buf(i), 1 << 28, 5);
      checkVarInt(buf(i), Integer.MAX_VALUE, 5);

      checkVarInt(buf(i), -1, 1);
      checkVarInt(buf(i), -1 << 6, 1);
      checkVarInt(buf(i), -1 << 7, 2);
      checkVarInt(buf(i), -1 << 13, 2);
      checkVarInt(buf(i), -1 << 14, 3);
      checkVarInt(buf(i), -1 << 20, 3);
      checkVarInt(buf(i), -1 << 21, 4);
      checkVarInt(buf(i), -1 << 27, 4);
      checkVarInt(buf(i), -1 << 28, 5);
      checkVarInt(buf(i), Byte.MIN_VALUE, 2);
      checkVarInt(buf(i), Byte.MAX_VALUE, 2);
      checkVarInt(buf(i), Short.MAX_VALUE, 3);
      checkVarInt(buf(i), Short.MIN_VALUE, 3);
      checkVarInt(buf(i), Integer.MAX_VALUE, 5);
      checkVarInt(buf(i), Integer.MIN_VALUE, 5);
    }
  }

  private MemoryBuffer buf(int numUnaligned) {
    MemoryBuffer buf = MemoryUtils.buffer(1);
    for (int j = 0; j < numUnaligned; j++) {
      buf.writeByte((byte) 1); // make address unaligned.
      buf.readByte();
    }
    return buf;
  }

  private void checkVarInt(MemoryBuffer buf, int value, int bytesWritten) {
    int readerIndex = buf.readerIndex();
    assertEquals(buf.writerIndex(), readerIndex);
    int actualBytesWritten = buf.writeVarInt(value);
    assertEquals(actualBytesWritten, bytesWritten);
    int varInt = buf.readVarInt();
    assertEquals(buf.writerIndex(), buf.readerIndex());
    assertEquals(value, varInt);
    // test slow read branch in `readVarInt`
    assertEquals(buf.slice(readerIndex, buf.readerIndex() - readerIndex).readVarInt(), value);
  }

  @Test
  public void testWriteVarLong() {
    MemoryBuffer buf = MemoryUtils.buffer(8);
    checkVarLong(buf, -1, 1);
    for (int i = 0; i < 9; i++) {
      for (int j = 0; j < i; j++) {
        checkVarLong(buf(i), -1, 1);
        checkVarLong(buf(i), 1, 1);
        checkVarLong(buf(i), 1L << 6, 2);
        checkVarLong(buf(i), 1L << 7, 2);
        checkVarLong(buf(i), -(2 << 5), 1);
        checkVarLong(buf(i), -(2 << 6), 2);
        checkVarLong(buf(i), 1L << 13, 3);
        checkVarLong(buf(i), 1L << 14, 3);
        checkVarLong(buf(i), -(2 << 12), 2);
        checkVarLong(buf(i), -(2 << 13), 3);
        checkVarLong(buf(i), 1L << 19, 3);
        checkVarLong(buf(i), 1L << 20, 4);
        checkVarLong(buf(i), 1L << 21, 4);
        checkVarLong(buf(i), -(2 << 19), 3);
        checkVarLong(buf(i), -(2 << 20), 4);
        checkVarLong(buf(i), 1L << 26, 4);
        checkVarLong(buf(i), 1L << 27, 5);
        checkVarLong(buf(i), 1L << 28, 5);
        checkVarLong(buf(i), -(2 << 26), 4);
        checkVarLong(buf(i), -(2 << 27), 5);
        checkVarLong(buf(i), 1L << 30, 5);
        checkVarLong(buf(i), -(2L << 29), 5);
        checkVarLong(buf(i), 1L << 30, 5);
        checkVarLong(buf(i), -(2L << 30), 5);
        checkVarLong(buf(i), 1L << 32, 5);
        checkVarLong(buf(i), -(2L << 31), 5);
        checkVarLong(buf(i), 1L << 34, 6);
        checkVarLong(buf(i), -(2L << 33), 5);
        checkVarLong(buf(i), 1L << 35, 6);
        checkVarLong(buf(i), -(2L << 34), 6);
        checkVarLong(buf(i), 1L << 41, 7);
        checkVarLong(buf(i), -(2L << 40), 6);
        checkVarLong(buf(i), 1L << 42, 7);
        checkVarLong(buf(i), -(2L << 41), 7);
        checkVarLong(buf(i), 1L << 48, 8);
        checkVarLong(buf(i), -(2L << 47), 7);
        checkVarLong(buf(i), -(2L << 48), 8);
        checkVarLong(buf(i), 1L << 49, 8);
        checkVarLong(buf(i), -(2L << 48), 8);
        checkVarLong(buf(i), -(2L << 54), 8);
        checkVarLong(buf(i), 1L << 54, 8);
        checkVarLong(buf(i), 1L << 55, 9);
        checkVarLong(buf(i), 1L << 56, 9);
        checkVarLong(buf(i), -(2L << 55), 9);
        checkVarLong(buf(i), 1L << 62, 9);
        checkVarLong(buf(i), -(2L << 62), 9);
        checkVarLong(buf(i), 1L << 63 - 1, 9);
        checkVarLong(buf(i), -(2L << 62), 9);
        checkVarLong(buf(i), Long.MAX_VALUE, 9);
        checkVarLong(buf(i), Long.MIN_VALUE, 9);
      }
    }
  }

  private void checkVarLong(MemoryBuffer buf, long value, int bytesWritten) {
    int readerIndex = buf.readerIndex();
    assertEquals(buf.writerIndex(), readerIndex);
    int actualBytesWritten = buf.writeVarLong(value);
    assertEquals(actualBytesWritten, bytesWritten);
    long varLong = buf.readVarLong();
    assertEquals(buf.writerIndex(), buf.readerIndex());
    assertEquals(value, varLong);
    // test slow read branch in `readVarLong`
    assertEquals(buf.slice(readerIndex, buf.readerIndex() - readerIndex).readVarLong(), value);
  }

  @Test
  public void testWritePositiveVarLong() {
    MemoryBuffer buf = MemoryUtils.buffer(8);
    checkPositiveVarint64(buf, -1, 9);
    for (int i = 0; i < 9; i++) {
      for (int j = 0; j < i; j++) {
        checkPositiveVarint64(buf(i), -1, 9);
        checkPositiveVarint64(buf(i), 1, 1);
        checkPositiveVarint64(buf(i), 1L << 6, 1);
        checkPositiveVarint64(buf(i), 1L << 7, 2);
        checkPositiveVarint64(buf(i), -(2 << 5), 9);
        checkPositiveVarint64(buf(i), -(2 << 6), 9);
        checkPositiveVarint64(buf(i), 1L << 13, 2);
        checkPositiveVarint64(buf(i), 1L << 14, 3);
        checkPositiveVarint64(buf(i), -(2 << 12), 9);
        checkPositiveVarint64(buf(i), -(2 << 13), 9);
        checkPositiveVarint64(buf(i), 1L << 20, 3);
        checkPositiveVarint64(buf(i), 1L << 21, 4);
        checkPositiveVarint64(buf(i), -(2 << 19), 9);
        checkPositiveVarint64(buf(i), -(2 << 20), 9);
        checkPositiveVarint64(buf(i), 1L << 27, 4);
        checkPositiveVarint64(buf(i), 1L << 28, 5);
        checkPositiveVarint64(buf(i), -(2 << 26), 9);
        checkPositiveVarint64(buf(i), -(2 << 27), 9);
        checkPositiveVarint64(buf(i), 1L << 30, 5);
        checkPositiveVarint64(buf(i), -(2L << 29), 9);
        checkPositiveVarint64(buf(i), 1L << 30, 5);
        checkPositiveVarint64(buf(i), -(2L << 30), 9);
        checkPositiveVarint64(buf(i), 1L << 32, 5);
        checkPositiveVarint64(buf(i), -(2L << 31), 9);
        checkPositiveVarint64(buf(i), 1L << 34, 5);
        checkPositiveVarint64(buf(i), -(2L << 33), 9);
        checkPositiveVarint64(buf(i), 1L << 35, 6);
        checkPositiveVarint64(buf(i), -(2L << 34), 9);
        checkPositiveVarint64(buf(i), 1L << 41, 6);
        checkPositiveVarint64(buf(i), -(2L << 40), 9);
        checkPositiveVarint64(buf(i), 1L << 42, 7);
        checkPositiveVarint64(buf(i), -(2L << 41), 9);
        checkPositiveVarint64(buf(i), 1L << 48, 7);
        checkPositiveVarint64(buf(i), -(2L << 47), 9);
        checkPositiveVarint64(buf(i), 1L << 49, 8);
        checkPositiveVarint64(buf(i), -(2L << 48), 9);
        checkPositiveVarint64(buf(i), 1L << 55, 8);
        checkPositiveVarint64(buf(i), -(2L << 54), 9);
        checkPositiveVarint64(buf(i), 1L << 56, 9);
        checkPositiveVarint64(buf(i), -(2L << 55), 9);
        checkPositiveVarint64(buf(i), 1L << 62, 9);
        checkPositiveVarint64(buf(i), -(2L << 62), 9);
        checkPositiveVarint64(buf(i), 1L << 63 - 1, 9);
        checkPositiveVarint64(buf(i), -(2L << 62), 9);
        checkPositiveVarint64(buf(i), Long.MAX_VALUE, 9);
        checkPositiveVarint64(buf(i), Long.MIN_VALUE, 9);
      }
    }
  }

  private void checkPositiveVarint64(MemoryBuffer buf, long value, int bytesWritten) {
    int readerIndex = buf.readerIndex();
    assertEquals(buf.writerIndex(), readerIndex);
    int actualBytesWritten = buf.writePositiveVarLong(value);
    assertEquals(actualBytesWritten, bytesWritten);
    long varLong = buf.readPositiveVarLong();
    assertEquals(buf.writerIndex(), buf.readerIndex());
    assertEquals(value, varLong);
    // test slow read branch in `readPositiveVarLong`
    assertEquals(
        buf.slice(readerIndex, buf.readerIndex() - readerIndex).readPositiveVarLong(), value);
  }

  @Test
  public void testWritePositiveVarIntAligned() {
    MemoryBuffer buf = MemoryUtils.buffer(16);
    assertEquals(buf.writePositiveVarIntAligned(1), 4);
    assertEquals(buf.readPositiveAlignedVarInt(), 1);
    assertEquals(buf.writePositiveVarIntAligned(1 << 5), 4);
    assertEquals(buf.readPositiveAlignedVarInt(), 1 << 5);
    assertEquals(buf.writePositiveVarIntAligned(1 << 10), 4);
    assertEquals(buf.readPositiveAlignedVarInt(), 1 << 10);
    assertEquals(buf.writePositiveVarIntAligned(1 << 15), 4);
    assertEquals(buf.readPositiveAlignedVarInt(), 1 << 15);
    assertEquals(buf.writePositiveVarIntAligned(1 << 20), 4);
    assertEquals(buf.readPositiveAlignedVarInt(), 1 << 20);
    assertEquals(buf.writePositiveVarIntAligned(1 << 25), 8);
    assertEquals(buf.readPositiveAlignedVarInt(), 1 << 25);
    assertEquals(buf.writePositiveVarIntAligned(1 << 30), 8);
    assertEquals(buf.readPositiveAlignedVarInt(), 1 << 30);
    assertEquals(buf.writePositiveVarIntAligned(Integer.MAX_VALUE), 8);
    assertEquals(buf.readPositiveAlignedVarInt(), Integer.MAX_VALUE);
    buf.writeByte((byte) 1); // make address unaligned.
    buf.writeShort((short) 1); // make address unaligned.
    assertEquals(buf.writePositiveVarIntAligned(Integer.MAX_VALUE), 9);
    buf.readByte();
    buf.readShort();
    assertEquals(buf.readPositiveAlignedVarInt(), Integer.MAX_VALUE);
    for (int i = 0; i < 32; i++) {
      MemoryBuffer buf1 = MemoryUtils.buffer(16);
      assertAligned(i, buf1);
    }
    MemoryBuffer buf1 = MemoryUtils.buffer(16);
    for (int i = 0; i < 32; i++) {
      assertAligned(i, buf1);
    }
  }

  private void assertAligned(int i, MemoryBuffer buffer) {
    for (int j = 0; j < 31; j++) {
      buffer.writeByte((byte) i); // make address unaligned.
      buffer.writePositiveVarIntAligned(1 << j);
      assertEquals(buffer.writerIndex() % 4, 0);
      buffer.readByte();
      assertEquals(buffer.readPositiveAlignedVarInt(), 1 << j);
      for (int k = 0; k < i % 4; k++) {
        buffer.writeByte((byte) i); // make address unaligned.
        buffer.writePositiveVarIntAligned(1 << j);
        assertEquals(buffer.writerIndex() % 4, 0);
        buffer.readByte();
        assertEquals(buffer.readPositiveAlignedVarInt(), 1 << j);
      }
    }
    buffer.writeByte((byte) i); // make address unaligned.
    buffer.writePositiveVarIntAligned(Integer.MAX_VALUE);
    assertEquals(buffer.writerIndex() % 4, 0);
    buffer.readByte();
    assertEquals(buffer.readPositiveAlignedVarInt(), Integer.MAX_VALUE);
  }

  @Test
  public void testGetShortB() {
    byte[] data = new byte[4];
    data[0] = (byte) 0xac;
    data[1] = (byte) 0xed;
    assertEquals(MemoryBuffer.getShortB(data, 0), (short) 0xaced);
    assertEquals(MemoryBuffer.fromByteArray(data).getShortB(0), (short) 0xaced);
  }

  @Test
  public void testWriteSliLong() {
    MemoryBuffer buf = MemoryUtils.buffer(8);
    checkSliLong(buf, -1, 4);
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < i; j++) {
        checkSliLong(buf(i), -1, 4);
        checkSliLong(buf(i), 1, 4);
        checkSliLong(buf(i), 1L << 6, 4);
        checkSliLong(buf(i), 1L << 7, 4);
        checkSliLong(buf(i), -(2 << 5), 4);
        checkSliLong(buf(i), -(2 << 6), 4);
        checkSliLong(buf(i), 1L << 28, 4);
        checkSliLong(buf(i), Integer.MAX_VALUE / 2, 4);
        checkSliLong(buf(i), Integer.MIN_VALUE / 2, 4);
        checkSliLong(buf(i), -1L << 30, 4);
        checkSliLong(buf(i), 1L << 30, 9);
        checkSliLong(buf(i), Integer.MAX_VALUE, 9);
        checkSliLong(buf(i), Integer.MIN_VALUE, 9);
        checkSliLong(buf(i), -1L << 31, 9);
        checkSliLong(buf(i), 1L << 31, 9);
        checkSliLong(buf(i), -1L << 32, 9);
        checkSliLong(buf(i), 1L << 32, 9);
        checkSliLong(buf(i), Long.MAX_VALUE, 9);
        checkSliLong(buf(i), Long.MIN_VALUE, 9);
      }
    }
  }

  private void checkSliLong(MemoryBuffer buf, long value, int bytesWritten) {
    int readerIndex = buf.readerIndex();
    assertEquals(buf.writerIndex(), readerIndex);
    int actualBytesWritten = buf.writeSliLong(value);
    assertEquals(actualBytesWritten, bytesWritten);
    long varLong = buf.readSliLong();
    assertEquals(buf.writerIndex(), buf.readerIndex());
    assertEquals(value, varLong);
    assertEquals(buf.slice(readerIndex, buf.readerIndex() - readerIndex).readSliLong(), value);
  }
}
