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

package org.apache.fury.io;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.fury.exception.DeserializationException;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.util.Platform;

@NotThreadSafe
public class FuryReadableChannel implements FuryStreamReader, ReadableByteChannel {
  private final ReadableByteChannel channel;
  private final MemoryBuffer memoryBuffer;
  private ByteBuffer byteBuffer;

  public FuryReadableChannel(ReadableByteChannel channel) {
    this(channel, ByteBuffer.allocate(4096));
  }

  public FuryReadableChannel(ReadableByteChannel channel, ByteBuffer directBuffer) {
    this.channel = channel;
    this.byteBuffer = directBuffer;

    int offset = directBuffer.arrayOffset() + directBuffer.position();
    this.memoryBuffer = MemoryBuffer.fromByteArray(directBuffer.array(), offset, 0, this);
  }

  @Override
  public int fillBuffer(int minFillSize) {
    try {
      ByteBuffer byteBuf = byteBuffer;
      MemoryBuffer memoryBuf = memoryBuffer;
      int newLimit = byteBuf.position() + minFillSize;
      if (newLimit > byteBuf.capacity()) {
        int newSize =
            newLimit < BUFFER_GROW_STEP_THRESHOLD ? newLimit << 2 : (int) (newLimit * 1.5);
        ByteBuffer newByteBuf = ByteBuffer.allocate(newSize);
        byteBuf.position(0);
        newByteBuf.put(byteBuf);
        byteBuf = byteBuffer = newByteBuf;
        memoryBuf.initHeapBuffer(byteBuf.array(), 0, newLimit);
      } else {
        byteBuf.limit(newLimit);
      }
      int readCount = channel.read(byteBuf);
      memoryBuf.increaseSize(readCount);
      return readCount;
    } catch (IOException e) {
      throw new DeserializationException("Failed to read the provided byte channel", e);
    }
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    int dstRemaining = dst.remaining();
    if (dstRemaining <= 0) {
      return 0;
    }
    MemoryBuffer buf = memoryBuffer;
    int readerIndex = buf.readerIndex();
    int remaining = buf.size() - readerIndex;
    if (remaining <= 0) {
      return -1;
    }
    if (dstRemaining <= remaining) {
      dst.put(buf.getHeapMemory(), readerIndex, remaining);
      return dstRemaining;
    } else {
      dst.put(buf.getHeapMemory(), readerIndex, remaining);
      int filledSize = fillBuffer(remaining - dstRemaining);
      dst.put(buf.getHeapMemory(), remaining, filledSize);
      return remaining + filledSize;
    }
  }

  @Override
  public void readTo(byte[] dst, int dstIndex, int length) {
    MemoryBuffer buf = memoryBuffer;
    int remaining = buf.remaining();
    if (remaining >= length) {
      buf.readBytes(dst, dstIndex, length);
    } else {
      buf.readBytes(dst, dstIndex, remaining);
      try {
        ByteBuffer buffer = ByteBuffer.wrap(dst, dstIndex + remaining, length - remaining);
        channel.read(buffer);
      } catch (IOException e) {
        throw new DeserializationException("Failed to read the provided byte channel", e);
      }
    }
  }

  @Override
  public void readToUnsafe(Object target, long targetPointer, int numBytes) {
    MemoryBuffer buf = memoryBuffer;
    int remaining = buf.remaining();
    if (remaining < numBytes) {
      fillBuffer(numBytes - remaining);
    }
    byte[] heapMemory = buf.getHeapMemory();
    long address = buf.getUnsafeReaderAddress();
    Platform.copyMemory(heapMemory, address, target, targetPointer, numBytes);
    buf.increaseReaderIndex(numBytes);
  }

  @Override
  public void readToByteBuffer(ByteBuffer dst, int length) {
    readToByteBuffer0(dst, length);
  }

  @Override
  public int readToByteBuffer(ByteBuffer dst) {
    return readToByteBuffer0(dst, dst.remaining());
  }

  private int readToByteBuffer0(ByteBuffer dst, int length) {
    MemoryBuffer buf = memoryBuffer;
    int remaining = buf.remaining();
    if (remaining >= length) {
      buf.read(dst, length);
      return length;
    } else {
      buf.read(dst, remaining);
      try {
        if (dst.isDirect()) {
          ByteBuffer tempBuff = ByteBuffer.allocate(length - remaining);
          int read = channel.read(tempBuff);
          dst.put(tempBuff);
          return remaining + read;
        } else {
          int offset = dst.arrayOffset() + dst.position();
          ByteBuffer wrap = ByteBuffer.wrap(dst.array(), offset, length - remaining);
          int read = channel.read(wrap);
          return remaining + read;
        }
      } catch (IOException e) {
        throw new DeserializationException("Failed to read the provided byte channel", e);
      }
    }
  }

  @Override
  public boolean isOpen() {
    return channel.isOpen();
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }

  @Override
  public MemoryBuffer getBuffer() {
    return memoryBuffer;
  }
}
