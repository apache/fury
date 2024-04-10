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
import org.apache.fury.util.Preconditions;

@NotThreadSafe
public class FuryReadableChannel implements FuryStreamReader, ReadableByteChannel {
  private final ReadableByteChannel channel;
  private final MemoryBuffer memoryBuffer;
  private ByteBuffer byteBuffer;

  public FuryReadableChannel(ReadableByteChannel channel) {
    this(channel, ByteBuffer.allocateDirect(4096));
  }

  public FuryReadableChannel(ReadableByteChannel channel, ByteBuffer directBuffer) {
    Preconditions.checkArgument(
        directBuffer.isDirect(), "FuryReadableChannel support only direct ByteBuffer.");
    this.channel = channel;
    this.byteBuffer = directBuffer;
    this.memoryBuffer = MemoryBuffer.fromDirectByteBuffer(directBuffer, 0, this);
  }

  @Override
  public int fillBuffer(int minFillSize) {
    try {
      ByteBuffer byteBuf = byteBuffer;
      MemoryBuffer memoryBuf = memoryBuffer;
      int position = byteBuf.position();
      int newLimit = position + minFillSize;
      if (newLimit > byteBuf.capacity()) {
        int newSize =
            newLimit < BUFFER_GROW_STEP_THRESHOLD ? newLimit << 2 : (int) (newLimit * 1.5);
        ByteBuffer newByteBuf = ByteBuffer.allocateDirect(newSize);
        byteBuf.position(0);
        newByteBuf.put(byteBuf);
        byteBuf = byteBuffer = newByteBuf;
        memoryBuf.initDirectBuffer(Platform.getAddress(byteBuf), position, byteBuf);
      }
      byteBuf.limit(newLimit);
      int readCount = channel.read(byteBuf);
      memoryBuf.increaseSize(readCount);
      return readCount;
    } catch (IOException e) {
      throw new DeserializationException("Failed to read the provided byte channel", e);
    }
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    int length = dst.remaining();
    MemoryBuffer buf = memoryBuffer;
    int remaining = buf.remaining();
    if (remaining >= length) {
      buf.read(dst, length);
      return length;
    } else {
      buf.read(dst, remaining);
      return channel.read(dst) + remaining;
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
    long address = buf.getUnsafeReaderAddress();
    Platform.copyMemory(null, address, target, targetPointer, numBytes);
    buf.increaseReaderIndex(numBytes);
  }

  @Override
  public void readToByteBuffer(ByteBuffer dst, int length) {
    MemoryBuffer buf = memoryBuffer;
    int remaining = buf.remaining();
    if (remaining >= length) {
      buf.read(dst, length);
    } else {
      buf.read(dst, remaining);
      try {
        int dstLimit = dst.limit();
        int newLimit = dst.position() + length - remaining;
        if (dstLimit > newLimit) {
          dst.limit(newLimit);
          channel.read(dst);
          dst.limit(dstLimit);
        } else {
          channel.read(dst);
        }
      } catch (IOException e) {
        throw new DeserializationException("Failed to read the provided byte channel", e);
      }
    }
  }

  @Override
  public int readToByteBuffer(ByteBuffer dst) {
    MemoryBuffer buf = memoryBuffer;
    int remaining = buf.remaining();
    if (remaining > 0) {
      buf.read(dst, remaining);
    }
    try {
      return channel.read(dst) + remaining;
    } catch (IOException e) {
      throw new DeserializationException("Failed to read the provided byte channel", e);
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
