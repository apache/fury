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
import java.io.InputStream;
import javax.annotation.concurrent.NotThreadSafe;
import org.apache.fury.memory.MemoryBuffer;

/**
 * A buffered stream by fury. Do not use original {@link InputStream} when this stream object
 * created. This stream will try to buffer data inside, the date read from original stream won't be
 * the data you expected. Use this stream as a wrapper instead.
 */
@NotThreadSafe
public class FuryInputStream extends InputStream implements FuryStreamReader {
  private final InputStream stream;
  private final int bufferSize;
  private final MemoryBuffer buffer;

  public FuryInputStream(InputStream stream) {
    this(stream, 4096);
  }

  public FuryInputStream(InputStream stream, int bufferSize) {
    this.stream = stream;
    this.bufferSize = bufferSize;
    this.buffer = MemoryBuffer.newHeapBuffer(bufferSize);
  }

  @Override
  public int fillBuffer(int minFillSize) {
    MemoryBuffer buffer = this.buffer;
    byte[] heapMemory = buffer.getHeapMemory();
    int offset = buffer.size();
    int targetSize = offset + minFillSize;
    if (targetSize > heapMemory.length) {
      if (targetSize < 536870912) {
        buffer.grow(targetSize * 2);
      } else {
        buffer.grow((int) (targetSize * 1.5));
      }
      heapMemory = buffer.getHeapMemory();
    }
    try {
      int read;
      read = stream.read(heapMemory, offset, heapMemory.length);
      while (read < minFillSize) {
        int newRead = stream.read(heapMemory, offset + read, minFillSize - read);
        if (newRead > 0) {
          read += newRead;
        } else {
          return read;
        }
      }
      return read;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public MemoryBuffer getBuffer() {
    return buffer;
  }

  public void clear() {
    int remaining = buffer.remaining();
    if (remaining > bufferSize) {
      byte[] heapMemory = buffer.getHeapMemory();
      byte[] newBuffer = new byte[remaining];
      System.arraycopy(heapMemory, buffer.readerIndex(), newBuffer, 0, remaining);
      buffer.initHeapBuffer(newBuffer, 0, remaining);
    }
  }

  @Override
  public int read() throws IOException {
    MemoryBuffer buf = buffer;
    if (buf.remaining() <= 0) {
      if (fillBuffer(1) == -1) {
        return -1;
      }
    }
    return buf.readByte() & 0xFF;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return read(b, 0, b.length);
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    MemoryBuffer buf = buffer;
    int remaining = buf.remaining();
    if (remaining >= len) {
      buf.readBytes(b, off, len);
      return len;
    } else {
      buf.readBytes(b, off, remaining);
      return stream.read(b, off + remaining, len - remaining) + remaining;
    }
  }

  @Override
  public long skip(long n) throws IOException {
    MemoryBuffer buf = buffer;
    int remaining = buf.remaining();
    if (remaining >= n) {
      buf.increaseReaderIndex((int) n);
      return n;
    } else {
      buf.increaseReaderIndex(remaining);
      return stream.skip(n - remaining) + remaining;
    }
  }

  @Override
  public int available() throws IOException {
    return buffer.remaining() + stream.available();
  }

  @Override
  public void close() throws IOException {
    stream.close();
  }
}
