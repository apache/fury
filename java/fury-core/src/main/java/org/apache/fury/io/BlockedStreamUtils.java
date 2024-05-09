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

import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.serializer.BufferCallback;
import org.apache.fury.util.ExceptionUtils;
import org.apache.fury.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A serialization helper as the fallback of streaming serialization/deserialization in
 * {@link FuryInputStream}/{@link FuryReadableChannel}.
 * {@link FuryInputStream}/{@link FuryReadableChannel} will buffer and read more data, which makes
 * the original passed stream when constructing {@link FuryInputStream} not usable. If this is not possible, use
 * this {@link BlockedStreamUtils} instead for streaming serialization and deserialization.
 * <p>
 * Note that this mode will disable streaming in essence. It's just a helper for make the usage in streaming
 * interface more easily. The deserialization will read whole bytes before do the actual deserialization, which
 * don't have any streaming behaviour under the hood.
 */
public class BlockedStreamUtils {
  public static void serialize(Fury fury, OutputStream outputStream, Object obj) {
    serializeToStream(fury, outputStream, buf -> fury.serialize(buf, obj, null));
  }

  public static void serialize(Fury fury, OutputStream outputStream, Object obj, BufferCallback callback) {
    serializeToStream(fury, outputStream, buf -> fury.serialize(buf, obj, callback));
  }

  /**
   * Serialize java object without class info, deserialization should use {@link
   * #deserializeJavaObject}.
   */
  public static void serializeJavaObject(Fury fury, OutputStream outputStream, Object obj) {
    serializeToStream(fury, outputStream, buf -> fury.serializeJavaObject(buf, obj));
  }

  private static void serializeToStream(Fury fury, OutputStream outputStream, Consumer<MemoryBuffer> function) {
    MemoryBuffer buf = fury.getBuffer();
    if (outputStream.getClass() == ByteArrayOutputStream.class) {
      byte[] oldBytes = buf.getHeapMemory(); // Note: This should not be null.
      MemoryUtils.wrap((ByteArrayOutputStream) outputStream, buf);
      int writerIndex = buf.writerIndex();
      buf.writeInt32(-1);
      function.accept(buf);
      buf.putInt32(writerIndex, buf.writerIndex() - writerIndex);
      MemoryUtils.wrap(buf, (ByteArrayOutputStream) outputStream);
      buf.pointTo(oldBytes, 0, oldBytes.length);
    } else {
      buf.writerIndex(0);
      buf.writeInt32(-1);
      function.accept(buf);
      buf.putInt32(0, buf.writerIndex() - 4);
      try {
        byte[] bytes = buf.getHeapMemory();
        if (bytes != null) {
          outputStream.write(bytes, 0, buf.writerIndex());
        } else {
          outputStream.write(buf.getBytes(0, buf.writerIndex()));
        }
        outputStream.flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        fury.resetBuffer();
      }
    }
  }

  public static Object deserialize(Fury fury, InputStream inputStream) {
    return deserialize(fury, inputStream, null);
  }

  public static Object deserialize(Fury fury, InputStream inputStream, Iterable<MemoryBuffer> outOfBandBuffers) {
    return deserializeFromStream(fury, inputStream, buf -> fury.deserialize(buf, outOfBandBuffers));
  }

  @SuppressWarnings("unchecked")
  public static <T> T deserializeJavaObject(Fury fury, InputStream inputStream, Class<T> cls) {
    return (T) deserializeFromStream(fury, inputStream, buf -> fury.deserializeJavaObject(buf, cls));
  }

  private static Object deserializeFromStream(
    Fury fury, InputStream inputStream, Function<MemoryBuffer, Object> function) {
    MemoryBuffer buf = fury.getBuffer();
    try {
      boolean isBis = inputStream.getClass() == ByteArrayInputStream.class;
      byte[] oldBytes = null;
      if (isBis) {
        buf.readerIndex(0);
        oldBytes = buf.getHeapMemory(); // Note: This should not be null.
        MemoryUtils.wrap((ByteArrayInputStream) inputStream, buf);
        buf.increaseReaderIndex(4); // skip size.
      } else {
        readToBufferFromStream(inputStream, buf);
      }
      Object o = function.apply(buf);
      if (isBis) {
        int i = buf.readerIndex();
        Preconditions.checkArgument(inputStream.skip(i) == i);
        buf.pointTo(oldBytes, 0, oldBytes.length);
      }
      return o;
    } catch (Throwable t) {
      throw ExceptionUtils.handleReadFailed(fury, t);
    } finally {
      fury.resetBuffer();
    }
  }

  private static void readToBufferFromStream(InputStream inputStream, MemoryBuffer buffer)
    throws IOException {
    buffer.readerIndex(0);
    int read = readBytes(inputStream, buffer.getHeapMemory(), 0, 4);
    Preconditions.checkArgument(read == 4);
    int size = buffer.readInt32();
    buffer.ensure(4 + size);
    read = readBytes(inputStream, buffer.getHeapMemory(), 4, size);
    Preconditions.checkArgument(read == size);
  }

  private static int readBytes(InputStream inputStream, byte[] buffer, int offset, int size)
    throws IOException {
    int read = 0;
    int count = 0;
    while (read < size) {
      if ((count = inputStream.read(buffer, offset + read, size - read)) == -1) {
        break;
      }
      read += count;
    }
    return (read == 0 && count == -1) ? -1 : read;
  }
}
