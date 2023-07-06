/*
 * Copyright 2023 The Fury authors
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

package io.fury.memory;

import com.google.common.base.Preconditions;
import io.fury.util.Platform;
import io.fury.util.ReflectionUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class MemoryUtils {

  public static MemoryBuffer buffer(int size) {
    return wrap(new byte[size]);
  }

  public static MemoryBuffer buffer(long address, int size) {
    return MemoryBuffer.fromNativeAddress(address, size);
  }

  /**
   * Creates a new memory segment that targets to the given heap memory region.
   *
   * <p>This method should be used to turn short lived byte arrays into memory segments.
   *
   * @param buffer The heap memory region.
   * @return A new memory segment that targets the given heap memory region.
   */
  public static MemoryBuffer wrap(byte[] buffer, int offset, int length) {
    return MemoryBuffer.fromByteArray(buffer, offset, length);
  }

  public static MemoryBuffer wrap(byte[] buffer) {
    return MemoryBuffer.fromByteArray(buffer);
  }

  /**
   * Creates a new memory segment that represents the memory backing the given byte buffer section
   * of [buffer.position(), buffer,limit()).
   *
   * @param buffer a direct buffer or heap buffer
   */
  public static MemoryBuffer wrap(ByteBuffer buffer) {
    if (buffer.isDirect()) {
      return MemoryBuffer.fromByteBuffer(buffer);
    } else {
      int offset = buffer.arrayOffset() + buffer.position();
      return MemoryBuffer.fromByteArray(buffer.array(), offset, buffer.remaining());
    }
  }

  private static final long BAS_BUF_BUF =
      ReflectionUtils.getFieldOffsetChecked(ByteArrayOutputStream.class, "buf");
  private static final long BAS_BUF_COUNT =
      ReflectionUtils.getFieldOffsetChecked(ByteArrayOutputStream.class, "count");

  private static final long BIS_BUF_BUF =
      ReflectionUtils.getFieldOffsetChecked(ByteArrayInputStream.class, "buf");
  private static final long BIS_BUF_POS =
      ReflectionUtils.getFieldOffsetChecked(ByteArrayInputStream.class, "pos");
  private static final long BIS_BUF_COUNT =
      ReflectionUtils.getFieldOffsetChecked(ByteArrayInputStream.class, "count");

  /**
   * Wrap a {@link ByteArrayOutputStream} into a {@link MemoryBuffer}. The writerIndex of buffer
   * will be the count of stream.
   */
  public static void wrap(ByteArrayOutputStream stream, MemoryBuffer buffer) {
    Preconditions.checkNotNull(stream);
    byte[] buf = (byte[]) Platform.getObject(stream, BAS_BUF_BUF);
    int count = Platform.getInt(stream, BAS_BUF_COUNT);
    buffer.pointTo(buf, 0, buf.length);
    buffer.writerIndex(count);
  }

  /**
   * Wrap a @link MemoryBuffer} into a {@link ByteArrayOutputStream}. The count of stream will be
   * the writerIndex of buffer.
   */
  public static void wrap(MemoryBuffer buffer, ByteArrayOutputStream stream) {
    Preconditions.checkNotNull(stream);
    byte[] bytes = buffer.getHeapMemory();
    Preconditions.checkNotNull(bytes);
    Platform.putObject(stream, BAS_BUF_BUF, bytes);
    Platform.putInt(stream, BAS_BUF_COUNT, buffer.writerIndex());
  }

  /**
   * Wrap a {@link ByteArrayInputStream} into a {@link MemoryBuffer}. The readerIndex of buffer will
   * be the pos of stream.
   */
  public static void wrap(ByteArrayInputStream stream, MemoryBuffer buffer) {
    Preconditions.checkNotNull(stream);
    byte[] buf = (byte[]) Platform.getObject(stream, BIS_BUF_BUF);
    int count = Platform.getInt(stream, BIS_BUF_COUNT);
    int pos = Platform.getInt(stream, BIS_BUF_POS);
    buffer.pointTo(buf, 0, count);
    buffer.readerIndex(pos);
  }

  public static int writePositiveVarInt(byte[] arr, int index, int v) {
    // The encoding algorithm are based on kryo UnsafeMemoryOutput.writeVarInt
    // varint are written using little endian byte order.
    if (v >>> 7 == 0) {
      arr[index] = (byte)v;
      return 1;
    }
    if (v >>> 14 == 0) {
      arr[index++] = (byte)((v & 0x7F) | 0x80);
      arr[index] = (byte)(v >>> 7);
      return 2;
    }
    if (v >>> 21 == 0) {
      arr[index++] = (byte)((v & 0x7F) | 0x80);
      arr[index++] = (byte)(v >>> 7 | 0x80);
      arr[index] = (byte)(v >>> 14);
      return 3;
    }
    if (v >>> 28 == 0) {
      arr[index++] = (byte)((v & 0x7F) | 0x80);
      arr[index++] = (byte)(v >>> 7 | 0x80);
      arr[index++] = (byte)(v >>> 14 | 0x80);
      arr[index] = (byte)(v >>> 21);
      return 4;
    }
    arr[index++] = (byte)((v & 0x7F) | 0x80);
    arr[index++] = (byte)(v >>> 7 | 0x80);
    arr[index++] = (byte)(v >>> 14 | 0x80);
    arr[index++] = (byte)(v >>> 21 | 0x80);
    arr[index] = (byte)(v >>> 28);
    return 5;
  }

}
