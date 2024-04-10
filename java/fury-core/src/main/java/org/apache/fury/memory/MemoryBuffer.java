/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fury.memory;

import static org.apache.fury.util.Preconditions.checkArgument;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ReadOnlyBufferException;
import java.util.Arrays;
import org.apache.fury.annotation.CodegenInvoke;
import org.apache.fury.io.AbstractStreamReader;
import org.apache.fury.io.FuryStreamReader;
import org.apache.fury.util.Platform;
import sun.misc.Unsafe;

/**
 * A class for operations on memory managed by Fury. The buffer may be backed by heap memory (byte
 * array) or by off-heap memory. Note that the buffer can auto grow on write operations and change
 * into a heap buffer when growing.
 *
 * <p>This is a byte buffer similar class with more features:
 *
 * <ul>
 *   <li>read/write data into a chunk of direct memory.
 *   <li>additional binary compare, swap, and copy methods.
 *   <li>little-endian access.
 *   <li>independent read/write index.
 *   <li>variant int/long encoding.
 *   <li>aligned int/long encoding.
 * </ul>
 *
 * <p>Note that this class is designed to final so that all the methods in this class can be inlined
 * by the just-in-time compiler.
 *
 * <p>TODO(chaokunyang) Let grow/readerIndex/writerIndex handled in this class and Make immutable
 * part as separate class, and use composition in this class. In this way, all fields can be final
 * and access will be much faster.
 *
 * <p>Warning: The instance of this class should not be hold on graalvm build time, the heap unsafe
 * offset are not correct in runtime since graalvm will change array base offset.
 *
 * <p>Note(chaokunyang): Buffer operations are very common, and jvm inline and branch elimination is
 * not reliable even in c2 compiler, so we try to inline and avoid checks as we can manually. jvm
 * jit may stop inline for some reasons: NodeCountInliningCutoff,
 * DesiredMethodLimit,MaxRecursiveInlineLevel,FreqInlineSize,MaxInlineSize
 */
//  DesiredMethodLimit,MaxRecursiveInlineLevel,FreqInlineSize,MaxInlineSize
public final class MemoryBuffer {
  // The unsafe handle for transparent memory copied (heap/off-heap).
  private static final Unsafe UNSAFE = Platform.UNSAFE;
  // Constant that flags the byte order. Because this is a boolean constant, the JIT compiler can
  // use this well to aggressively eliminate the non-applicable code paths.
  private static final boolean LITTLE_ENDIAN = (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN);

  // If the data in on the heap, `heapMemory` will be non-null, and its' the object relative to
  // which we access the memory.
  // If we have this buffer, we must never void this reference, or the memory buffer will point
  // to undefined addresses outside the heap and may in out-of-order execution cases cause
  // buffer faults.
  private byte[] heapMemory;
  private int heapOffset;
  // If the data is off the heap, `offHeapBuffer` will be non-null, and it's the direct byte buffer
  // that allocated on the off-heap memory.
  // This memory buffer holds a reference to that buffer, so as long as this memory buffer lives,
  // the memory will not be released.
  private ByteBuffer offHeapBuffer;
  // The readable/writeable range is [address, addressLimit).
  // If the data in on the heap, this is the relative offset to the `heapMemory` byte array.
  // If the data is off the heap, this is the absolute memory address.
  private long address;
  // The address one byte after the last addressable byte, i.e. `address + size` while the
  // buffer is not disposed.
  private long addressLimit;
  // The size in bytes of the memory buffer.
  private int size;
  private int readerIndex;
  private int writerIndex;
  private final FuryStreamReader streamReader;

  /**
   * Creates a new memory buffer that represents the memory of the byte array.
   *
   * @param buffer The byte array whose memory is represented by this memory buffer.
   * @param offset The offset of the sub array to be used; must be non-negative and no larger than
   *     <tt>array.length</tt>.
   * @param length buffer size
   */
  private MemoryBuffer(byte[] buffer, int offset, int length) {
    this(buffer, offset, length, null);
  }

  /**
   * Creates a new memory buffer that represents the memory of the byte array.
   *
   * @param buffer The byte array whose memory is represented by this memory buffer.
   * @param offset The offset of the sub array to be used; must be non-negative and no larger than
   *     <tt>array.length</tt>.
   * @param length buffer size
   * @param streamReader a reader for reading from a stream.
   */
  private MemoryBuffer(byte[] buffer, int offset, int length, FuryStreamReader streamReader) {
    checkArgument(offset >= 0 && length >= 0);
    if (offset + length > buffer.length) {
      throw new IllegalArgumentException(
          String.format("%d exceeds buffer size %d", offset + length, buffer.length));
    }
    initHeapBuffer(buffer, offset, length);
    if (streamReader != null) {
      this.streamReader = streamReader;
    } else {
      this.streamReader = new BoundChecker();
    }
  }

  /**
   * Creates a new memory buffer that represents the native memory at the absolute address given by
   * the pointer.
   *
   * @param offHeapAddress The address of the memory represented by this memory buffer.
   * @param size The size of this memory buffer.
   * @param offHeapBuffer The byte buffer whose memory is represented by this memory buffer which
   *     may be null if the memory is not allocated by `DirectByteBuffer`. Hold this buffer to avoid
   *     the memory being released.
   */
  private MemoryBuffer(long offHeapAddress, int size, ByteBuffer offHeapBuffer) {
    this(offHeapAddress, size, offHeapBuffer, null);
  }

  /**
   * Creates a new memory buffer that represents the native memory at the absolute address given by
   * the pointer.
   *
   * @param offHeapAddress The address of the memory represented by this memory buffer.
   * @param size The size of this memory buffer.
   * @param offHeapBuffer The byte buffer whose memory is represented by this memory buffer which
   *     may be null if the memory is not allocated by `DirectByteBuffer`. Hold this buffer to avoid
   *     the memory being released.
   * @param streamReader a reader for reading from a stream.
   */
  private MemoryBuffer(
      long offHeapAddress, int size, ByteBuffer offHeapBuffer, FuryStreamReader streamReader) {
    initDirectBuffer(offHeapAddress, size, offHeapBuffer);
    if (streamReader != null) {
      this.streamReader = streamReader;
    } else {
      this.streamReader = new BoundChecker();
    }
  }

  public void initDirectBuffer(long offHeapAddress, int size, ByteBuffer offHeapBuffer) {
    this.offHeapBuffer = offHeapBuffer;
    if (offHeapAddress <= 0) {
      throw new IllegalArgumentException("negative pointer or size");
    }
    if (offHeapAddress >= Long.MAX_VALUE - Integer.MAX_VALUE) {
      // this is necessary to make sure the collapsed checks are safe against numeric overflows
      throw new IllegalArgumentException(
          "Buffer initialized with too large address: "
              + offHeapAddress
              + " ; Max allowed address is "
              + (Long.MAX_VALUE - Integer.MAX_VALUE - 1));
    }

    this.heapMemory = null;
    this.address = offHeapAddress;
    this.addressLimit = this.address + size;
    this.size = size;
  }

  private class BoundChecker extends AbstractStreamReader {
    @Override
    public int fillBuffer(int minFillSize) {
      throw new IndexOutOfBoundsException(
          String.format(
              "readerIndex(%d) + length(%d) exceeds size(%d): %s",
              readerIndex, minFillSize, size, this));
    }

    @Override
    public MemoryBuffer getBuffer() {
      return MemoryBuffer.this;
    }
  }

  public void initHeapBuffer(byte[] buffer, int offset, int length) {
    if (buffer == null) {
      throw new NullPointerException("buffer");
    }
    this.heapMemory = buffer;
    this.heapOffset = offset;
    final long startPos = Platform.BYTE_ARRAY_OFFSET + offset;
    this.address = startPos;
    this.size = length;
    this.addressLimit = startPos + length;
  }

  // ------------------------------------------------------------------------
  // Memory buffer Operations
  // ------------------------------------------------------------------------

  /**
   * Gets the size of the memory buffer, in bytes.
   *
   * @return The size of the memory buffer.
   */
  public int size() {
    return size;
  }

  public void increaseSize(int diff) {
    this.addressLimit = address + (size += diff);
  }

  /**
   * Checks whether this memory buffer is backed by off-heap memory.
   *
   * @return <tt>true</tt>, if the memory buffer is backed by off-heap memory, <tt>false</tt> if it
   *     is backed by heap memory.
   */
  public boolean isOffHeap() {
    return heapMemory == null;
  }

  /**
   * Get the heap byte array object.
   *
   * @return Return non-null if the memory is on the heap, and return null, if the memory if off the
   *     heap.
   */
  public byte[] getHeapMemory() {
    return heapMemory;
  }

  /**
   * Gets the buffer that owns the memory of this memory buffer.
   *
   * @return The byte buffer that owns the memory of this memory buffer.
   */
  public ByteBuffer getOffHeapBuffer() {
    if (offHeapBuffer != null) {
      return offHeapBuffer;
    } else {
      throw new IllegalStateException("Memory buffer does not represent off heap ByteBuffer");
    }
  }

  /**
   * Returns the byte array of on-heap memory buffers.
   *
   * @return underlying byte array
   * @throws IllegalStateException if the memory buffer does not represent on-heap memory
   */
  public byte[] getArray() {
    if (heapMemory != null) {
      return heapMemory;
    } else {
      throw new IllegalStateException("Memory buffer does not represent heap memory");
    }
  }

  /**
   * Returns the memory address of off-heap memory buffers.
   *
   * @return absolute memory address outside the heap
   * @throws IllegalStateException if the memory buffer does not represent off-heap memory
   */
  public long getAddress() {
    if (heapMemory == null) {
      return address;
    } else {
      throw new IllegalStateException("Memory buffer does not represent off heap memory");
    }
  }

  public long getUnsafeAddress() {
    return address;
  }

  // ------------------------------------------------------------------------
  //                    Random Access get() and put() methods
  // ------------------------------------------------------------------------

  // ------------------------------------------------------------------------
  // Notes on the implementation: We try to collapse as many checks as
  // possible. We need to obey the following rules to make this safe
  // against segfaults:
  //
  //  - Grab mutable fields onto the stack before checking and using. This
  //    guards us against concurrent modifications which invalidate the
  //    pointers
  //  - Use subtractions for range checks, as they are tolerant
  // ------------------------------------------------------------------------

  private void checkPosition(long index, long pos, long length) {
    if (BoundsChecking.BOUNDS_CHECKING_ENABLED) {
      if (index < 0 || pos > addressLimit - length) {
        // index is in fact invalid
        throwOOBException();
      }
    }
  }

  public byte unsafeGet(int index) {
    final long pos = address + index;
    return UNSAFE.getByte(heapMemory, pos);
  }

  public byte get(int index) {
    final long pos = address + index;
    checkPosition(index, pos, 1);
    return UNSAFE.getByte(heapMemory, pos);
  }

  public void get(int index, byte[] dst) {
    get(index, dst, 0, dst.length);
  }

  /**
   * Bulk get method. Copies length memory from the specified position to the destination memory,
   * beginning at the given offset.
   *
   * @param index The position at which the first byte will be read.
   * @param dst The memory into which the memory will be copied.
   * @param offset The copying offset in the destination memory.
   * @param length The number of bytes to be copied.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large that the
   *     requested number of bytes exceed the amount of memory between the index and the memory
   *     buffer's end.
   */
  public void get(int index, byte[] dst, int offset, int length) {
    // check the byte array offset and length and the status
    if ((offset | length | (offset + length) | (dst.length - (offset + length))) < 0) {
      throwOOBException();
    }
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - length) {
      final long arrayAddress = Platform.BYTE_ARRAY_OFFSET + offset;
      Platform.copyMemory(heapMemory, pos, dst, arrayAddress, length);
    } else {
      // index is in fact invalid
      throwOOBException();
    }
  }

  /**
   * Bulk get method. Copies {@code numBytes} bytes from this memory buffer, starting at position
   * {@code offset} to the target {@code ByteBuffer}. The bytes will be put into the target buffer
   * starting at the buffer's current position. If this method attempts to write more bytes than the
   * target byte buffer has remaining (with respect to {@link ByteBuffer#remaining()}), this method
   * will cause a {@link BufferOverflowException}.
   *
   * @param offset The position where the bytes are started to be read from in this memory buffer.
   * @param target The ByteBuffer to copy the bytes to.
   * @param numBytes The number of bytes to copy.
   * @throws IndexOutOfBoundsException If the offset is invalid, or this buffer does not contain the
   *     given number of bytes (starting from offset), or the target byte buffer does not have
   *     enough space for the bytes.
   * @throws ReadOnlyBufferException If the target buffer is read-only.
   */
  public void get(int offset, ByteBuffer target, int numBytes) {
    // check the byte array offset and length
    if ((offset | numBytes | (offset + numBytes)) < 0) {
      throwOOBException();
    }
    final int targetOffset = target.position();
    final int remaining = target.remaining();
    if (remaining < numBytes) {
      throwOOBException();
    }
    if (target.isDirect()) {
      if (target.isReadOnly()) {
        throwOOBException();
      }
      // copy to the target memory directly
      final long targetPointer = Platform.getAddress(target) + targetOffset;
      final long sourcePointer = address + offset;
      if (sourcePointer <= addressLimit - numBytes) {
        Platform.copyMemory(heapMemory, sourcePointer, null, targetPointer, numBytes);
        target.position(targetOffset + numBytes);
      } else {
        throwOOBException();
      }
    } else if (target.hasArray()) {
      // move directly into the byte array
      get(offset, target.array(), targetOffset + target.arrayOffset(), numBytes);
      // this must be after the get() call to ensue that the byte buffer is not
      // modified in case the call fails
      target.position(targetOffset + numBytes);
    } else {
      // neither heap buffer nor direct buffer
      while (target.hasRemaining()) {
        target.put(get(offset++));
      }
    }
  }

  public static void unsafePut(Object o, long offset, byte b) {
    UNSAFE.putByte(o, offset, b);
  }

  public void unsafePut(int index, byte b) {
    final long pos = address + index;
    UNSAFE.putByte(heapMemory, pos, b);
  }

  /**
   * Bulk put method. Copies {@code numBytes} bytes from the given {@code ByteBuffer}, into this
   * memory buffer. The bytes will be read from the target buffer starting at the buffer's current
   * position, and will be written to this memory buffer starting at {@code offset}. If this method
   * attempts to read more bytes than the target byte buffer has remaining (with respect to {@link
   * ByteBuffer#remaining()}), this method will cause a {@link BufferUnderflowException}.
   *
   * @param offset The position where the bytes are started to be written to in this memory buffer.
   * @param source The ByteBuffer to copy the bytes from.
   * @param numBytes The number of bytes to copy.
   * @throws IndexOutOfBoundsException If the offset is invalid, or the source buffer does not
   *     contain the given number of bytes, or this buffer does not have enough space for the
   *     bytes(counting from offset).
   */
  public void put(int offset, ByteBuffer source, int numBytes) {
    // check the byte array offset and length
    if ((offset | numBytes | (offset + numBytes)) < 0) {
      throwOOBException();
    }
    final int sourceOffset = source.position();
    final int remaining = source.remaining();
    if (remaining < numBytes) {
      throwOOBException();
    }
    if (source.isDirect()) {
      // copy to the target memory directly
      final long sourcePointer = Platform.getAddress(source) + sourceOffset;
      final long targetPointer = address + offset;
      if (targetPointer <= addressLimit - numBytes) {
        Platform.copyMemory(null, sourcePointer, heapMemory, targetPointer, numBytes);
        source.position(sourceOffset + numBytes);
      } else {
        throwOOBException();
      }
    } else if (source.hasArray()) {
      // move directly into the byte array
      put(offset, source.array(), sourceOffset + source.arrayOffset(), numBytes);
      // this must be after the get() call to ensue that the byte buffer is not
      // modified in case the call fails
      source.position(sourceOffset + numBytes);
    } else {
      // neither heap buffer nor direct buffer
      while (source.hasRemaining()) {
        put(offset++, source.get());
      }
    }
  }

  public void put(int index, byte b) {
    final long pos = address + index;
    checkPosition(index, pos, 1);
    UNSAFE.putByte(heapMemory, pos, b);
  }

  public void put(int index, byte[] src) {
    put(index, src, 0, src.length);
  }

  /**
   * Bulk put method. Copies length memory starting at position offset from the source memory into
   * the memory buffer starting at the specified index.
   *
   * @param index The position in the memory buffer array, where the data is put.
   * @param src The source array to copy the data from.
   * @param offset The offset in the source array where the copying is started.
   * @param length The number of bytes to copy.
   * @throws IndexOutOfBoundsException Thrown, if the index is negative, or too large such that the
   *     array portion to copy exceed the amount of memory between the index and the memory buffer's
   *     end.
   */
  public void put(int index, byte[] src, int offset, int length) {
    // check the byte array offset and length
    if ((offset | length | (offset + length) | (src.length - (offset + length))) < 0) {
      throwOOBException();
    }
    final long pos = address + index;
    if (index >= 0 && pos <= addressLimit - length) {
      final long arrayAddress = Platform.BYTE_ARRAY_OFFSET + offset;
      Platform.copyMemory(src, arrayAddress, heapMemory, pos, length);
    } else {
      // index is in fact invalid
      throwOOBException();
    }
  }

  public boolean getBoolean(int index) {
    return get(index) != 0;
  }

  public void putBoolean(int index, boolean value) {
    put(index, (byte) (value ? 1 : 0));
  }

  public void unsafePutBoolean(int index, boolean value) {
    unsafePut(index, (byte) (value ? 1 : 0));
  }

  public static void unsafePutBoolean(Object o, long offset, boolean value) {
    UNSAFE.putBoolean(o, offset, value);
  }

  public char getCharN(int index) {
    final long pos = address + index;
    checkPosition(index, pos, 2);
    return UNSAFE.getChar(heapMemory, pos);
  }

  public char getChar(int index) {
    if (LITTLE_ENDIAN) {
      return getCharN(index);
    } else {
      return Character.reverseBytes(getCharN(index));
    }
  }

  public void putCharN(int index, char value) {
    final long pos = address + index;
    checkPosition(index, pos, 2);
    UNSAFE.putChar(heapMemory, pos, value);
  }

  public void putChar(int index, char value) {
    if (LITTLE_ENDIAN) {
      putCharN(index, value);
    } else {
      putCharN(index, Character.reverseBytes(value));
    }
  }

  public void unsafePutChar(int index, char value) {
    final long pos = address + index;
    if (LITTLE_ENDIAN) {
      UNSAFE.putChar(heapMemory, pos, value);
    } else {
      UNSAFE.putChar(heapMemory, pos, Character.reverseBytes(value));
    }
  }

  public static void unsafePutChar(Object o, long offset, char value) {
    if (LITTLE_ENDIAN) {
      UNSAFE.putChar(o, offset, value);
    } else {
      UNSAFE.putChar(o, offset, Character.reverseBytes(value));
    }
  }

  /** Get short in big endian order from provided buffer. */
  public static short getShortB(byte[] b, int off) {
    return (short) ((b[off + 1] & 0xFF) + (b[off] << 8));
  }

  /** Get short in big endian order from specified offset. */
  public short getShortB(int index) {
    final long pos = address + index;
    checkPosition(index, pos, 2);
    if (LITTLE_ENDIAN) {
      return Short.reverseBytes(UNSAFE.getShort(heapMemory, pos));
    } else {
      return UNSAFE.getShort(heapMemory, pos);
    }
  }

  public short getShort(int index) {
    final long pos = address + index;
    checkPosition(index, pos, 2);
    if (LITTLE_ENDIAN) {
      return UNSAFE.getShort(heapMemory, pos);
    } else {
      return Short.reverseBytes(UNSAFE.getShort(heapMemory, pos));
    }
  }

  public void putShort(int index, short value) {
    final long pos = address + index;
    checkPosition(index, pos, 2);
    if (LITTLE_ENDIAN) {
      UNSAFE.putShort(heapMemory, pos, value);
    } else {
      UNSAFE.putShort(heapMemory, pos, Short.reverseBytes(value));
    }
  }

  public short unsafeGetShort(int index) {
    final long pos = address + index;
    if (LITTLE_ENDIAN) {
      return UNSAFE.getShort(heapMemory, pos);
    } else {
      return Short.reverseBytes(UNSAFE.getShort(heapMemory, pos));
    }
  }

  public void unsafePutShort(int index, short value) {
    final long pos = address + index;
    if (LITTLE_ENDIAN) {
      UNSAFE.putShort(heapMemory, pos, value);
    } else {
      UNSAFE.putShort(heapMemory, pos, Short.reverseBytes(value));
    }
  }

  public static void unsafePutShort(Object o, long pos, short value) {
    if (LITTLE_ENDIAN) {
      UNSAFE.putShort(o, pos, value);
    } else {
      UNSAFE.putShort(o, pos, Short.reverseBytes(value));
    }
  }

  public int getInt(int index) {
    final long pos = address + index;
    checkPosition(index, pos, 4);
    if (LITTLE_ENDIAN) {
      return UNSAFE.getInt(heapMemory, pos);
    } else {
      return Integer.reverseBytes(UNSAFE.getInt(heapMemory, pos));
    }
  }

  public void putInt(int index, int value) {
    final long pos = address + index;
    checkPosition(index, pos, 4);
    if (LITTLE_ENDIAN) {
      UNSAFE.putInt(heapMemory, pos, value);
    } else {
      UNSAFE.putInt(heapMemory, pos, Integer.reverseBytes(value));
    }
  }

  public int unsafeGetInt(int index) {
    final long pos = address + index;
    if (LITTLE_ENDIAN) {
      return UNSAFE.getInt(heapMemory, pos);
    } else {
      return Integer.reverseBytes(UNSAFE.getInt(heapMemory, pos));
    }
  }

  public static int unsafeGetInt(Object o, long pos) {
    if (LITTLE_ENDIAN) {
      return UNSAFE.getInt(o, pos);
    } else {
      return Integer.reverseBytes(UNSAFE.getInt(o, pos));
    }
  }

  public void unsafePutInt(int index, int value) {
    final long pos = address + index;
    if (LITTLE_ENDIAN) {
      UNSAFE.putInt(heapMemory, pos, value);
    } else {
      UNSAFE.putInt(heapMemory, pos, Integer.reverseBytes(value));
    }
  }

  public static void unsafePutInt(Object o, long pos, int value) {
    if (LITTLE_ENDIAN) {
      UNSAFE.putInt(o, pos, value);
    } else {
      UNSAFE.putInt(o, pos, Integer.reverseBytes(value));
    }
  }

  public long getLong(int index) {
    final long pos = address + index;
    checkPosition(index, pos, 8);
    if (LITTLE_ENDIAN) {
      return UNSAFE.getLong(heapMemory, pos);
    } else {
      return Long.reverseBytes(UNSAFE.getLong(heapMemory, pos));
    }
  }

  public long getLongB(int index) {
    final long pos = address + index;
    checkPosition(index, pos, 8);
    if (LITTLE_ENDIAN) {
      return Long.reverseBytes(UNSAFE.getLong(heapMemory, pos));
    } else {
      return UNSAFE.getLong(heapMemory, pos);
    }
  }

  public void putLongN(int index, long value) {
    final long pos = address + index;
    checkPosition(index, pos, 8);
    UNSAFE.putLong(heapMemory, pos, value);
  }

  public void putLong(int index, long value) {
    final long pos = address + index;
    checkPosition(index, pos, 8);
    if (LITTLE_ENDIAN) {
      UNSAFE.putLong(heapMemory, pos, value);
    } else {
      UNSAFE.putLong(heapMemory, pos, Long.reverseBytes(value));
    }
  }

  public void putLongB(int index, long value) {
    if (LITTLE_ENDIAN) {
      putLongN(index, Long.reverseBytes(value));
    } else {
      putLongN(index, value);
    }
  }

  public long unsafeGetLong(int index) {
    final long pos = address + index;
    if (LITTLE_ENDIAN) {
      return UNSAFE.getLong(heapMemory, pos);
    } else {
      return Long.reverseBytes(UNSAFE.getLong(heapMemory, pos));
    }
  }

  public static long unsafeGetLong(Object o, long pos) {
    if (LITTLE_ENDIAN) {
      return UNSAFE.getLong(o, pos);
    } else {
      return Long.reverseBytes(UNSAFE.getLong(o, pos));
    }
  }

  public void unsafePutLong(int index, long value) {
    final long pos = address + index;
    if (LITTLE_ENDIAN) {
      UNSAFE.putLong(heapMemory, pos, value);
    } else {
      UNSAFE.putLong(heapMemory, pos, Long.reverseBytes(value));
    }
  }

  public static void unsafePutLong(Object o, long pos, long value) {
    if (LITTLE_ENDIAN) {
      UNSAFE.putLong(o, pos, value);
    } else {
      UNSAFE.putLong(o, pos, Long.reverseBytes(value));
    }
  }

  public float getFloat(int index) {
    final long pos = address + index;
    if (LITTLE_ENDIAN) {
      return Float.intBitsToFloat(UNSAFE.getInt(heapMemory, pos));
    } else {
      return Float.intBitsToFloat(Integer.reverseBytes(UNSAFE.getInt(heapMemory, pos)));
    }
  }

  public void putFloat(int index, float value) {
    final long pos = address + index;
    checkPosition(index, pos, 4);
    if (LITTLE_ENDIAN) {
      UNSAFE.putInt(heapMemory, pos, Float.floatToRawIntBits(value));
    } else {
      UNSAFE.putInt(heapMemory, pos, Integer.reverseBytes(Float.floatToRawIntBits(value)));
    }
  }

  public float unsafeGetFloat(int index) {
    final long pos = address + index;
    if (LITTLE_ENDIAN) {
      return Float.intBitsToFloat(UNSAFE.getInt(heapMemory, pos));
    } else {
      return Float.intBitsToFloat(Integer.reverseBytes(UNSAFE.getInt(heapMemory, pos)));
    }
  }

  public void unsafePutFloat(int index, float value) {
    final long pos = address + index;
    if (LITTLE_ENDIAN) {
      UNSAFE.putInt(heapMemory, pos, Float.floatToRawIntBits(value));
    } else {
      UNSAFE.putInt(heapMemory, pos, Integer.reverseBytes(Float.floatToRawIntBits(value)));
    }
  }

  public static void unsafePutFloat(Object o, long pos, float value) {
    if (LITTLE_ENDIAN) {
      UNSAFE.putInt(o, pos, Float.floatToRawIntBits(value));
    } else {
      UNSAFE.putInt(o, pos, Integer.reverseBytes(Float.floatToRawIntBits(value)));
    }
  }

  public double getDouble(int index) {
    final long pos = address + index;
    checkPosition(index, pos, 8);
    if (LITTLE_ENDIAN) {
      return Double.longBitsToDouble(UNSAFE.getLong(heapMemory, pos));
    } else {
      return Double.longBitsToDouble(Long.reverseBytes(UNSAFE.getLong(heapMemory, pos)));
    }
  }

  public void putDouble(int index, double value) {
    final long pos = address + index;
    checkPosition(index, pos, 8);
    if (LITTLE_ENDIAN) {
      UNSAFE.putLong(heapMemory, pos, Double.doubleToRawLongBits(value));
    } else {
      UNSAFE.putLong(heapMemory, pos, Long.reverseBytes(Double.doubleToRawLongBits(value)));
    }
  }

  public double unsafeGetDouble(int index) {
    final long pos = address + index;
    if (LITTLE_ENDIAN) {
      return Double.longBitsToDouble(UNSAFE.getLong(heapMemory, pos));
    } else {
      return Double.longBitsToDouble(Long.reverseBytes(UNSAFE.getLong(heapMemory, pos)));
    }
  }

  public void unsafePutDouble(int index, double value) {
    final long pos = address + index;
    if (LITTLE_ENDIAN) {
      UNSAFE.putLong(heapMemory, pos, Double.doubleToRawLongBits(value));
    } else {
      UNSAFE.putLong(heapMemory, pos, Long.reverseBytes(Double.doubleToRawLongBits(value)));
    }
  }

  public static void unsafePutDouble(Object o, long pos, double value) {
    if (LITTLE_ENDIAN) {
      UNSAFE.putLong(o, pos, Double.doubleToRawLongBits(value));
    } else {
      UNSAFE.putLong(o, pos, Long.reverseBytes(Double.doubleToRawLongBits(value)));
    }
  }

  // -------------------------------------------------------------------------
  //                     Read and Write Methods
  // -------------------------------------------------------------------------

  /** Returns the {@code readerIndex} of this buffer. */
  public int readerIndex() {
    return readerIndex;
  }

  /**
   * Sets the {@code readerIndex} of this buffer.
   *
   * @throws IndexOutOfBoundsException if the specified {@code readerIndex} is less than {@code 0}
   *     or greater than {@code this.size}
   */
  public void readerIndex(int readerIndex) {
    if (readerIndex < 0) {
      throwIndexOOBExceptionForRead();
    } else if (readerIndex > size) {
      // in this case, diff must be greater than 0.
      streamReader.fillBuffer(readerIndex - size);
    }
    this.readerIndex = readerIndex;
  }

  public void readerIndexUnsafe(int readerIndex) {
    this.readerIndex = readerIndex;
  }

  /** Returns array index for reader index if buffer is a heap buffer. */
  public int unsafeHeapReaderIndex() {
    return readerIndex + heapOffset;
  }

  public void increaseReaderIndexUnsafe(int diff) {
    readerIndex += diff;
  }

  public void increaseReaderIndex(int diff) {
    int readerIdx = readerIndex;
    readerIndex = readerIdx += diff;
    if (readerIdx < 0) {
      throwIndexOOBExceptionForRead();
    } else if (readerIdx > size) {
      // in this case, diff must be greater than 0.
      streamReader.fillBuffer(readerIdx - size);
    }
  }

  public long getUnsafeReaderAddress() {
    return address + readerIndex;
  }

  public int remaining() {
    return size - readerIndex;
  }

  /** Returns the {@code writerIndex} of this buffer. */
  public int writerIndex() {
    return writerIndex;
  }

  /**
   * Sets the {@code writerIndex} of this buffer.
   *
   * @throws IndexOutOfBoundsException if the specified {@code writerIndex} is less than {@code 0}
   *     or greater than {@code this.size}
   */
  public void writerIndex(int writerIndex) {
    if (writerIndex < 0 || writerIndex > size) {
      throwOOBExceptionForWriteIndex(writerIndex);
    }
    this.writerIndex = writerIndex;
  }

  private void throwOOBExceptionForWriteIndex(int writerIndex) {
    throw new IndexOutOfBoundsException(
        String.format(
            "writerIndex: %d (expected: 0 <= writerIndex <= size(%d))", writerIndex, size));
  }

  // Check should be done outside to avoid this method got into the critical path.
  private void throwOOBException() {
    throw new IndexOutOfBoundsException(
        String.format("size: %d, address %s, addressLimit %d", size, address, addressLimit));
  }

  // Check should be done outside to avoid this method got into the critical path.
  private void throwIndexOOBExceptionForRead() {
    throw new IndexOutOfBoundsException(
        String.format(
            "readerIndex: %d (expected: 0 <= readerIndex <= size(%d))", readerIndex, size));
  }

  // Check should be done outside to avoid this method got into the critical path.
  private void throwIndexOOBExceptionForRead(int length) {
    throw new IndexOutOfBoundsException(
        String.format(
            "readerIndex: %d (expected: 0 <= readerIndex <= size(%d)), length %d",
            readerIndex, size, length));
  }

  public void unsafeWriterIndex(int writerIndex) {
    this.writerIndex = writerIndex;
  }

  /** Returns heap index for writer index if buffer is a heap buffer. */
  public int unsafeHeapWriterIndex() {
    return writerIndex + heapOffset;
  }

  public long getUnsafeWriterAddress() {
    return address + writerIndex;
  }

  public void increaseWriterIndexUnsafe(int diff) {
    this.writerIndex = writerIndex + diff;
  }

  /** Increase writer index and grow buffer if needed. */
  public void increaseWriterIndex(int diff) {
    int writerIdx = writerIndex + diff;
    ensure(writerIdx);
    this.writerIndex = writerIdx;
  }

  public void writeBoolean(boolean value) {
    final int writerIdx = writerIndex;
    final int newIdx = writerIdx + 1;
    ensure(newIdx);
    final long pos = address + writerIdx;
    UNSAFE.putByte(heapMemory, pos, (byte) (value ? 1 : 0));
    writerIndex = newIdx;
  }

  public void unsafeWriteByte(byte value) {
    final int writerIdx = writerIndex;
    final int newIdx = writerIdx + 1;
    final long pos = address + writerIdx;
    UNSAFE.putByte(heapMemory, pos, value);
    writerIndex = newIdx;
  }

  public void writeByte(byte value) {
    final int writerIdx = writerIndex;
    final int newIdx = writerIdx + 1;
    ensure(newIdx);
    final long pos = address + writerIdx;
    UNSAFE.putByte(heapMemory, pos, value);
    writerIndex = newIdx;
  }

  public void writeByte(int value) {
    writeByte((byte) value);
  }

  public void writeChar(char value) {
    final int writerIdx = writerIndex;
    final int newIdx = writerIdx + 2;
    ensure(newIdx);
    final long pos = address + writerIdx;
    if (LITTLE_ENDIAN) {
      UNSAFE.putChar(heapMemory, pos, value);
    } else {
      UNSAFE.putChar(heapMemory, pos, Character.reverseBytes(value));
    }
    writerIndex = newIdx;
  }

  public void unsafeWriteShort(short value) {
    final int writerIdx = writerIndex;
    final int newIdx = writerIdx + 2;
    final long pos = address + writerIdx;
    if (LITTLE_ENDIAN) {
      UNSAFE.putShort(heapMemory, pos, value);
    } else {
      UNSAFE.putShort(heapMemory, pos, Short.reverseBytes(value));
    }
    writerIndex = newIdx;
  }

  public void writeShort(short value) {
    final int writerIdx = writerIndex;
    final int newIdx = writerIdx + 2;
    ensure(newIdx);
    final long pos = address + writerIdx;
    if (LITTLE_ENDIAN) {
      UNSAFE.putShort(heapMemory, pos, value);
    } else {
      UNSAFE.putShort(heapMemory, pos, Short.reverseBytes(value));
    }
    writerIndex = newIdx;
  }

  public void writeInt(int value) {
    final int writerIdx = writerIndex;
    final int newIdx = writerIdx + 4;
    ensure(newIdx);
    final long pos = address + writerIdx;
    if (LITTLE_ENDIAN) {
      UNSAFE.putInt(heapMemory, pos, value);
    } else {
      UNSAFE.putInt(heapMemory, pos, Integer.reverseBytes(value));
    }
    writerIndex = newIdx;
  }

  public void unsafeWriteInt(int value) {
    final int writerIdx = writerIndex;
    final int newIdx = writerIdx + 4;
    final long pos = address + writerIdx;
    if (LITTLE_ENDIAN) {
      UNSAFE.putInt(heapMemory, pos, value);
    } else {
      UNSAFE.putInt(heapMemory, pos, Integer.reverseBytes(value));
    }
    writerIndex = newIdx;
  }

  public void writeLong(long value) {
    final int writerIdx = writerIndex;
    final int newIdx = writerIdx + 8;
    ensure(newIdx);
    final long pos = address + writerIdx;
    if (LITTLE_ENDIAN) {
      UNSAFE.putLong(heapMemory, pos, value);
    } else {
      UNSAFE.putLong(heapMemory, pos, Long.reverseBytes(value));
    }
    writerIndex = newIdx;
  }

  public void unsafeWriteLong(long value) {
    final int writerIdx = writerIndex;
    final int newIdx = writerIdx + 8;
    final long pos = address + writerIdx;
    if (LITTLE_ENDIAN) {
      UNSAFE.putLong(heapMemory, pos, value);
    } else {
      UNSAFE.putLong(heapMemory, pos, Long.reverseBytes(value));
    }
    writerIndex = newIdx;
  }

  public void writeFloat(float value) {
    final int writerIdx = writerIndex;
    final int newIdx = writerIdx + 4;
    ensure(newIdx);
    final long pos = address + writerIdx;
    if (LITTLE_ENDIAN) {
      UNSAFE.putInt(heapMemory, pos, Float.floatToRawIntBits(value));
    } else {
      UNSAFE.putInt(heapMemory, pos, Integer.reverseBytes(Float.floatToRawIntBits(value)));
    }
    writerIndex = newIdx;
  }

  public void writeDouble(double value) {
    final int writerIdx = writerIndex;
    final int newIdx = writerIdx + 8;
    ensure(newIdx);
    final long pos = address + writerIdx;
    if (LITTLE_ENDIAN) {
      UNSAFE.putLong(heapMemory, pos, Double.doubleToRawLongBits(value));
    } else {
      UNSAFE.putLong(heapMemory, pos, Long.reverseBytes(Double.doubleToRawLongBits(value)));
    }
    writerIndex = newIdx;
  }

  /**
   * Write int using variable length encoding. If the value is positive, use {@link
   * #writePositiveVarInt} to save one bit.
   */
  public int writeVarInt(int v) {
    ensure(writerIndex + 8);
    return unsafeWriteVarInt(v);
  }

  /**
   * Writes a 1-5 byte int.
   *
   * @return The number of bytes written.
   */
  public int writePositiveVarInt(int v) {
    // ensure at least 8 bytes are writable at once, so jvm-jit
    // generated code is smaller. Otherwise, `MapRefResolver.writeRefOrNull`
    // may be `callee is too large`/`already compiled into a big method`
    ensure(writerIndex + 8);
    return unsafeWritePositiveVarInt(v);
  }

  /**
   * For implementation efficiency, this method needs at most 8 bytes for writing 5 bytes using long
   * to avoid using two memory operations.
   */
  @CodegenInvoke
  public int unsafeWriteVarInt(int v) {
    // Ensure negatives close to zero is encode in little bytes.
    v = (v << 1) ^ (v >> 31);
    return unsafeWritePositiveVarInt(v);
  }

  /** Reads the 1-5 byte int part of a varint. */
  @CodegenInvoke
  public int readVarInt() {
    if (LITTLE_ENDIAN) {
      return readVarIntOnLE();
    } else {
      return readVarIntOnBE();
    }
  }

  /** Reads the 1-5 byte as a varint on a little endian mache. */
  @CodegenInvoke
  public int readVarIntOnLE() {
    // noinspection Duplicates
    int readIdx = readerIndex;
    int result;
    if (size - readIdx < 5) {
      result = readPositiveVarIntSlow();
    } else {
      long address = this.address;
      // | 1bit + 7bits | 1bit + 7bits | 1bit + 7bits | 1bit + 7bits |
      int fourByteValue = UNSAFE.getInt(heapMemory, address + readIdx);
      // Duplicate and manual inline for performance.
      // noinspection Duplicates
      readIdx++;
      result = fourByteValue & 0x7F;
      if ((fourByteValue & 0x80) != 0) {
        readIdx++;
        // 0x3f80: 0b1111111 << 7
        result |= (fourByteValue >>> 1) & 0x3f80;
        // 0x8000: 0b1 << 15
        if ((fourByteValue & 0x8000) != 0) {
          readIdx++;
          // 0x1fc000: 0b1111111 << 14
          result |= (fourByteValue >>> 2) & 0x1fc000;
          // 0x800000: 0b1 << 23
          if ((fourByteValue & 0x800000) != 0) {
            readIdx++;
            // 0xfe00000: 0b1111111 << 21
            result |= (fourByteValue >>> 3) & 0xfe00000;
            if ((fourByteValue & 0x80000000) != 0) {
              result |= (UNSAFE.getByte(heapMemory, address + readIdx++) & 0x7F) << 28;
            }
          }
        }
      }
      readerIndex = readIdx;
    }
    return (result >>> 1) ^ -(result & 1);
  }

  /** Reads the 1-5 byte as a varint on a big endian mache. */
  @CodegenInvoke
  public int readVarIntOnBE() {
    // noinspection Duplicates
    int readIdx = readerIndex;
    int result;
    if (size - readIdx < 5) {
      result = readPositiveVarIntSlow();
    } else {
      long address = this.address;
      int fourByteValue = Integer.reverseBytes(UNSAFE.getInt(heapMemory, address + readIdx));
      // Duplicate and manual inline for performance.
      // noinspection Duplicates
      readIdx++;
      result = fourByteValue & 0x7F;
      if ((fourByteValue & 0x80) != 0) {
        readIdx++;
        // 0x3f80: 0b1111111 << 7
        result |= (fourByteValue >>> 1) & 0x3f80;
        // 0x8000: 0b1 << 15
        if ((fourByteValue & 0x8000) != 0) {
          readIdx++;
          // 0x1fc000: 0b1111111 << 14
          result |= (fourByteValue >>> 2) & 0x1fc000;
          // 0x800000: 0b1 << 23
          if ((fourByteValue & 0x800000) != 0) {
            readIdx++;
            // 0xfe00000: 0b1111111 << 21
            result |= (fourByteValue >>> 3) & 0xfe00000;
            if ((fourByteValue & 0x80000000) != 0) {
              result |= (UNSAFE.getByte(heapMemory, address + readIdx++) & 0x7F) << 28;
            }
          }
        }
      }
      readerIndex = readIdx;
    }
    return (result >>> 1) ^ -(result & 1);
  }

  /**
   * For implementation efficiency, this method needs at most 8 bytes for writing 5 bytes using long
   * to avoid using two memory operations.
   */
  public int unsafeWritePositiveVarInt(int v) {
    int varintBytes = unsafePutPositiveVarInt(writerIndex, v);
    writerIndex += varintBytes;
    return varintBytes;
  }

  /**
   * Caller must ensure there must be at least 8 bytes for writing, otherwise the crash may occur.
   */
  public int unsafePutPositiveVarInt(int index, int v) {
    // The encoding algorithm are based on kryo UnsafeMemoryOutput.writeVarInt
    // varint are written using little endian byte order.
    // This version should have better performance since it remove an index update.
    long value = v;
    long varInt = (value & 0x7F);
    value >>>= 7;
    if (value == 0) {
      UNSAFE.putByte(heapMemory, address + index, (byte) varInt);
      return 1;
    }
    // bit 8 `set` indicates have next data bytes.
    varInt |= 0x80;
    varInt |= ((value & 0x7F) << 8);
    value >>>= 7;
    if (value == 0) {
      unsafePutInt(index, (int) varInt);
      return 2;
    }
    varInt |= (0x80 << 8);
    varInt |= ((value & 0x7F) << 16);
    value >>>= 7;
    if (value == 0) {
      unsafePutInt(index, (int) varInt);
      return 3;
    }
    varInt |= (0x80 << 16);
    varInt |= ((value & 0x7F) << 24);
    value >>>= 7;
    if (value == 0) {
      unsafePutInt(index, (int) varInt);
      return 4;
    }
    varInt |= (0x80L << 24);
    varInt |= ((value & 0x7F) << 32);
    varInt &= 0xFFFFFFFFFL;
    unsafePutLong(index, varInt);
    return 5;
  }

  /** Reads the 1-5 byte int part of a non-negative varint. */
  public int readPositiveVarInt() {
    int readIdx = readerIndex;
    if (size - readIdx < 5) {
      return readPositiveVarIntSlow();
    }
    // | 1bit + 7bits | 1bit + 7bits | 1bit + 7bits | 1bit + 7bits |
    int fourByteValue = unsafeGetInt(readIdx);
    readIdx++;
    int result = fourByteValue & 0x7F;
    // Duplicate and manual inline for performance.
    // noinspection Duplicates
    if ((fourByteValue & 0x80) != 0) {
      readIdx++;
      // 0x3f80: 0b1111111 << 7
      result |= (fourByteValue >>> 1) & 0x3f80;
      // 0x8000: 0b1 << 15
      if ((fourByteValue & 0x8000) != 0) {
        readIdx++;
        // 0x1fc000: 0b1111111 << 14
        result |= (fourByteValue >>> 2) & 0x1fc000;
        // 0x800000: 0b1 << 23
        if ((fourByteValue & 0x800000) != 0) {
          readIdx++;
          // 0xfe00000: 0b1111111 << 21
          result |= (fourByteValue >>> 3) & 0xfe00000;
          if ((fourByteValue & 0x80000000) != 0) {
            result |= (UNSAFE.getByte(heapMemory, address + readIdx++) & 0x7F) << 28;
          }
        }
      }
    }
    readerIndex = readIdx;
    return result;
  }

  /**
   * Fast path for read a unsigned varint which is mostly a smaller value in [0, 16384). When the
   * value is equal or greater than 16384, the read will be a little slower.
   */
  public int readVarUintSmall() {
    int readIdx = readerIndex;
    if (size - readIdx >= 5) {
      int fourByteValue = unsafeGetInt(readIdx++);
      int binarySize = fourByteValue & 0x7F;
      // Duplicate and manual inline for performance.
      // noinspection Duplicates
      if ((fourByteValue & 0x80) != 0) {
        readIdx++;
        binarySize |= (fourByteValue >>> 1) & 0x3f80;
        if ((fourByteValue & 0x8000) != 0) {
          // merely executed path, make it as a separate method to reduce
          // code size of current method for better jvm inline
          return continueRead(readIdx, fourByteValue, binarySize);
        }
      }
      readerIndex = readIdx;
      return binarySize;
    } else {
      return readPositiveVarIntSlow();
    }
  }

  private int readPositiveVarIntSlow() {
    // Note:
    //  Loop are not used here to improve performance,
    //  we manually unroll the loop for better performance.
    int b = readByte();
    int result = b & 0x7F;
    if ((b & 0x80) != 0) {
      b = readByte();
      result |= (b & 0x7F) << 7;
      if ((b & 0x80) != 0) {
        b = readByte();
        result |= (b & 0x7F) << 14;
        if ((b & 0x80) != 0) {
          b = readByte();
          result |= (b & 0x7F) << 21;
          if ((b & 0x80) != 0) {
            b = readByte();
            result |= (b & 0x7F) << 28;
          }
        }
      }
    }
    return result;
  }

  /**
   * Writes a 1-9 byte int, padding necessary bytes to align `writerIndex` to 4-byte.
   *
   * @return The number of bytes written.
   */
  public int writePositiveVarIntAligned(int value) {
    // Mask first 6 bits,
    // bit 7 `unset` indicates have next padding bytes,
    // bit 8 `set` indicates have next data bytes.
    if (value >>> 6 == 0) {
      return writePositiveVarIntAligned1(value);
    }
    if (value >>> 12 == 0) { // 2 byte data
      return writePositiveVarIntAligned2(value);
    }
    if (value >>> 18 == 0) { // 3 byte data
      return writePositiveVarIntAligned3(value);
    }
    if (value >>> 24 == 0) { // 4 byte data
      return writePositiveVarIntAligned4(value);
    }
    if (value >>> 30 == 0) { // 5 byte data
      return writePositiveVarIntAligned5(value);
    }
    // 6 byte data
    return writePositiveVarIntAligned6(value);
  }

  private int writePositiveVarIntAligned1(int value) {
    final int writerIdx = writerIndex;
    int numPaddingBytes = 4 - writerIdx % 4;
    ensure(writerIdx + 5); // 1 byte + 4 bytes(zero out), padding range in (zero out)
    int first = (value & 0x3F);
    final long pos = address + writerIdx;
    if (numPaddingBytes == 1) {
      // bit 7 `set` indicates not have padding bytes.
      // bit 8 `set` indicates have next data bytes.
      UNSAFE.putByte(heapMemory, pos, (byte) (first | 0x40));
      writerIndex = (writerIdx + 1);
      return 1;
    } else {
      UNSAFE.putByte(heapMemory, pos, (byte) first);
      // zero out 4 bytes, so that `bit 7` value can be trusted.
      UNSAFE.putInt(heapMemory, pos + 1, 0);
      UNSAFE.putByte(heapMemory, pos + numPaddingBytes - 1, (byte) (0x40));
      writerIndex = writerIdx + numPaddingBytes;
      return numPaddingBytes;
    }
  }

  private int writePositiveVarIntAligned2(int value) {
    final int writerIdx = writerIndex;
    int numPaddingBytes = 4 - writerIdx % 4;
    ensure(writerIdx + 6); // 2 byte + 4 bytes(zero out), padding range in (zero out)
    int first = (value & 0x3F);
    final long pos = address + writerIdx;
    UNSAFE.putByte(heapMemory, pos, (byte) (first | 0x80));
    if (numPaddingBytes == 2) {
      // bit 7 `set` indicates not have padding bytes.
      // bit 8 `set` indicates have next data bytes.
      UNSAFE.putByte(heapMemory, pos + 1, (byte) ((value >>> 6) | 0x40));
      writerIndex = writerIdx + 2;
      return 2;
    } else {
      UNSAFE.putByte(heapMemory, pos + 1, (byte) (value >>> 6));
      // zero out 4 bytes, so that `bit 7` value can be trusted.
      UNSAFE.putInt(heapMemory, pos + 2, 0);
      if (numPaddingBytes > 2) {
        UNSAFE.putByte(heapMemory, pos + numPaddingBytes - 1, (byte) (0x40));
        writerIndex = writerIdx + numPaddingBytes;
        return numPaddingBytes;
      } else {
        UNSAFE.putByte(heapMemory, pos + 4, (byte) (0x40));
        writerIndex = writerIdx + numPaddingBytes + 4;
        return numPaddingBytes + 4;
      }
    }
  }

  private int writePositiveVarIntAligned3(int value) {
    final int writerIdx = writerIndex;
    int numPaddingBytes = 4 - writerIdx % 4;
    ensure(writerIdx + 7); // 3 byte + 4 bytes(zero out), padding range in (zero out)
    int first = (value & 0x3F);
    final long pos = address + writerIdx;
    UNSAFE.putByte(heapMemory, pos, (byte) (first | 0x80));
    UNSAFE.putByte(heapMemory, pos + 1, (byte) ((value >>> 6) | 0x80));
    if (numPaddingBytes == 3) {
      // bit 7 `set` indicates not have padding bytes.
      // bit 8 `set` indicates have next data bytes.
      UNSAFE.putByte(heapMemory, pos + 2, (byte) ((value >>> 12) | 0x40));
      writerIndex = writerIdx + 3;
      return 3;
    } else {
      UNSAFE.putByte(heapMemory, pos + 2, (byte) (value >>> 12));
      // zero out 4 bytes, so that `bit 7` value can be trusted.
      UNSAFE.putInt(heapMemory, pos + 3, 0);
      if (numPaddingBytes == 4) {
        UNSAFE.putByte(heapMemory, pos + numPaddingBytes - 1, (byte) (0x40));
        writerIndex = writerIdx + numPaddingBytes;
        return numPaddingBytes;
      } else {
        UNSAFE.putByte(heapMemory, pos + numPaddingBytes + 3, (byte) (0x40));
        writerIndex = writerIdx + numPaddingBytes + 4;
        return numPaddingBytes + 4;
      }
    }
  }

  private int writePositiveVarIntAligned4(int value) {
    final int writerIdx = writerIndex;
    int numPaddingBytes = 4 - writerIdx % 4;
    ensure(writerIdx + 8); // 4 byte + 4 bytes(zero out), padding range in (zero out)
    int first = (value & 0x3F);
    final long pos = address + writerIdx;
    UNSAFE.putByte(heapMemory, pos, (byte) (first | 0x80));
    UNSAFE.putByte(heapMemory, pos + 1, (byte) (value >>> 6 | 0x80));
    UNSAFE.putByte(heapMemory, pos + 2, (byte) (value >>> 12 | 0x80));
    if (numPaddingBytes == 4) {
      // bit 7 `set` indicates not have padding bytes.
      // bit 8 `set` indicates have next data bytes.
      UNSAFE.putByte(heapMemory, pos + 3, (byte) ((value >>> 18) | 0x40));
      writerIndex = writerIdx + 4;
      return 4;
    } else {
      UNSAFE.putByte(heapMemory, pos + 3, (byte) (value >>> 18));
      // zero out 4 bytes, so that `bit 7` value can be trusted.
      UNSAFE.putInt(heapMemory, pos + 4, 0);
      UNSAFE.putByte(heapMemory, pos + numPaddingBytes + 3, (byte) (0x40));
      writerIndex = writerIdx + numPaddingBytes + 4;
      return numPaddingBytes + 4;
    }
  }

  private int writePositiveVarIntAligned5(int value) {
    final int writerIdx = writerIndex;
    int numPaddingBytes = 4 - writerIdx % 4;
    ensure(writerIdx + 9); // 5 byte + 4 bytes(zero out), padding range in (zero out)
    int first = (value & 0x3F);
    final long pos = address + writerIdx;
    UNSAFE.putByte(heapMemory, pos, (byte) (first | 0x80));
    UNSAFE.putByte(heapMemory, pos + 1, (byte) (value >>> 6 | 0x80));
    UNSAFE.putByte(heapMemory, pos + 2, (byte) (value >>> 12 | 0x80));
    UNSAFE.putByte(heapMemory, pos + 3, (byte) (value >>> 18 | 0x80));
    if (numPaddingBytes == 1) {
      // bit 7 `set` indicates not have padding bytes.
      // bit 8 `set` indicates have next data bytes.
      UNSAFE.putByte(heapMemory, pos + 4, (byte) ((value >>> 24) | 0x40));
      writerIndex = writerIdx + 5;
      return 5;
    } else {
      UNSAFE.putByte(heapMemory, pos + 4, (byte) (value >>> 24));
      // zero out 4 bytes, so that `bit 7` value can be trusted.
      UNSAFE.putInt(heapMemory, pos + 5, 0);
      UNSAFE.putByte(heapMemory, pos + numPaddingBytes + 3, (byte) (0x40));
      writerIndex = writerIdx + numPaddingBytes + 4;
      return numPaddingBytes + 4;
    }
  }

  private int writePositiveVarIntAligned6(int value) {
    final int writerIdx = writerIndex;
    int numPaddingBytes = 4 - writerIdx % 4;
    ensure(writerIdx + 10); // 6 byte + 4 bytes(zero out), padding range in (zero out)
    int first = (value & 0x3F);
    final long pos = address + writerIdx;
    UNSAFE.putByte(heapMemory, pos, (byte) (first | 0x80));
    UNSAFE.putByte(heapMemory, pos + 1, (byte) (value >>> 6 | 0x80));
    UNSAFE.putByte(heapMemory, pos + 2, (byte) (value >>> 12 | 0x80));
    UNSAFE.putByte(heapMemory, pos + 3, (byte) (value >>> 18 | 0x80));
    UNSAFE.putByte(heapMemory, pos + 4, (byte) (value >>> 24 | 0x80));
    if (numPaddingBytes == 2) {
      // bit 7 `set` indicates not have padding bytes.
      // bit 8 `set` indicates have next data bytes.
      UNSAFE.putByte(heapMemory, pos + 5, (byte) ((value >>> 30) | 0x40));
      writerIndex = writerIdx + 6;
      return 6;
    } else {
      UNSAFE.putByte(heapMemory, pos + 5, (byte) (value >>> 30));
      // zero out 4 bytes, so that `bit 7` value can be trusted.
      UNSAFE.putInt(heapMemory, pos + 6, 0);
      if (numPaddingBytes == 1) {
        UNSAFE.putByte(heapMemory, pos + 8, (byte) (0x40));
        writerIndex = writerIdx + 9;
        return 9;
      } else {
        UNSAFE.putByte(heapMemory, pos + numPaddingBytes + 3, (byte) (0x40));
        writerIndex = writerIdx + numPaddingBytes + 4;
        return numPaddingBytes + 4;
      }
    }
  }

  /** Reads the 1-9 byte int part of an aligned varint. */
  public int readPositiveAlignedVarInt() {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    if (readerIdx < size - 10) {
      return slowReadPositiveAlignedVarInt();
    }
    long pos = address + readerIdx;
    long startPos = pos;
    int b = UNSAFE.getByte(heapMemory, pos++);
    // Mask first 6 bits,
    // bit 8 `set` indicates have next data bytes.
    int result = b & 0x3F;
    // Note:
    //  Loop are not used here to improve performance.
    //  We manually unroll the loop for better performance.
    if ((b & 0x80) != 0) { // has 2nd byte
      b = UNSAFE.getByte(heapMemory, pos++);
      result |= (b & 0x3F) << 6;
      if ((b & 0x80) != 0) { // has 3rd byte
        b = UNSAFE.getByte(heapMemory, pos++);
        result |= (b & 0x3F) << 12;
        if ((b & 0x80) != 0) { // has 4th byte
          b = UNSAFE.getByte(heapMemory, pos++);
          result |= (b & 0x3F) << 18;
          if ((b & 0x80) != 0) { // has 5th byte
            b = UNSAFE.getByte(heapMemory, pos++);
            result |= (b & 0x3F) << 24;
            if ((b & 0x80) != 0) { // has 6th byte
              b = UNSAFE.getByte(heapMemory, pos++);
              result |= (b & 0x3F) << 30;
            }
          }
        }
      }
    }
    pos = skipPadding(pos, b); // split method for `readPositiveVarInt` inlined
    readerIndex = (int) (pos - startPos + readerIdx);
    return result;
  }

  public int slowReadPositiveAlignedVarInt() {
    int b = readByte();
    // Mask first 6 bits,
    // bit 8 `set` indicates have next data bytes.
    int result = b & 0x3F;
    if ((b & 0x80) != 0) { // has 2nd byte
      b = readByte();
      result |= (b & 0x3F) << 6;
      if ((b & 0x80) != 0) { // has 3rd byte
        b = readByte();
        result |= (b & 0x3F) << 12;
        if ((b & 0x80) != 0) { // has 4th byte
          b = readByte();
          result |= (b & 0x3F) << 18;
          if ((b & 0x80) != 0) { // has 5th byte
            b = readByte();
            result |= (b & 0x3F) << 24;
            if ((b & 0x80) != 0) { // has 6th byte
              b = readByte();
              result |= (b & 0x3F) << 30;
            }
          }
        }
      }
    }
    // bit 7 `unset` indicates have next padding bytes,
    if ((b & 0x40) == 0) { // has first padding bytes
      b = readByte();
      if ((b & 0x40) == 0) { // has 2nd padding bytes
        b = readByte();
        if ((b & 0x40) == 0) { // has 3rd padding bytes
          b = readByte();
          checkArgument((b & 0x40) != 0, "At most 3 padding bytes.");
        }
      }
    }
    return result;
  }

  private long skipPadding(long pos, int b) {
    // bit 7 `unset` indicates have next padding bytes,
    if ((b & 0x40) == 0) { // has first padding bytes
      b = UNSAFE.getByte(heapMemory, pos++);
      if ((b & 0x40) == 0) { // has 2nd padding bytes
        b = UNSAFE.getByte(heapMemory, pos++);
        if ((b & 0x40) == 0) { // has 3rd padding bytes
          b = UNSAFE.getByte(heapMemory, pos++);
          checkArgument((b & 0x40) != 0, "At most 3 padding bytes.");
        }
      }
    }
    return pos;
  }

  /**
   * Write long using variable length encoding. If the value is positive, use {@link
   * #writePositiveVarLong} to save one bit.
   */
  public int writeVarLong(long value) {
    ensure(writerIndex + 9);
    value = (value << 1) ^ (value >> 63);
    return unsafeWritePositiveVarLong(value);
  }

  @CodegenInvoke
  public int unsafeWriteVarLong(long value) {
    value = (value << 1) ^ (value >> 63);
    return unsafeWritePositiveVarLong(value);
  }

  public int writePositiveVarLong(long value) {
    // Var long encoding algorithm is based kryo UnsafeMemoryOutput.writeVarLong.
    // var long are written using little endian byte order.
    ensure(writerIndex + 9);
    return unsafeWritePositiveVarLong(value);
  }

  public int unsafeWritePositiveVarLong(long value) {
    final int writerIndex = this.writerIndex;
    int varInt;
    varInt = (int) (value & 0x7F);
    value >>>= 7;
    if (value == 0) {
      UNSAFE.putByte(heapMemory, address + writerIndex, (byte) varInt);
      this.writerIndex = writerIndex + 1;
      return 1;
    }
    varInt |= (int) ((value & 0x7F) << 8) | 0x80;
    value >>>= 7;
    if (value == 0) {
      unsafePutInt(writerIndex, varInt);
      this.writerIndex = writerIndex + 2;
      return 2;
    }
    varInt |= (int) ((value & 0x7F) << 16) | 0x8000;
    value >>>= 7;
    if (value == 0) {
      unsafePutInt(writerIndex, varInt);
      this.writerIndex = writerIndex + 3;
      return 3;
    }
    varInt |= (int) ((value & 0x7F) << 24) | 0x800000;
    value >>>= 7;
    if (value == 0) {
      unsafePutInt(writerIndex, varInt);
      this.writerIndex = writerIndex + 4;
      return 4;
    }
    long varLong = (varInt & 0xFFFFFFFFL);
    varLong |= ((value & 0x7F) << 32) | 0x80000000L;
    value >>>= 7;
    if (value == 0) {
      unsafePutLong(writerIndex, varLong);
      this.writerIndex = writerIndex + 5;
      return 5;
    }
    varLong |= ((value & 0x7F) << 40) | 0x8000000000L;
    value >>>= 7;
    if (value == 0) {
      unsafePutLong(writerIndex, varLong);
      this.writerIndex = writerIndex + 6;
      return 6;
    }
    varLong |= ((value & 0x7F) << 48) | 0x800000000000L;
    value >>>= 7;
    if (value == 0) {
      unsafePutLong(writerIndex, varLong);
      this.writerIndex = writerIndex + 7;
      return 7;
    }
    varLong |= ((value & 0x7F) << 56) | 0x80000000000000L;
    value >>>= 7;
    if (value == 0) {
      unsafePutLong(writerIndex, varLong);
      this.writerIndex = writerIndex + 8;
      return 8;
    }
    varLong |= 0x8000000000000000L;
    unsafePutLong(writerIndex, varLong);
    UNSAFE.putByte(heapMemory, address + writerIndex + 8, (byte) (value & 0xFF));
    this.writerIndex = writerIndex + 9;
    return 9;
  }

  /** Reads the 1-9 byte int part of a var long. */
  public long readVarLong() {
    return LITTLE_ENDIAN ? readVarLongOnLE() : readVarLongOnBE();
  }

  @CodegenInvoke
  public long readVarLongOnLE() {
    // Duplicate and manual inline for performance.
    // noinspection Duplicates
    int readIdx = readerIndex;
    long result;
    if (size - readIdx < 9) {
      result = readPositiveVarLongSlow();
    } else {
      long address = this.address;
      long value = UNSAFE.getLong(heapMemory, address + readIdx);
      // Duplicate and manual inline for performance.
      // noinspection Duplicates
      readIdx++;
      result = value & 0x7F;
      if ((value & 0x80) != 0) {
        readIdx++;
        // 0x3f80: 0b1111111 << 7
        result |= (value >>> 1) & 0x3f80;
        // 0x8000: 0b1 << 15
        if ((value & 0x8000) != 0) {
          readIdx++;
          // 0x1fc000: 0b1111111 << 14
          result |= (value >>> 2) & 0x1fc000;
          // 0x800000: 0b1 << 23
          if ((value & 0x800000) != 0) {
            readIdx++;
            // 0xfe00000: 0b1111111 << 21
            result |= (value >>> 3) & 0xfe00000;
            if ((value & 0x80000000L) != 0) {
              readIdx++;
              result |= (value >>> 4) & 0x7f0000000L;
              if ((value & 0x8000000000L) != 0) {
                readIdx++;
                result |= (value >>> 5) & 0x3f800000000L;
                if ((value & 0x800000000000L) != 0) {
                  readIdx++;
                  result |= (value >>> 6) & 0x1fc0000000000L;
                  if ((value & 0x80000000000000L) != 0) {
                    readIdx++;
                    result |= (value >>> 7) & 0xfe000000000000L;
                    if ((value & 0x8000000000000000L) != 0) {
                      long b = UNSAFE.getByte(heapMemory, address + readIdx++);
                      result |= b << 56;
                    }
                  }
                }
              }
            }
          }
        }
      }
      readerIndex = readIdx;
    }
    return ((result >>> 1) ^ -(result & 1));
  }

  @CodegenInvoke
  public long readVarLongOnBE() {
    int readIdx = readerIndex;
    long result;
    if (size - readIdx < 9) {
      result = readPositiveVarLongSlow();
    } else {
      long address = this.address;
      long value = Long.reverseBytes(UNSAFE.getLong(heapMemory, address + readIdx));
      // Duplicate and manual inline for performance.
      // noinspection Duplicates
      readIdx++;
      result = value & 0x7F;
      if ((value & 0x80) != 0) {
        readIdx++;
        // 0x3f80: 0b1111111 << 7
        result |= (value >>> 1) & 0x3f80;
        // 0x8000: 0b1 << 15
        if ((value & 0x8000) != 0) {
          readIdx++;
          // 0x1fc000: 0b1111111 << 14
          result |= (value >>> 2) & 0x1fc000;
          // 0x800000: 0b1 << 23
          if ((value & 0x800000) != 0) {
            readIdx++;
            // 0xfe00000: 0b1111111 << 21
            result |= (value >>> 3) & 0xfe00000;
            if ((value & 0x80000000L) != 0) {
              readIdx++;
              result |= (value >>> 4) & 0x7f0000000L;
              if ((value & 0x8000000000L) != 0) {
                readIdx++;
                result |= (value >>> 5) & 0x3f800000000L;
                if ((value & 0x800000000000L) != 0) {
                  readIdx++;
                  result |= (value >>> 6) & 0x1fc0000000000L;
                  if ((value & 0x80000000000000L) != 0) {
                    readIdx++;
                    result |= (value >>> 7) & 0xfe000000000000L;
                    if ((value & 0x8000000000000000L) != 0) {
                      long b = UNSAFE.getByte(heapMemory, address + readIdx++);
                      result |= b << 56;
                    }
                  }
                }
              }
            }
          }
        }
      }
      readerIndex = readIdx;
    }
    return ((result >>> 1) ^ -(result & 1));
  }

  /** Reads the 1-9 byte int part of a non-negative var long. */
  public long readPositiveVarLong() {
    int readIdx = readerIndex;
    if (size - readIdx < 9) {
      return readPositiveVarLongSlow();
    }
    // varint are written using little endian byte order, so read by little endian byte order.
    long value = unsafeGetLong(readIdx);
    // Duplicate and manual inline for performance.
    // noinspection Duplicates
    readIdx++;
    long result = value & 0x7F;
    if ((value & 0x80) != 0) {
      readIdx++;
      // 0x3f80: 0b1111111 << 7
      result |= (value >>> 1) & 0x3f80;
      // 0x8000: 0b1 << 15
      if ((value & 0x8000) != 0) {
        readIdx++;
        // 0x1fc000: 0b1111111 << 14
        result |= (value >>> 2) & 0x1fc000;
        // 0x800000: 0b1 << 23
        if ((value & 0x800000) != 0) {
          readIdx++;
          // 0xfe00000: 0b1111111 << 21
          result |= (value >>> 3) & 0xfe00000;
          if ((value & 0x80000000L) != 0) {
            readIdx++;
            result |= (value >>> 4) & 0x7f0000000L;
            if ((value & 0x8000000000L) != 0) {
              readIdx++;
              result |= (value >>> 5) & 0x3f800000000L;
              if ((value & 0x800000000000L) != 0) {
                readIdx++;
                result |= (value >>> 6) & 0x1fc0000000000L;
                if ((value & 0x80000000000000L) != 0) {
                  readIdx++;
                  result |= (value >>> 7) & 0xfe000000000000L;
                  if ((value & 0x8000000000000000L) != 0) {
                    long b = UNSAFE.getByte(heapMemory, address + readIdx++);
                    result |= b << 56;
                  }
                }
              }
            }
          }
        }
      }
    }
    readerIndex = readIdx;
    return result;
  }

  private long readPositiveVarLongSlow() {
    long b = readByte();
    long result = b & 0x7F;
    // Note:
    //  Loop are not used here to improve performance.
    //  We manually unroll the loop for better performance.
    if ((b & 0x80) != 0) {
      b = readByte();
      result |= (b & 0x7F) << 7;
      if ((b & 0x80) != 0) {
        b = readByte();
        result |= (b & 0x7F) << 14;
        if ((b & 0x80) != 0) {
          b = readByte();
          result |= (b & 0x7F) << 21;
          if ((b & 0x80) != 0) {
            b = readByte();
            result |= (b & 0x7F) << 28;
            if ((b & 0x80) != 0) {
              b = readByte();
              result |= (b & 0x7F) << 35;
              if ((b & 0x80) != 0) {
                b = readByte();
                result |= (b & 0x7F) << 42;
                if ((b & 0x80) != 0) {
                  b = readByte();
                  result |= (b & 0x7F) << 49;
                  if ((b & 0x80) != 0) {
                    b = readByte();
                    // highest bit in last byte is symbols bit.
                    result |= b << 56;
                  }
                }
              }
            }
          }
        }
      }
    }
    return result;
  }

  /**
   * Write long using fury SLI(Small long as int) encoding. If long is in [0xc0000000, 0x3fffffff],
   * encode as 4 bytes int: | little-endian: ((int) value) << 1 |; Otherwise write as 9 bytes: | 0b1
   * | little-endian 8bytes long |
   */
  public int writeSliLong(long value) {
    ensure(writerIndex + 9);
    return unsafeWriteSliLong(value);
  }

  private static final long HALF_MAX_INT_VALUE = Integer.MAX_VALUE / 2;
  private static final long HALF_MIN_INT_VALUE = Integer.MIN_VALUE / 2;
  private static final byte BIG_LONG_FLAG = 0b1; // bit 0 set, means big long.

  /** Write long using fury SLI(Small Long as Int) encoding. */
  public int unsafeWriteSliLong(long value) {
    final int writerIndex = this.writerIndex;
    final long pos = address + writerIndex;
    final byte[] heapMemory = this.heapMemory;
    if (value >= HALF_MIN_INT_VALUE && value <= HALF_MAX_INT_VALUE) {
      // write:
      // 00xxx -> 0xxx
      // 11xxx -> 1xxx
      // read:
      // 0xxx -> 00xxx
      // 1xxx -> 11xxx
      int v = ((int) value) << 1; // bit 0 unset, means int.
      if (LITTLE_ENDIAN) {
        UNSAFE.putInt(heapMemory, pos, v);
      } else {
        UNSAFE.putInt(heapMemory, pos, Integer.reverseBytes(v));
      }
      this.writerIndex = writerIndex + 4;
      return 4;
    } else {
      UNSAFE.putByte(heapMemory, pos, BIG_LONG_FLAG);
      if (LITTLE_ENDIAN) {
        UNSAFE.putLong(heapMemory, pos + 1, value);
      } else {
        UNSAFE.putLong(heapMemory, pos + 1, Long.reverseBytes(value));
      }
      this.writerIndex = writerIndex + 9;
      return 9;
    }
  }

  public void writeBytes(byte[] bytes) {
    writeBytes(bytes, 0, bytes.length);
  }

  public void writeBytes(byte[] bytes, int offset, int length) {
    final int writerIdx = writerIndex;
    final int newIdx = writerIdx + length;
    ensure(newIdx);
    put(writerIdx, bytes, offset, length);
    writerIndex = newIdx;
  }

  public void write(ByteBuffer source) {
    write(source, source.remaining());
  }

  public void write(ByteBuffer source, int numBytes) {
    final int writerIdx = writerIndex;
    final int newIdx = writerIdx + numBytes;
    ensure(newIdx);
    put(writerIdx, source, numBytes);
    writerIndex = newIdx;
  }

  public void writeBytesWithSize(byte[] arr) {
    writePrimitiveArrayWithSize(arr, Platform.BYTE_ARRAY_OFFSET, arr.length);
  }

  /** Write a primitive array into buffer with size varint encoded into the buffer. */
  public void writePrimitiveArrayWithSize(Object arr, int offset, int numBytes) {
    int idx = writerIndex;
    ensure(idx + 5 + numBytes);
    idx += unsafeWritePositiveVarInt(numBytes);
    final long destAddr = address + idx;
    Platform.copyMemory(arr, offset, heapMemory, destAddr, numBytes);
    writerIndex = idx + numBytes;
  }

  public void writePrimitiveArrayAlignedSize(Object arr, int offset, int numBytes) {
    writePositiveVarIntAligned(numBytes);
    final int writerIdx = writerIndex;
    final int newIdx = writerIdx + numBytes;
    ensure(newIdx);
    final long destAddr = address + writerIdx;
    Platform.copyMemory(arr, offset, heapMemory, destAddr, numBytes);
    writerIndex = newIdx;
  }

  public void writePrimitiveArray(Object arr, int offset, int numBytes) {
    final int writerIdx = writerIndex;
    final int newIdx = writerIdx + numBytes;
    ensure(newIdx);
    final long destAddr = address + writerIdx;
    Platform.copyMemory(arr, offset, heapMemory, destAddr, numBytes);
    writerIndex = newIdx;
  }

  /** For off-heap buffer, this will make a heap buffer internally. */
  public void grow(int neededSize) {
    ensure(writerIndex + neededSize);
  }

  /** For off-heap buffer, this will make a heap buffer internally. */
  public void ensure(int length) {
    if (length > size) {
      byte[] data = new byte[length * 2];
      copyToUnsafe(0, data, Platform.BYTE_ARRAY_OFFSET, size());
      initHeapBuffer(data, 0, data.length);
    }
  }

  public boolean readBoolean() {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    if (readerIdx > size - 1) {
      streamReader.fillBuffer(1);
    }
    readerIndex = readerIdx + 1;
    return UNSAFE.getByte(heapMemory, address + readerIdx) != 0;
  }

  public byte readByte() {
    int readerIdx = readerIndex;
    if (readerIdx > size - 1) {
      streamReader.fillBuffer(1);
    }
    readerIndex = readerIdx + 1;
    return UNSAFE.getByte(heapMemory, address + readerIdx);
  }

  public char readChar() {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    int remaining = size - readerIdx;
    if (remaining < 2) {
      streamReader.fillBuffer(2 - remaining);
    }
    readerIndex = readerIdx + 2;
    final long pos = address + readerIdx;
    if (LITTLE_ENDIAN) {
      return UNSAFE.getChar(heapMemory, pos);
    } else {
      return Character.reverseBytes(UNSAFE.getChar(heapMemory, pos));
    }
  }

  // Reduce method body for better inline in the caller.
  @CodegenInvoke
  public char readCharOnLE() {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    int remaining = size - readerIdx;
    if (remaining < 2) {
      streamReader.fillBuffer(2 - remaining);
    }
    readerIndex = readerIdx + 2;
    return UNSAFE.getChar(heapMemory, address + readerIdx);
  }

  // Reduce method body for better inline in the caller.
  @CodegenInvoke
  public char readCharOnBE() {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    int remaining = size - readerIdx;
    if (remaining < 2) {
      streamReader.fillBuffer(2 - remaining);
    }
    readerIndex = readerIdx + 2;
    return Character.reverseBytes(UNSAFE.getChar(heapMemory, address + readerIdx));
  }

  public short readShort() {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    int remaining = size - readerIdx;
    if (remaining < 2) {
      streamReader.fillBuffer(2 - remaining);
    }
    readerIndex = readerIdx + 2;
    final long pos = address + readerIdx;
    if (LITTLE_ENDIAN) {
      return UNSAFE.getShort(heapMemory, pos);
    } else {
      return Short.reverseBytes(UNSAFE.getShort(heapMemory, pos));
    }
  }

  // Reduce method body for better inline in the caller.
  @CodegenInvoke
  public short readShortOnLE() {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    int remaining = size - readerIdx;
    if (remaining < 2) {
      streamReader.fillBuffer(2 - remaining);
    }
    readerIndex = readerIdx + 2;
    return UNSAFE.getShort(heapMemory, address + readerIdx);
  }

  // Reduce method body for better inline in the caller.
  @CodegenInvoke
  public short readShortOnBE() {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    int remaining = size - readerIdx;
    if (remaining < 2) {
      streamReader.fillBuffer(2 - remaining);
    }
    readerIndex = readerIdx + 2;
    return Short.reverseBytes(UNSAFE.getShort(heapMemory, address + readerIdx));
  }

  public int readInt() {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    int remaining = size - readerIdx;
    if (remaining < 4) {
      streamReader.fillBuffer(4 - remaining);
    }
    readerIndex = readerIdx + 4;
    final long pos = address + readerIdx;
    if (LITTLE_ENDIAN) {
      return UNSAFE.getInt(heapMemory, pos);
    } else {
      return Integer.reverseBytes(UNSAFE.getInt(heapMemory, pos));
    }
  }

  // Reduce method body for better inline in the caller.
  @CodegenInvoke
  public int readIntOnLE() {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    int remaining = size - readerIdx;
    if (remaining < 4) {
      streamReader.fillBuffer(4 - remaining);
    }
    readerIndex = readerIdx + 4;
    return UNSAFE.getInt(heapMemory, address + readerIdx);
  }

  // Reduce method body for better inline in the caller.
  @CodegenInvoke
  public int readIntOnBE() {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    int remaining = size - readerIdx;
    if (remaining < 4) {
      streamReader.fillBuffer(4 - remaining);
    }
    readerIndex = readerIdx + 4;
    return Integer.reverseBytes(UNSAFE.getInt(heapMemory, address + readerIdx));
  }

  public long readLong() {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    int remaining = size - readerIdx;
    if (remaining < 8) {
      streamReader.fillBuffer(8 - remaining);
    }
    readerIndex = readerIdx + 8;
    final long pos = address + readerIdx;
    if (LITTLE_ENDIAN) {
      return UNSAFE.getLong(heapMemory, pos);
    } else {
      return Long.reverseBytes(UNSAFE.getLong(heapMemory, pos));
    }
  }

  // Reduce method body for better inline in the caller.
  @CodegenInvoke
  public long readLongOnLE() {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    int remaining = size - readerIdx;
    if (remaining < 8) {
      streamReader.fillBuffer(8 - remaining);
    }
    readerIndex = readerIdx + 8;
    return UNSAFE.getLong(heapMemory, address + readerIdx);
  }

  // Reduce method body for better inline in the caller.
  @CodegenInvoke
  public long readLongOnBE() {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    int remaining = size - readerIdx;
    if (remaining < 8) {
      streamReader.fillBuffer(8 - remaining);
    }
    readerIndex = readerIdx + 8;
    return Long.reverseBytes(UNSAFE.getLong(heapMemory, address + readerIdx));
  }

  /** Read fury SLI(Small Long as Int) encoded long. */
  public long readSliLong() {
    if (LITTLE_ENDIAN) {
      return readSliLongOnLE();
    } else {
      return readSliLongOnBE();
    }
  }

  @CodegenInvoke
  public long readSliLongOnLE() {
    // Duplicate and manual inline for performance.
    // noinspection Duplicates
    final int readIdx = readerIndex;
    int diff = size - readIdx;
    if (diff < 4) {
      streamReader.fillBuffer(4 - diff);
    }
    int i = UNSAFE.getInt(heapMemory, address + readIdx);
    if ((i & 0b1) != 0b1) {
      readerIndex = readIdx + 4;
      return i >> 1;
    }
    if (diff < 9) {
      streamReader.fillBuffer(9 - diff);
    }
    readerIndex = readIdx + 9;
    return UNSAFE.getLong(heapMemory, address + readIdx + 1);
  }

  @CodegenInvoke
  public long readSliLongOnBE() {
    // noinspection Duplicates
    final int readIdx = readerIndex;
    int diff = size - readIdx;
    if (diff < 4) {
      streamReader.fillBuffer(4 - diff);
    }
    int i = Integer.reverseBytes(UNSAFE.getInt(heapMemory, address + readIdx));
    if ((i & 0b1) != 0b1) {
      readerIndex = readIdx + 4;
      return i >> 1;
    }
    if (diff < 9) {
      streamReader.fillBuffer(9 - diff);
    }
    readerIndex = readIdx + 9;
    return Long.reverseBytes(UNSAFE.getLong(heapMemory, address + readIdx + 1));
  }

  public float readFloat() {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    int remaining = size - readerIdx;
    if (remaining < 4) {
      streamReader.fillBuffer(4 - remaining);
    }
    readerIndex = readerIdx + 4;
    final long pos = address + readerIdx;
    if (LITTLE_ENDIAN) {
      return Float.intBitsToFloat(UNSAFE.getInt(heapMemory, pos));
    } else {
      return Float.intBitsToFloat(Integer.reverseBytes(UNSAFE.getInt(heapMemory, pos)));
    }
  }

  // Reduce method body for better inline in the caller.
  @CodegenInvoke
  public float readFloatOnLE() {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    int remaining = size - readerIdx;
    if (remaining < 4) {
      streamReader.fillBuffer(4 - remaining);
    }
    readerIndex = readerIdx + 4;
    return Float.intBitsToFloat(UNSAFE.getInt(heapMemory, address + readerIdx));
  }

  // Reduce method body for better inline in the caller.
  @CodegenInvoke
  public float readFloatOnBE() {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    int remaining = size - readerIdx;
    if (remaining < 4) {
      streamReader.fillBuffer(4 - remaining);
    }
    readerIndex = readerIdx + 4;
    return Float.intBitsToFloat(
        Integer.reverseBytes(UNSAFE.getInt(heapMemory, address + readerIdx)));
  }

  public double readDouble() {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    int remaining = size - readerIdx;
    if (remaining < 8) {
      streamReader.fillBuffer(8 - remaining);
    }
    readerIndex = readerIdx + 8;
    final long pos = address + readerIdx;
    if (LITTLE_ENDIAN) {
      return Double.longBitsToDouble(UNSAFE.getLong(heapMemory, pos));
    } else {
      return Double.longBitsToDouble(Long.reverseBytes(UNSAFE.getLong(heapMemory, pos)));
    }
  }

  // Reduce method body for better inline in the caller.
  @CodegenInvoke
  public double readDoubleOnLE() {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    int remaining = size - readerIdx;
    if (remaining < 8) {
      streamReader.fillBuffer(8 - remaining);
    }
    readerIndex = readerIdx + 8;
    return Double.longBitsToDouble(UNSAFE.getLong(heapMemory, address + readerIdx));
  }

  // Reduce method body for better inline in the caller.
  @CodegenInvoke
  public double readDoubleOnBE() {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    int remaining = size - readerIdx;
    if (remaining < 8) {
      streamReader.fillBuffer(8 - remaining);
    }
    readerIndex = readerIdx + 8;
    return Double.longBitsToDouble(
        Long.reverseBytes(UNSAFE.getLong(heapMemory, address + readerIdx)));
  }

  public byte[] readBytes(int length) {
    int readerIdx = readerIndex;
    byte[] bytes = new byte[length];
    // use subtract to avoid overflow
    if (length > size - readerIdx) {
      streamReader.readTo(bytes, 0, length);
      return bytes;
    }
    byte[] heapMemory = this.heapMemory;
    if (heapMemory != null) {
      // System.arraycopy faster for some jdk than Unsafe.
      System.arraycopy(heapMemory, heapOffset + readerIdx, bytes, 0, length);
    } else {
      Platform.copyMemory(null, address + readerIdx, bytes, Platform.BYTE_ARRAY_OFFSET, length);
    }
    readerIndex = readerIdx + length;
    return bytes;
  }

  public void readBytes(byte[] dst, int dstIndex, int length) {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    if (readerIdx > size - length) {
      streamReader.readTo(dst, dstIndex, length);
      return;
    }
    if (dstIndex < 0 || dstIndex > dst.length - length) {
      throwIndexOOBExceptionForRead();
    }
    copyToUnsafe(readerIdx, dst, Platform.BYTE_ARRAY_OFFSET + dstIndex, length);
    readerIndex = readerIdx + length;
  }

  public void readBytes(byte[] dst) {
    readBytes(dst, 0, dst.length);
  }

  public int read(ByteBuffer dst) {
    int readerIdx = readerIndex;
    int len = dst.remaining();
    // use subtract to avoid overflow
    if (readerIdx > size - len) {
      return streamReader.readToByteBuffer(dst);
    }
    if (heapMemory != null) {
      dst.put(heapMemory, readerIndex + heapOffset, len);
    } else {
      dst.put(sliceAsByteBuffer(readerIdx, len));
    }
    readerIndex = readerIdx + len;
    return len;
  }

  public void read(ByteBuffer dst, int len) {
    int readerIdx = readerIndex;
    // use subtract to avoid overflow
    if (readerIdx > size - len) {
      streamReader.readToByteBuffer(dst, len);
    } else {
      if (heapMemory != null) {
        dst.put(heapMemory, readerIndex + heapOffset, len);
      } else {
        dst.put(sliceAsByteBuffer(readerIdx, len));
      }
      readerIndex = readerIdx + len;
    }
  }

  /**
   * Read size for following binary, this method will check and fill readable bytes too. This method
   * is optimized for small size, it's faster than {@link #readPositiveVarInt}.
   */
  public int readBinarySize() {
    int binarySize;
    int readIdx = readerIndex;
    if (size - readIdx >= 5) {
      int fourByteValue = unsafeGetInt(readIdx++);
      binarySize = fourByteValue & 0x7F;
      // Duplicate and manual inline for performance.
      // noinspection Duplicates
      if ((fourByteValue & 0x80) != 0) {
        readIdx++;
        binarySize |= (fourByteValue >>> 1) & 0x3f80;
        if ((fourByteValue & 0x8000) != 0) {
          // merely executed path, make it as a separate method to reduce
          // code size of current method for better jvm inline
          return continueRead(readIdx, fourByteValue, binarySize);
        }
      }
      readerIndex = readIdx;
    } else {
      binarySize = readPositiveVarIntSlow();
      readIdx = readerIndex;
    }
    int diff = size - readIdx;
    if (diff < binarySize) {
      streamReader.fillBuffer(diff);
    }
    return binarySize;
  }

  private int continueRead(int readIdx, int bulkRead, int binarySize) {
    // Duplicate and manual inline for performance.
    // noinspection Duplicates
    readIdx++;
    binarySize |= (bulkRead >>> 2) & 0x1fc000;
    if ((bulkRead & 0x800000) != 0) {
      readIdx++;
      binarySize |= (bulkRead >>> 3) & 0xfe00000;
      if ((bulkRead & 0x80000000) != 0) {
        binarySize |= (UNSAFE.getByte(heapMemory, address + readIdx++) & 0x7F) << 28;
      }
    }
    int diff = size - readIdx;
    if (diff < binarySize) {
      streamReader.fillBuffer(diff);
    }
    return binarySize;
  }

  public byte[] readBytesAndSize() {
    final int numBytes = readBinarySize();
    int readerIdx = readerIndex;
    final byte[] arr = new byte[numBytes];
    byte[] heapMemory = this.heapMemory;
    if (heapMemory != null) {
      System.arraycopy(heapMemory, heapOffset + readerIdx, arr, 0, numBytes);
    } else {
      Platform.UNSAFE.copyMemory(
          null, address + readerIdx, arr, Platform.BYTE_ARRAY_OFFSET, numBytes);
    }
    readerIndex = readerIdx + numBytes;
    return arr;
  }

  public byte[] readBytesWithAlignedSize() {
    final int numBytes = readPositiveAlignedVarInt();
    int readerIdx = readerIndex;
    final byte[] arr = new byte[numBytes];
    // use subtract to avoid overflow
    if (readerIdx > size - numBytes) {
      streamReader.readTo(arr, 0, numBytes);
      return arr;
    }
    Platform.UNSAFE.copyMemory(
        this.heapMemory, this.address + readerIdx, arr, Platform.BYTE_ARRAY_OFFSET, numBytes);
    readerIndex = readerIdx + numBytes;
    return arr;
  }

  /** This method should be used to read data written by {@link #writePrimitiveArrayWithSize}. */
  public char[] readChars(int numBytes) {
    int readerIdx = readerIndex;
    final char[] chars = new char[numBytes >> 1];
    // use subtract to avoid overflow
    if (readerIdx > size - numBytes) {
      streamReader.readToUnsafe(chars, 0, numBytes);
      return chars;
    }
    Platform.copyMemory(
        heapMemory, address + readerIdx, chars, Platform.CHAR_ARRAY_OFFSET, numBytes);
    readerIndex = readerIdx + numBytes;
    return chars;
  }

  public void readChars(char[] chars, int offset, int numBytes) {
    final int readerIdx = readerIndex;
    // use subtract to avoid overflow
    if (readerIdx > size - numBytes) {
      streamReader.readToUnsafe(chars, offset, numBytes);
      return;
    }
    Platform.copyMemory(heapMemory, address + readerIdx, chars, offset, numBytes);
    readerIndex = readerIdx + numBytes;
  }

  public char[] readCharsWithAlignedSize() {
    final int numBytes = readPositiveAlignedVarInt();
    return readChars(numBytes);
  }

  public long[] readLongs(int numBytes) {
    int readerIdx = readerIndex;
    int numElements = numBytes >> 3;
    final long[] longs = new long[numElements];
    // use subtract to avoid overflow
    if (readerIdx > size - numBytes) {
      streamReader.readToUnsafe(longs, 0, numElements);
      return longs;
    }
    Platform.copyMemory(
        heapMemory, address + readerIdx, longs, Platform.LONG_ARRAY_OFFSET, numBytes);
    readerIndex = readerIdx + numBytes;
    return longs;
  }

  /**
   * Bulk copy method. Copies {@code numBytes} bytes to target unsafe object and pointer. NOTE: This
   * is a unsafe method, no check here, please be carefully.
   */
  public void readToUnsafe(Object target, long targetPointer, int numBytes) {
    int remaining = size - readerIndex;
    if (numBytes > remaining) {
      streamReader.readToUnsafe(target, targetPointer, numBytes);
    } else {
      int readerIdx = readerIndex;
      Platform.copyMemory(heapMemory, address + readerIdx, target, targetPointer, numBytes);
      readerIndex = readerIdx + numBytes;
    }
  }

  public void checkReadableBytes(int minimumReadableBytes) {
    // use subtract to avoid overflow
    int remaining = size - readerIndex;
    if (minimumReadableBytes > remaining) {
      streamReader.fillBuffer(minimumReadableBytes - remaining);
    }
  }

  /**
   * Bulk copy method. Copies {@code numBytes} bytes to target unsafe object and pointer. NOTE: This
   * is a unsafe method, no check here, please be carefully.
   */
  public void copyToUnsafe(long offset, Object target, long targetPointer, int numBytes) {
    final long thisPointer = this.address + offset;
    checkArgument(thisPointer + numBytes <= addressLimit);
    Platform.copyMemory(this.heapMemory, thisPointer, target, targetPointer, numBytes);
  }

  /**
   * Bulk copy method. Copies {@code numBytes} bytes from source unsafe object and pointer. NOTE:
   * This is an unsafe method, no check here, please be carefully.
   */
  public void copyFromUnsafe(long offset, Object source, long sourcePointer, long numBytes) {
    final long thisPointer = this.address + offset;
    checkArgument(thisPointer + numBytes <= addressLimit);
    Platform.copyMemory(source, sourcePointer, this.heapMemory, thisPointer, numBytes);
  }

  /**
   * Bulk copy method. Copies {@code numBytes} bytes from this memory buffer, starting at position
   * {@code offset} to the target memory buffer. The bytes will be put into the target buffer
   * starting at position {@code targetOffset}.
   *
   * @param offset The position where the bytes are started to be read from in this memory buffer.
   * @param target The memory buffer to copy the bytes to.
   * @param targetOffset The position in the target memory buffer to copy the chunk to.
   * @param numBytes The number of bytes to copy.
   * @throws IndexOutOfBoundsException If either of the offsets is invalid, or the source buffer
   *     does not contain the given number of bytes (starting from offset), or the target buffer
   *     does not have enough space for the bytes (counting from targetOffset).
   */
  public void copyTo(int offset, MemoryBuffer target, int targetOffset, int numBytes) {
    final byte[] thisHeapRef = this.heapMemory;
    final byte[] otherHeapRef = target.heapMemory;
    final long thisPointer = this.address + offset;
    final long otherPointer = target.address + targetOffset;

    if ((numBytes | offset | targetOffset) >= 0
        && thisPointer <= this.addressLimit - numBytes
        && otherPointer <= target.addressLimit - numBytes) {
      UNSAFE.copyMemory(thisHeapRef, thisPointer, otherHeapRef, otherPointer, numBytes);
    } else if (this.address > this.addressLimit) {
      throw new IllegalStateException("this memory buffer has been freed.");
    } else if (target.address > target.addressLimit) {
      throw new IllegalStateException("target memory buffer has been freed.");
    } else {
      throw new IndexOutOfBoundsException(
          String.format(
              "offset=%d, targetOffset=%d, numBytes=%d, address=%d, targetAddress=%d",
              offset, targetOffset, numBytes, this.address, target.address));
    }
  }

  public void copyFrom(int offset, MemoryBuffer source, int sourcePointer, int numBytes) {
    source.copyTo(sourcePointer, this, offset, numBytes);
  }

  /**
   * Returns internal byte array if data is on heap and remaining buffer size is equal to internal
   * byte array size, or create a new byte array which copy remaining data from off-heap.
   */
  public byte[] getRemainingBytes() {
    int length = size - readerIndex;
    if (heapMemory != null && size == length) {
      return heapMemory;
    } else {
      return getBytes(readerIndex, length);
    }
  }

  /**
   * Returns internal byte array if data is on heap and buffer size is equal to internal byte array
   * size , or create a new byte array which copy data from off-heap.
   */
  public byte[] getAllBytes() {
    if (heapMemory != null && size == heapMemory.length) {
      return heapMemory;
    } else {
      return getBytes(0, size);
    }
  }

  public byte[] getBytes(int index, int length) {
    if (index == 0 && heapMemory != null && heapOffset == 0) {
      // Arrays.copyOf is an intrinsics, which is faster
      return Arrays.copyOf(heapMemory, length);
    }
    if (index + length > size) {
      throwIndexOOBExceptionForRead(length);
    }
    byte[] data = new byte[length];
    copyToUnsafe(index, data, Platform.BYTE_ARRAY_OFFSET, length);
    return data;
  }

  public void getBytes(int index, byte[] dst, int dstIndex, int length) {
    if (dstIndex > dst.length - length) {
      throwOOBException();
    }
    if (index > size - length) {
      throwOOBException();
    }
    copyToUnsafe(index, dst, Platform.BYTE_ARRAY_OFFSET + dstIndex, length);
  }

  public MemoryBuffer slice(int offset) {
    return slice(offset, size - offset);
  }

  public MemoryBuffer slice(int offset, int length) {
    if (offset + length > size) {
      throw new IndexOutOfBoundsException(
          String.format(
              "offset(%d) + length(%d) exceeds size(%d): %s", offset, length, size, this));
    }
    if (heapMemory != null) {
      return new MemoryBuffer(heapMemory, heapOffset + offset, length);
    } else {
      return new MemoryBuffer(address + offset, length, offHeapBuffer);
    }
  }

  public ByteBuffer sliceAsByteBuffer() {
    return sliceAsByteBuffer(readerIndex, size - readerIndex);
  }

  public ByteBuffer sliceAsByteBuffer(int offset, int length) {
    if (offset + length > size) {
      throw new IndexOutOfBoundsException(
          String.format(
              "offset(%d) + length(%d) exceeds size(%d): %s", offset, length, size, this));
    }
    if (heapMemory != null) {
      return ByteBuffer.wrap(heapMemory, heapOffset + offset, length).slice();
    } else {
      ByteBuffer offHeapBuffer = this.offHeapBuffer;
      if (offHeapBuffer != null) {
        ByteBuffer duplicate = offHeapBuffer.duplicate();
        int start = (int) (address - Platform.getAddress(duplicate));
        duplicate.position(start + offset);
        duplicate.limit(start + offset + length);
        return duplicate.slice();
      } else {
        return Platform.createDirectByteBufferFromNativeAddress(address + offset, length);
      }
    }
  }

  public FuryStreamReader getStreamReader() {
    return streamReader;
  }

  /**
   * Compares two memory buffer regions.
   *
   * @param buf2 Buffer to compare this buffer with
   * @param offset1 Offset of this buffer to start comparing
   * @param offset2 Offset of buf2 to start comparing
   * @param len Length of the compared memory region
   * @return 0 if equal, -1 if buf1 &lt; buf2, 1 otherwise
   */
  public int compare(MemoryBuffer buf2, int offset1, int offset2, int len) {
    while (len >= 8) {
      // Since compare is byte-wise, we need to use big endian byte-order.
      long l1 = this.getLongB(offset1);
      long l2 = buf2.getLongB(offset2);

      if (l1 != l2) {
        return (l1 < l2) ^ (l1 < 0) ^ (l2 < 0) ? -1 : 1;
      }

      offset1 += 8;
      offset2 += 8;
      len -= 8;
    }
    while (len > 0) {
      int b1 = this.get(offset1) & 0xff;
      int b2 = buf2.get(offset2) & 0xff;
      int cmp = b1 - b2;
      if (cmp != 0) {
        return cmp;
      }
      offset1++;
      offset2++;
      len--;
    }
    return 0;
  }

  /**
   * Equals two memory buffer regions.
   *
   * @param buf2 Buffer to equal this buffer with
   * @param offset1 Offset of this buffer to start equaling
   * @param offset2 Offset of buf2 to start equaling
   * @param len Length of the equaled memory region
   * @return true if equal, false otherwise
   */
  public boolean equalTo(MemoryBuffer buf2, int offset1, int offset2, int len) {
    final long pos1 = address + offset1;
    final long pos2 = buf2.address + offset2;
    checkArgument(pos1 < addressLimit);
    checkArgument(pos2 < buf2.addressLimit);
    return Platform.arrayEquals(heapMemory, pos1, buf2.heapMemory, pos2, len);
  }

  /**
   * Return a new MemoryBuffer with the same buffer and clear the data (reuse the buffer).
   *
   * @return a new MemoryBuffer object.
   */
  public MemoryBuffer cloneReference() {
    if (heapMemory != null) {
      return new MemoryBuffer(heapMemory, heapOffset, size);
    } else {
      return new MemoryBuffer(address, size, offHeapBuffer);
    }
  }

  @Override
  public String toString() {
    return "MemoryBuffer{"
        + "size="
        + size
        + ", readerIndex="
        + readerIndex
        + ", writerIndex="
        + writerIndex
        + ", heapMemory="
        + (heapMemory == null ? null : "len(" + heapMemory.length + ")")
        + ", heapData="
        + Arrays.toString(heapMemory)
        + ", heapOffset="
        + heapOffset
        + ", offHeapBuffer="
        + offHeapBuffer
        + ", address="
        + address
        + ", addressLimit="
        + addressLimit
        + '}';
  }

  /** Point this buffer to a new byte array. */
  public void pointTo(byte[] buffer, int offset, int length) {
    initHeapBuffer(buffer, offset, length);
  }

  /** Creates a new memory buffer that targets to the given heap memory region. */
  public static MemoryBuffer fromByteArray(byte[] buffer, int offset, int length) {
    return new MemoryBuffer(buffer, offset, length, null);
  }

  public static MemoryBuffer fromByteArray(
      byte[] buffer, int offset, int length, FuryStreamReader streamReader) {
    return new MemoryBuffer(buffer, offset, length, streamReader);
  }

  /** Creates a new memory buffer that targets to the given heap memory region. */
  public static MemoryBuffer fromByteArray(byte[] buffer) {
    return new MemoryBuffer(buffer, 0, buffer.length);
  }

  /**
   * Creates a new memory buffer that represents the memory backing the given byte buffer section of
   * {@code [buffer.position(), buffer.limit())}. The buffer will change into a heap buffer
   * automatically if not enough.
   *
   * @param buffer a direct buffer or heap buffer
   */
  public static MemoryBuffer fromByteBuffer(ByteBuffer buffer) {
    if (buffer.isDirect()) {
      return new MemoryBuffer(
          Platform.getAddress(buffer) + buffer.position(), buffer.remaining(), buffer);
    } else {
      int offset = buffer.arrayOffset() + buffer.position();
      return new MemoryBuffer(buffer.array(), offset, buffer.remaining());
    }
  }

  public static MemoryBuffer fromDirectByteBuffer(
      ByteBuffer buffer, int size, FuryStreamReader streamReader) {
    long offHeapAddress = Platform.getAddress(buffer) + buffer.position();
    return new MemoryBuffer(offHeapAddress, size, buffer, streamReader);
  }

  /**
   * Creates a new memory buffer that represents the provided native memory. The buffer will change
   * into a heap buffer automatically if not enough.
   */
  public static MemoryBuffer fromNativeAddress(long address, int size) {
    return new MemoryBuffer(address, size, null);
  }

  /**
   * Create a heap buffer of specified initial size. The buffer will grow automatically if not
   * enough.
   */
  public static MemoryBuffer newHeapBuffer(int initialSize) {
    return fromByteArray(new byte[initialSize]);
  }
}
