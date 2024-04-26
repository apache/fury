package org.apache.fury.memory;

import org.apache.fury.util.Preconditions;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

public class ByteBufferUtil {
  private static final long BUFFER_ADDRESS_FIELD_OFFSET;
  private static final long BUFFER_CAPACITY_FIELD_OFFSET;

  static {
    try {
      Field addressField = Buffer.class.getDeclaredField("address");
      BUFFER_ADDRESS_FIELD_OFFSET = Platform.objectFieldOffset(addressField);
      Preconditions.checkArgument(BUFFER_ADDRESS_FIELD_OFFSET != 0);
      Field capacityField = Buffer.class.getDeclaredField("capacity");
      BUFFER_CAPACITY_FIELD_OFFSET = Platform.objectFieldOffset(capacityField);
      Preconditions.checkArgument(BUFFER_CAPACITY_FIELD_OFFSET != 0);
    } catch (NoSuchFieldException e) {
      throw new IllegalStateException(e);
    }
  }

  public static long getAddress(ByteBuffer buffer) {
    Preconditions.checkNotNull(buffer, "buffer is null");
    Preconditions.checkArgument(buffer.isDirect(), "Can't get address of a non-direct ByteBuffer.");
    long offHeapAddress;
    try {
      offHeapAddress = Platform.getLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET);
    } catch (Throwable t) {
      throw new Error("Could not access direct byte buffer address field.", t);
    }
    return offHeapAddress;
  }

  private static final ByteBuffer localBuffer = ByteBuffer.allocateDirect(0);

  /** Create a direct buffer from native memory represented by address [address, address + size). */
  public static ByteBuffer createDirectByteBufferFromNativeAddress(long address, int size) {
    try {
      // ByteBuffer.allocateDirect(0) is about 30x slower than `localBuffer.duplicate()`.
      ByteBuffer buffer = localBuffer.duplicate();
      Platform.putLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET, address);
      Platform.putInt(buffer, BUFFER_CAPACITY_FIELD_OFFSET, size);
      buffer.clear();
      return buffer;
    } catch (Throwable t) {
      throw new Error("Failed to wrap unsafe off-heap memory with ByteBuffer", t);
    }
  }

  /** Wrap a buffer [address, address + size) into provided <code>buffer</code>. */
  public static void wrapDirectByteBufferFromNativeAddress(
      ByteBuffer buffer, long address, int size) {
    Preconditions.checkArgument(
        buffer.isDirect(), "Can't wrap native memory into a non-direct ByteBuffer.");
    Platform.putLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET, address);
    Platform.putInt(buffer, BUFFER_CAPACITY_FIELD_OFFSET, size);
    buffer.clear();
  }

  public static ByteBuffer wrapDirectBuffer(long address, int size) {
    return createDirectByteBufferFromNativeAddress(address, size);
  }

  /** Wrap a buffer [address, address + size) into provided <code>buffer</code>. */
  public static void wrapDirectBuffer(ByteBuffer buffer, long address, int size) {
    Platform.putLong(buffer, BUFFER_ADDRESS_FIELD_OFFSET, address);
    Platform.putInt(buffer, BUFFER_CAPACITY_FIELD_OFFSET, size);
    buffer.clear();
  }

  public static void clearBuffer(Buffer buffer) {
    buffer.clear();
  }

  public static void flipBuffer(Buffer buffer) {
    buffer.flip();
  }

  public static void rewind(Buffer buffer) {
    buffer.rewind();
  }
}
