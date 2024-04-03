package org.apache.fury.io;

import java.nio.ByteBuffer;
import org.apache.fury.memory.MemoryBuffer;

/** An abstract {@link FuryStreamReader} for subclass implementation convenience. */
public abstract class AbstractStreamReader implements FuryStreamReader {
  @Override
  public int fillBuffer(int minFillSize) {
    return 0;
  }

  @Override
  public void readTo(byte[] dst, int dstIndex, int length) {}

  @Override
  public void readToUnsafe(Object target, long targetPointer, int numBytes) {}

  @Override
  public void readToByteBuffer(ByteBuffer dst, int pos, int length) {}

  @Override
  public MemoryBuffer getBuffer() {
    return null;
  }
}
