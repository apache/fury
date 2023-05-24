package io.fury.io;

import org.testng.annotations.Test;

import java.nio.ByteBuffer;

import static org.testng.Assert.assertEquals;

public class MockWritableByteChannelTest {

  @Test
  public void testTotalBytes() {
    try (MockWritableByteChannel channel = new MockWritableByteChannel()) {
      channel.write(ByteBuffer.allocate(100));
      channel.write(ByteBuffer.allocateDirect(100));
      ByteBuffer buffer = ByteBuffer.allocate(100);
      buffer.position(50);
      channel.write(buffer);
      assertEquals(channel.totalBytes(), 250);
    }
  }
}