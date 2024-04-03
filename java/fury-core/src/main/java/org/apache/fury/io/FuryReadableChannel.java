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
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import org.apache.fury.memory.MemoryBuffer;

// TODO support zero-copy channel reading.
public class FuryReadableChannel extends AbstractStreamReader implements ReadableByteChannel {
  private final ReadableByteChannel channel;
  private final ByteBuffer byteBuffer;
  private final MemoryBuffer buffer;

  public FuryReadableChannel(ReadableByteChannel channel) {
    this(channel, ByteBuffer.allocate(4096));
  }

  private FuryReadableChannel(ReadableByteChannel channel, ByteBuffer directBuffer) {
    this.channel = channel;
    this.byteBuffer = directBuffer;
    this.buffer = MemoryBuffer.fromByteBuffer(directBuffer);
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    throw new UnsupportedEncodingException();
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
    return buffer;
  }
}
