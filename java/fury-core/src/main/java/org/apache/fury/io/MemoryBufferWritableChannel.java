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

import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import org.apache.fury.memory.MemoryBuffer;

/** {@link WritableByteChannel} based on fury {@link MemoryBuffer}. */
public class MemoryBufferWritableChannel implements WritableByteChannel {
  private boolean open = true;
  private final MemoryBuffer buffer;

  public MemoryBufferWritableChannel(MemoryBuffer buffer) {
    this.buffer = buffer;
  }

  @Override
  public int write(ByteBuffer src) {
    int remaining = src.remaining();
    buffer.write(src, remaining);
    return remaining;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public void close() {
    open = false;
  }
}
