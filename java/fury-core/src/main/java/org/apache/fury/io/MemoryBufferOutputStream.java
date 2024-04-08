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

import java.io.OutputStream;
import java.nio.ByteBuffer;
import org.apache.fury.memory.MemoryBuffer;

/** OutputStream based on {@link MemoryBuffer}. */
public class MemoryBufferOutputStream extends OutputStream {
  private final MemoryBuffer buffer;

  public MemoryBufferOutputStream(MemoryBuffer buffer) {
    this.buffer = buffer;
  }

  public void write(int b) {
    buffer.writeByte((byte) b);
  }

  public void write(byte[] bytes, int offset, int length) {
    buffer.writeBytes(bytes, offset, length);
  }

  public void write(ByteBuffer byteBuffer, int numBytes) {
    buffer.write(byteBuffer, numBytes);
  }
}
