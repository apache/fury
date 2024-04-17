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

package org.apache.fury.memory;

import java.util.function.Function;

/** A class that manages MemoryBuffer for Fury. */
public class MemoryBufferManager {
  private static final int BUFFER_SIZE_LIMIT = 128 * 1024;
  private static final int INITIAL_SIZE = 64;
  private MemoryBuffer buffer;

  public <R> R execute(Function<MemoryBuffer, R> action) {
    try {
      MemoryBuffer buf = getBuffer();
      return action.apply(buf);
    } finally {
      resetBuffer();
    }
  }

  private MemoryBuffer getBuffer() {
    MemoryBuffer buf = buffer;
    if (buf == null) {
      buf = buffer = MemoryBuffer.newHeapBuffer(INITIAL_SIZE);
    }
    return buf;
  }

  private void resetBuffer() {
    MemoryBuffer buf = buffer;
    if (buf != null && buf.size() > BUFFER_SIZE_LIMIT) {
      buffer = MemoryBuffer.newHeapBuffer(BUFFER_SIZE_LIMIT);
    }
    if (buf != null) {
      buf.writerIndex(0);
    }
  }
}
