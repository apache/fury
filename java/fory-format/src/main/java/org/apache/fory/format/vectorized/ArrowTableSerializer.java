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

package org.apache.fory.format.vectorized;

import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.fory.Fory;
import org.apache.fory.io.MemoryBufferReadableChannel;
import org.apache.fory.memory.MemoryBuffer;
import org.apache.fory.memory.Platform;
import org.apache.fory.serializer.Serializers;

/** Serializers for {@link ArrowTable}. */
public class ArrowTableSerializer
    extends Serializers.CrossLanguageCompatibleSerializer<ArrowTable> {
  private static final BufferAllocator defaultAllocator =
      ArrowUtils.allocator.newChildAllocator("arrow-table-reader", 64, Long.MAX_VALUE);
  private final BufferAllocator allocator;

  public ArrowTableSerializer(Fory fory) {
    this(fory, defaultAllocator);
  }

  public ArrowTableSerializer(Fory fory, BufferAllocator allocator) {
    super(fory, ArrowTable.class);
    this.allocator = allocator;
  }

  @Override
  public void write(MemoryBuffer buffer, ArrowTable value) {
    fory.writeBufferObject(buffer, new ArrowSerializers.ArrowTableBufferObject(value));
  }

  @Override
  public ArrowTable read(MemoryBuffer buffer) {
    MemoryBuffer buf = fory.readBufferObject(buffer);
    List<ArrowRecordBatch> recordBatches = new ArrayList<>();
    try {
      ReadableByteChannel channel = new MemoryBufferReadableChannel(buf);
      ArrowStreamReader reader = new ArrowStreamReader(channel, allocator);
      VectorSchemaRoot root = reader.getVectorSchemaRoot();
      while (reader.loadNextBatch()) {
        recordBatches.add(new VectorUnloader(root).getRecordBatch());
      }
      return new ArrowTable(root.getSchema(), recordBatches, allocator);
    } catch (Exception e) {
      Platform.throwException(e);
      throw new RuntimeException("unreachable");
    }
  }
}
