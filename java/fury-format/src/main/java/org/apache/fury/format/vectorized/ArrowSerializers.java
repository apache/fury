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

package org.apache.fury.format.vectorized;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.ipc.WriteChannel;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.fury.Fury;
import org.apache.fury.io.MemoryBufferReadableChannel;
import org.apache.fury.io.MemoryBufferWritableChannel;
import org.apache.fury.io.MockWritableChannel;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.serializer.BufferObject;
import org.apache.fury.serializer.Serializers.CrossLanguageCompatibleSerializer;
import org.apache.fury.type.Type;
import org.apache.fury.util.Platform;

/** Serializers for apache arrow. */
public class ArrowSerializers {

  /** Use {@link ArrowTableSerializer} is more recommended. */
  public static class VectorSchemaRootSerializer
      extends CrossLanguageCompatibleSerializer<VectorSchemaRoot> {
    private static final BufferAllocator defaultAllocator =
        ArrowUtils.allocator.newChildAllocator(
            "arrow-vector-schema-root-reader", 64, Long.MAX_VALUE);
    private final BufferAllocator allocator;

    public VectorSchemaRootSerializer(Fury fury) {
      this(fury, defaultAllocator);
    }

    public VectorSchemaRootSerializer(Fury fury, BufferAllocator allocator) {
      super(fury, VectorSchemaRoot.class, Type.FURY_ARROW_RECORD_BATCH.getId());
      this.allocator = allocator;
    }

    @Override
    public void write(MemoryBuffer buffer, VectorSchemaRoot root) {
      fury.writeBufferObject(buffer, new VectorSchemaRootBufferObject(root));
    }

    @Override
    public VectorSchemaRoot read(MemoryBuffer buffer) {
      MemoryBuffer buf = fury.readBufferObject(buffer);
      try {
        ReadableByteChannel channel = new MemoryBufferReadableChannel(buf);
        ArrowStreamReader reader = new ArrowStreamReader(channel, allocator);
        // FIXME close reader will close `root`.
        // since there is no possibility for resource leak, we can skip `reader.close`
        // and let the user to close `root`.
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        // Only single record batch are supported for now.
        // Since call loadNextBatch again will clear previous loaded data, so that we can't
        // check `reader.loadNextBatch()` again to check whether there is any batch left.
        reader.loadNextBatch();
        return root;
      } catch (Exception e) {
        throw new RuntimeException("Unable to read a record batch message", e);
      }
    }
  }

  private static class VectorSchemaRootBufferObject implements BufferObject {
    private final int totalBytes;
    private final VectorSchemaRoot root;

    VectorSchemaRootBufferObject(VectorSchemaRoot root) {
      this.root = root;
      MockWritableChannel mockWritableChannel = new MockWritableChannel();
      write(root, mockWritableChannel);
      totalBytes = mockWritableChannel.totalBytes();
    }

    @Override
    public int totalBytes() {
      return totalBytes;
    }

    @Override
    public void writeTo(MemoryBuffer buffer) {
      write(root, new MemoryBufferWritableChannel(buffer));
    }

    private static void write(VectorSchemaRoot root, WritableByteChannel byteChannel) {
      try (ArrowStreamWriter writer = new ArrowStreamWriter(root, null, byteChannel)) {
        writer.writeBatch();
      } catch (IOException e) {
        Platform.throwException(e);
      }
    }

    @Override
    public MemoryBuffer toBuffer() {
      MemoryBuffer buffer = MemoryUtils.buffer(totalBytes);
      write(root, new MemoryBufferWritableChannel(buffer));
      return buffer.slice(0, buffer.writerIndex());
    }
  }

  public static class ArrowTableBufferObject implements BufferObject {
    private final ArrowTable table;
    private final int totalBytes;

    public ArrowTableBufferObject(ArrowTable table) {
      this.table = table;
      MockWritableChannel mockWritableChannel = new MockWritableChannel();
      write(table, mockWritableChannel);
      totalBytes = mockWritableChannel.totalBytes();
    }

    @Override
    public int totalBytes() {
      return totalBytes;
    }

    @Override
    public void writeTo(MemoryBuffer buffer) {
      write(table, new MemoryBufferWritableChannel(buffer));
    }

    private static void write(ArrowTable table, WritableByteChannel byteChannel) {
      try (WriteChannel channel = new WriteChannel(byteChannel)) {
        MessageSerializer.serialize(channel, table.getSchema());
        for (ArrowRecordBatch recordBatch : table.getRecordBatches()) {
          MessageSerializer.serialize(channel, recordBatch);
        }
        ArrowStreamWriter.writeEndOfStream(channel, new IpcOption());
      } catch (IOException e) {
        Platform.throwException(e);
      }
    }

    @Override
    public MemoryBuffer toBuffer() {
      MemoryBuffer buffer = MemoryUtils.buffer(totalBytes);
      write(table, new MemoryBufferWritableChannel(buffer));
      return buffer.slice(0, buffer.writerIndex());
    }
  }

  public static void registerSerializers(Fury fury) {
    fury.registerSerializer(ArrowTable.class, new ArrowTableSerializer(fury));
    fury.registerSerializer(VectorSchemaRoot.class, new VectorSchemaRootSerializer(fury));
  }
}
