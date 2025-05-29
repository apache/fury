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

import java.util.Iterator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fory.util.Preconditions;

/** A custom pyarrow-style arrow table by attach {@link Schema} to {@link ArrowRecordBatch}. */
public class ArrowTable {
  private static final BufferAllocator tableBufferAllocator =
      ArrowUtils.allocator.newChildAllocator("table_buffer_allocator", 64, Long.MAX_VALUE);
  private final Schema schema;
  private final BufferAllocator allocator;
  private Iterable<ArrowRecordBatch> recordBatches;
  private Iterator<ArrowRecordBatch> batchIterator;
  private VectorSchemaRoot root;

  public ArrowTable(Schema schema, Iterable<ArrowRecordBatch> recordBatches) {
    this(schema, recordBatches, tableBufferAllocator);
  }

  public ArrowTable(
      Schema schema, Iterable<ArrowRecordBatch> recordBatches, BufferAllocator allocator) {
    this.schema = schema;
    this.recordBatches = recordBatches;
    this.allocator = allocator;
  }

  public Schema getSchema() {
    return schema;
  }

  public Iterable<ArrowRecordBatch> getRecordBatches() {
    return recordBatches;
  }

  public VectorSchemaRoot toVectorSchemaRoot() {
    return toVectorSchemaRoot(false);
  }

  public VectorSchemaRoot toVectorSchemaRoot(boolean reload) {
    if (!reload) {
      Preconditions.checkArgument(batchIterator == null);
    }
    batchIterator = recordBatches.iterator();
    root = VectorSchemaRoot.create(schema, allocator);
    return root;
  }

  public boolean loadNextBatch() {
    VectorLoader loader = new VectorLoader(root);
    if (batchIterator.hasNext()) {
      loader.load(batchIterator.next());
      return true;
    } else {
      return false;
    }
  }
}
