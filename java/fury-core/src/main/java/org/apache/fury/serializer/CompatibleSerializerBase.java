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

package org.apache.fury.serializer;

import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;

/**
 * Base class for compatible serializer. Both JIT mode serializer and interpreter-mode serializer
 * will extend this class.
 */
public abstract class CompatibleSerializerBase<T> extends Serializer<T> {
  public CompatibleSerializerBase(Fury fury, Class<T> type) {
    super(fury, type);
  }

  public T readAndSetFields(MemoryBuffer buffer, T obj) {
    // java record object doesn't support update state.
    throw new UnsupportedOperationException();
  }
}
