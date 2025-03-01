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

/**
 * Serializer for immutable objects.
 *
 * @param <T> type of objects being serializing/deserializing
 */
public abstract class ImmutableSerializer<T> extends Serializer<T> {

  public ImmutableSerializer(Fury fury, Class<T> type) {
    super(fury, type, true);
  }

  public ImmutableSerializer(Fury fury, Class<T> type, boolean needToWriteRef) {
    super(fury, type, needToWriteRef, true);
  }

  public ImmutableSerializer(Fury fury, Class<T> type, boolean needToWriteRef, boolean immutable) {
    super(fury, type, needToWriteRef, immutable);
  }
}
