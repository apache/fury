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

package org.apache.fury.serializer.collection;

import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.resolver.ClassInfoHolder;
import org.apache.fury.serializer.Serializer;

// This polymorphic interface has cost, do not expose it as a public class
// If it's used in other packages in fury, duplicate it in those packages.
@SuppressWarnings({"rawtypes", "unchecked"})
// noinspection Duplicates
interface SerializationBinding {
  <T> void writeRef(MemoryBuffer buffer, T obj);

  <T> void writeRef(MemoryBuffer buffer, T obj, Serializer<T> serializer);

  void writeRef(MemoryBuffer buffer, Object obj, ClassInfoHolder classInfoHolder);

  void writeNonRef(MemoryBuffer buffer, Object elem);

  void write(MemoryBuffer buffer, Serializer serializer, Object value);

  Object read(MemoryBuffer buffer, Serializer serializer);

  <T> T readRef(MemoryBuffer buffer, Serializer<T> serializer);

  Object readRef(MemoryBuffer buffer, ClassInfoHolder classInfoHolder);

  Object readRef(MemoryBuffer buffer);

  Object readNonRef(MemoryBuffer buffer);

  static SerializationBinding createBinding(Fury fury) {
    if (fury.isCrossLanguage()) {
      return new XlangSerializationBinding(fury);
    } else {
      return new JavaSerializationBinding(fury);
    }
  }

  final class JavaSerializationBinding implements SerializationBinding {
    private final Fury fury;

    JavaSerializationBinding(Fury fury) {
      this.fury = fury;
    }

    @Override
    public <T> void writeRef(MemoryBuffer buffer, T obj) {
      fury.writeRef(buffer, obj);
    }

    @Override
    public <T> void writeRef(MemoryBuffer buffer, T obj, Serializer<T> serializer) {
      fury.writeRef(buffer, obj, serializer);
    }

    @Override
    public void writeRef(MemoryBuffer buffer, Object obj, ClassInfoHolder classInfoHolder) {
      fury.writeRef(buffer, obj, classInfoHolder);
    }

    @Override
    public <T> T readRef(MemoryBuffer buffer, Serializer<T> serializer) {
      return fury.readRef(buffer, serializer);
    }

    @Override
    public Object readRef(MemoryBuffer buffer, ClassInfoHolder classInfoHolder) {
      return fury.readRef(buffer, classInfoHolder);
    }

    @Override
    public Object readRef(MemoryBuffer buffer) {
      return fury.readRef(buffer);
    }

    @Override
    public Object readNonRef(MemoryBuffer buffer) {
      return fury.readNonRef(buffer);
    }

    @Override
    public void write(MemoryBuffer buffer, Serializer serializer, Object value) {
      serializer.write(buffer, value);
    }

    @Override
    public Object read(MemoryBuffer buffer, Serializer serializer) {
      return serializer.read(buffer);
    }

    @Override
    public void writeNonRef(MemoryBuffer buffer, Object elem) {
      fury.writeNonRef(buffer, elem);
    }
  }

  final class XlangSerializationBinding implements SerializationBinding {

    private final Fury fury;

    XlangSerializationBinding(Fury fury) {
      this.fury = fury;
    }

    @Override
    public <T> void writeRef(MemoryBuffer buffer, T obj) {
      fury.xwriteRef(buffer, obj);
    }

    @Override
    public <T> void writeRef(MemoryBuffer buffer, T obj, Serializer<T> serializer) {
      fury.xwriteRef(buffer, obj, serializer);
    }

    @Override
    public void writeRef(MemoryBuffer buffer, Object obj, ClassInfoHolder classInfoHolder) {
      fury.xwriteRef(buffer, obj);
    }

    @Override
    public <T> T readRef(MemoryBuffer buffer, Serializer<T> serializer) {
      return (T) fury.xreadRef(buffer, serializer);
    }

    @Override
    public Object readRef(MemoryBuffer buffer, ClassInfoHolder classInfoHolder) {
      return fury.xreadRef(buffer);
    }

    @Override
    public Object readRef(MemoryBuffer buffer) {
      return fury.xreadRef(buffer);
    }

    @Override
    public Object readNonRef(MemoryBuffer buffer) {
      return fury.xreadNonRef(buffer);
    }

    @Override
    public void write(MemoryBuffer buffer, Serializer serializer, Object value) {
      serializer.xwrite(buffer, value);
    }

    @Override
    public Object read(MemoryBuffer buffer, Serializer serializer) {
      return serializer.xread(buffer);
    }

    @Override
    public void writeNonRef(MemoryBuffer buffer, Object elem) {
      fury.xwriteNonRef(buffer, elem);
    }
  }
}
