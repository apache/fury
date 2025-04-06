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
import org.apache.fury.resolver.ClassInfo;
import org.apache.fury.resolver.ClassInfoHolder;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.XtypeResolver;

// This polymorphic interface has cost, do not expose it as a public class
// If it's used in other packages in fury, duplicate it in those packages.
@SuppressWarnings({"rawtypes", "unchecked"})
// noinspection Duplicates
interface SerializationBinding {
  <T> void writeRef(MemoryBuffer buffer, T obj);

  <T> void writeRef(MemoryBuffer buffer, T obj, Serializer<T> serializer);

  void writeRef(MemoryBuffer buffer, Object obj, ClassInfoHolder classInfoHolder);

  void writeNonRef(MemoryBuffer buffer, Object obj);

  void writeNonRef(MemoryBuffer buffer, Object obj, ClassInfo classInfo);

  void writeNullable(MemoryBuffer buffer, Object obj);

  void writeNullable(MemoryBuffer buffer, Object obj, Serializer serializer);

  void writeNullable(MemoryBuffer buffer, Object obj, ClassInfoHolder classInfoHolder);

  void writeNullable(MemoryBuffer buffer, Object obj, ClassInfo classInfo);

  void write(MemoryBuffer buffer, Serializer serializer, Object value);

  Object read(MemoryBuffer buffer, Serializer serializer);

  <T> T readRef(MemoryBuffer buffer, Serializer<T> serializer);

  Object readRef(MemoryBuffer buffer, ClassInfoHolder classInfoHolder);

  Object readRef(MemoryBuffer buffer);

  Object readNonRef(MemoryBuffer buffer);

  Object readNonRef(MemoryBuffer buffer, ClassInfoHolder classInfoHolder);

  Object readNullable(MemoryBuffer buffer, Serializer<Object> serializer);

  static SerializationBinding createBinding(Fury fury) {
    if (fury.isCrossLanguage()) {
      return new XlangSerializationBinding(fury);
    } else {
      return new JavaSerializationBinding(fury);
    }
  }

  final class JavaSerializationBinding implements SerializationBinding {
    private final Fury fury;
    private final ClassResolver classResolver;

    JavaSerializationBinding(Fury fury) {
      this.fury = fury;
      classResolver = fury.getClassResolver();
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
    public Object readNonRef(MemoryBuffer buffer, ClassInfoHolder classInfoHolder) {
      return fury.readNonRef(buffer, classInfoHolder);
    }

    @Override
    public Object readNullable(MemoryBuffer buffer, Serializer<Object> serializer) {
      return fury.readNullable(buffer, serializer);
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
    public void writeNonRef(MemoryBuffer buffer, Object obj) {
      fury.writeNonRef(buffer, obj);
    }

    @Override
    public void writeNonRef(MemoryBuffer buffer, Object obj, ClassInfo classInfo) {
      fury.writeNonRef(buffer, obj, classInfo);
    }

    @Override
    public void writeNullable(MemoryBuffer buffer, Object obj) {
      if (obj == null) {
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
        writeNonRef(buffer, obj);
      }
    }

    @Override
    public void writeNullable(MemoryBuffer buffer, Object obj, Serializer serializer) {
      if (obj == null) {
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
        serializer.write(buffer, obj);
      }
    }

    @Override
    public void writeNullable(MemoryBuffer buffer, Object obj, ClassInfoHolder classInfoHolder) {
      if (obj == null) {
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
        fury.writeNonRef(buffer, obj, classResolver.getClassInfo(obj.getClass(), classInfoHolder));
      }
    }

    @Override
    public void writeNullable(MemoryBuffer buffer, Object obj, ClassInfo classInfo) {
      if (obj == null) {
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
        fury.writeNonRef(buffer, obj, classInfo);
      }
    }
  }

  final class XlangSerializationBinding implements SerializationBinding {

    private final Fury fury;
    private final XtypeResolver xtypeResolver;

    XlangSerializationBinding(Fury fury) {
      this.fury = fury;
      xtypeResolver = fury.getXtypeResolver();
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
    public Object readNonRef(MemoryBuffer buffer, ClassInfoHolder classInfoHolder) {
      return fury.xreadNonRef(buffer, xtypeResolver.readClassInfo(buffer, classInfoHolder));
    }

    @Override
    public Object readNullable(MemoryBuffer buffer, Serializer<Object> serializer) {
      return fury.xreadNullable(buffer, serializer);
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
    public void writeNonRef(MemoryBuffer buffer, Object obj) {
      fury.xwriteNonRef(buffer, obj);
    }

    @Override
    public void writeNonRef(MemoryBuffer buffer, Object obj, ClassInfo classInfo) {
      fury.xwriteNonRef(buffer, obj, classInfo);
    }

    @Override
    public void writeNullable(MemoryBuffer buffer, Object obj) {
      if (obj == null) {
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
        fury.xwriteNonRef(buffer, obj);
      }
    }

    @Override
    public void writeNullable(MemoryBuffer buffer, Object obj, Serializer serializer) {
      if (obj == null) {
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
        serializer.xwrite(buffer, obj);
      }
    }

    @Override
    public void writeNullable(MemoryBuffer buffer, Object obj, ClassInfoHolder classInfoHolder) {
      if (obj == null) {
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
        fury.xwriteNonRef(buffer, obj, xtypeResolver.getClassInfo(obj.getClass(), classInfoHolder));
      }
    }

    @Override
    public void writeNullable(MemoryBuffer buffer, Object obj, ClassInfo classInfo) {
      if (obj == null) {
        buffer.writeByte(Fury.NULL_FLAG);
      } else {
        buffer.writeByte(Fury.NOT_NULL_VALUE_FLAG);
        fury.xwriteNonRef(buffer, obj, classInfo);
      }
    }
  }
}
