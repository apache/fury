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
  <T> void writeRef(MemoryBuffer buffer, T obj, Serializer<T> serializer);

  void writeRef(MemoryBuffer buffer, Object obj, ClassInfoHolder classInfoHolder);

  <T> T readRef(MemoryBuffer buffer, Serializer<T> serializer);

  Object readRef(MemoryBuffer buffer, ClassInfoHolder classInfoHolder);

  void write(MemoryBuffer buffer, Serializer serializer, Object value);

  Object read(MemoryBuffer buffer, Serializer serializer);

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
    public void write(MemoryBuffer buffer, Serializer serializer, Object value) {
      serializer.write(buffer, value);
    }

    @Override
    public Object read(MemoryBuffer buffer, Serializer serializer) {
      return serializer.read(buffer);
    }
  }

  final class XlangSerializationBinding implements SerializationBinding {

    private final Fury fury;

    XlangSerializationBinding(Fury fury) {
      this.fury = fury;
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
    public void write(MemoryBuffer buffer, Serializer serializer, Object value) {
      serializer.xwrite(buffer, value);
    }

    @Override
    public Object read(MemoryBuffer buffer, Serializer serializer) {
      return serializer.xread(buffer);
    }
  }
}
