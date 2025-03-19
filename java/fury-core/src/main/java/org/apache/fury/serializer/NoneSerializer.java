package org.apache.fury.serializer;

import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;

@SuppressWarnings("rawtypes")
public class NoneSerializer extends Serializer {
  public NoneSerializer(Fury fury, Class type) {
    super(fury, type);
  }

  @Override
  public void write(MemoryBuffer buffer, Object value) {}

  @Override
  public Object read(MemoryBuffer buffer) {
    return null;
  }

  @Override
  public void xwrite(MemoryBuffer buffer, Object value) {}

  @Override
  public Object xread(MemoryBuffer buffer) {
    return null;
  }
}
