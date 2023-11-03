package io.fury.serializer.collection;

import io.fury.Fury;
import io.fury.memory.MemoryBuffer;

import java.util.Collection;

public class CollectionSerializer<T extends Collection>
  extends BaseCollectionSerializer<T> {
  public CollectionSerializer(Fury fury, Class<T> type) {
    super(fury, type);
  }

  public CollectionSerializer(Fury fury, Class<T> type, boolean supportCodegenHook) {
    super(fury, type, supportCodegenHook);
  }

  @Override
  public Collection onCollectionWrite(MemoryBuffer buffer, T value) {
    buffer.writePositiveVarInt(value.size());
    return value;
  }

  @Override
  public T onCollectionRead(Collection collection) {
    return (T) collection;
  }

  @Override
  public T read(MemoryBuffer buffer) {
    Collection collection = newCollection(buffer);
    if (numElements != 0) {
      readElements(fury, buffer, collection, numElements);
    }
    return onCollectionRead(collection);
  }
}
