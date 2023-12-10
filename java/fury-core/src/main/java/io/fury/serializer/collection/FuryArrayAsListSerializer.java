package io.fury.serializer.collection;

import io.fury.Fury;
import io.fury.annotation.Internal;
import io.fury.memory.MemoryBuffer;
import io.fury.type.Type;

import java.util.Collection;

/**
 * Serializer for {@link ArrayAsList}.
 * Helper for serialization of other classes.
 *
 * @author chaokunyang
 */
@Internal
@SuppressWarnings("rawtypes")
public final class FuryArrayAsListSerializer extends CollectionSerializer<ArrayAsList> {
  public FuryArrayAsListSerializer(Fury fury) {
    super(fury, ArrayAsList.class, true);
  }

  @Override
  public short getXtypeId() {
    return (short) -Type.LIST.getId();
  }

  public Collection newCollection(MemoryBuffer buffer) {
    numElements = buffer.readPositiveVarInt();
    return new ArrayAsList(numElements);
  }
}