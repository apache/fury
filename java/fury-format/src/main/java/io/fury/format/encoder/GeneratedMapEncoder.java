package io.fury.format.encoder;

import io.fury.builder.Generated;
import io.fury.format.row.binary.BinaryArray;
import io.fury.format.row.binary.BinaryMap;

/** A map row format codec for java bean. */
public interface GeneratedMapEncoder extends Generated {

  BinaryMap toMap(Object obj);

  default Object fromMap(BinaryMap array) {
    return fromMap(array.keyArray(), array.valueArray());
  }

  Object fromMap(BinaryArray key, BinaryArray value);
}
