package io.fury.format.encoder;

import io.fury.builder.Generated;
import io.fury.format.row.binary.BinaryArray;

/** A array row format codec for java bean. */
public interface GeneratedArrayEncoder extends Generated {

  BinaryArray toArray(Object obj);

  Object fromArray(BinaryArray array);
}
