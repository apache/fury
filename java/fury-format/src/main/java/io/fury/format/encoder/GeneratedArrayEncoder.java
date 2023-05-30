package io.fury.format.encoder;

import io.fury.builder.Generated;
import io.fury.format.row.binary.BinaryArray;

/**
 * A list container row format codec for java bean.
 *
 * @author ganrunsheng
 */
public interface GeneratedArrayEncoder extends Generated {

  BinaryArray toArray(Object obj);

  Object fromArray(BinaryArray array);
}
