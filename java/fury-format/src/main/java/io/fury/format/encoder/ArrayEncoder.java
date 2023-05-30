package io.fury.format.encoder;

import io.fury.format.row.binary.BinaryArray;
import org.apache.arrow.vector.types.pojo.Field;

public interface ArrayEncoder<T> extends Encoder<T> {
  Field field();

  T fromArray(BinaryArray array);

  BinaryArray toArray(T obj);
}
