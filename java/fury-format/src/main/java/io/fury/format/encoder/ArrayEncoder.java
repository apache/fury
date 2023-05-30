package io.fury.format.encoder;

import io.fury.format.row.binary.BinaryArray;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Encoder to encode/decode object in the list container by toArray/fromArray row.
 *
 * @author ganrunsheng
 */
public interface ArrayEncoder<T> extends Encoder<T> {
  Field field();

  T fromArray(BinaryArray array);

  BinaryArray toArray(T obj);
}
