/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
