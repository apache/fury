/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fory.format.encoder;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.fory.format.row.binary.BinaryArray;
import org.apache.fory.format.type.DataTypes;
import org.apache.fory.memory.MemoryBuffer;

public interface CustomCodec<T, E> {
  Field getField(String fieldName);

  Class<E> encodedType();

  E encode(T value);

  T decode(E value);

  interface MemoryBufferCodec<T> extends CustomCodec<T, MemoryBuffer> {
    @Override
    default Class<MemoryBuffer> encodedType() {
      return MemoryBuffer.class;
    }

    @Override
    default Field getField(final String fieldName) {
      return Field.nullable(fieldName, ArrowType.Binary.INSTANCE);
    }
  }

  interface ByteArrayCodec<T> extends CustomCodec<T, byte[]> {
    @Override
    default Class<byte[]> encodedType() {
      return byte[].class;
    }

    @Override
    default Field getField(final String fieldName) {
      return DataTypes.primitiveArrayField(fieldName, DataTypes.int8());
    }
  }

  interface BinaryArrayCodec<T> extends CustomCodec<T, BinaryArray> {
    @Override
    default Class<BinaryArray> encodedType() {
      return BinaryArray.class;
    }

    @Override
    default Field getField(final String fieldName) {
      return DataTypes.primitiveArrayField(fieldName, DataTypes.int8());
    }
  }
}
