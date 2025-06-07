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

package org.apache.fory.format.type;

import org.apache.fory.annotation.Internal;
import org.apache.fory.format.encoder.CustomCodec;
import org.apache.fory.format.encoder.CustomCollectionFactory;
import org.apache.fory.format.row.binary.BinaryArray;
import org.apache.fory.memory.MemoryBuffer;
import org.apache.fory.reflect.TypeRef;
import org.apache.fory.type.CustomTypeRegistry;

@Internal
public interface CustomTypeHandler extends CustomTypeRegistry {
  static final CustomTypeHandler EMPTY =
      new CustomTypeHandler() {
        @Override
        public <T> CustomCodec<T, ?> findCodec(final Class<?> beanType, final Class<T> fieldType) {
          return null;
        }

        @Override
        public CustomCollectionFactory<?, ?> findCollectionFactory(
            final Class<?> collectionType, final Class<?> elementType) {
          return null;
        }
      };

  <T> CustomCodec<T, ?> findCodec(Class<?> beanType, Class<T> fieldType);

  CustomCollectionFactory<?, ?> findCollectionFactory(
      Class<?> collectionType, Class<?> elementType);

  @Override
  default TypeRef<?> replacementTypeFor(final Class<?> beanType, final Class<?> fieldType) {
    final CustomCodec<?, ?> codec = findCodec(beanType, fieldType);
    return codec == null ? null : codec.encodedType();
  }

  @Override
  default boolean canConstructCollection(
      final Class<?> collectionType, final Class<?> elementType) {
    return findCollectionFactory(collectionType, elementType) != null;
  }

  @Override
  default boolean isExtraSupportedType(final TypeRef<?> type) {
    final Class<?> cls = type.getRawType();
    return cls == BinaryArray.class || cls == MemoryBuffer.class;
  }
}
