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

package io.fury.serializer.collection;

import io.fury.Fury;
import io.fury.memory.MemoryBuffer;
import java.util.Collection;

/**
 * Base serializer for all java collections.
 *
 * @author chaokunyang
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class CollectionSerializer<T extends Collection> extends AbstractCollectionSerializer<T> {
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
