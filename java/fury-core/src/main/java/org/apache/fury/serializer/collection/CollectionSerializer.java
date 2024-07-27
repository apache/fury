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

package org.apache.fury.serializer.collection;

import java.util.Collection;
import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.resolver.ClassInfo;
import org.apache.fury.resolver.ClassResolver;

/** Base serializer for all java collections. */
@SuppressWarnings({"unchecked", "rawtypes"})
public class CollectionSerializer<T extends Collection> extends AbstractCollectionSerializer<T> {
  public CollectionSerializer(Fury fury, Class<T> type) {
    super(fury, type);
  }

  public CollectionSerializer(Fury fury, Class<T> type, boolean supportCodegenHook) {
    super(fury, type, supportCodegenHook);
  }

  public CollectionSerializer(
      Fury fury, Class<T> type, boolean supportCodegenHook, boolean immutable) {
    super(fury, type, supportCodegenHook, immutable);
  }

  @Override
  public Collection onCollectionWrite(MemoryBuffer buffer, T value) {
    buffer.writeVarUint32Small7(value.size());
    return value;
  }

  @Override
  public T onCollectionRead(Collection collection) {
    return (T) collection;
  }

  @Override
  public T copy(T originCollection) {
    if (isImmutable()) {
      return originCollection;
    }
    Collection newCollection = newCollection(originCollection);
    if (needToCopyRef) {
      fury.reference(originCollection, newCollection);
    }
    copyElements(originCollection, newCollection);
    return (T) newCollection;
  }

  public void copyElements(T originCollection, Collection newCollection) {
    ClassResolver classResolver = fury.getClassResolver();
    for (Object element : originCollection) {
      if (element != null) {
        ClassInfo classInfo =
            classResolver.getClassInfo(element.getClass(), elementClassInfoHolder);
        if (!classInfo.getSerializer().isImmutable()) {
          element = fury.copyObject(element, classInfo.getClassId());
        }
      }
      newCollection.add(element);
    }
  }

  @Override
  public T read(MemoryBuffer buffer) {
    Collection collection = newCollection(buffer);
    int numElements = getAndClearNumElements();
    if (numElements != 0) {
      readElements(fury, buffer, collection, numElements);
    }
    return onCollectionRead(collection);
  }
}
