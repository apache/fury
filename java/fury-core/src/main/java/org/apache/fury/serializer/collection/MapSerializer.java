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

import java.util.Map;
import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;

/** Base serializer for all java maps. */
@SuppressWarnings({"unchecked", "rawtypes"})
public class MapSerializer<T extends Map> extends AbstractMapSerializer<T> {
  public MapSerializer(Fury fury, Class<T> cls) {
    super(fury, cls);
  }

  public MapSerializer(Fury fury, Class<T> cls, boolean supportCodegenHook) {
    super(fury, cls, supportCodegenHook);
  }

  public MapSerializer(Fury fury, Class<T> cls, boolean supportCodegenHook, boolean immutable) {
    super(fury, cls, supportCodegenHook, immutable);
  }

  @Override
  public Map onMapWrite(MemoryBuffer buffer, T value) {
    buffer.writeVarUint32Small7(value.size());
    return value;
  }

  @Override
  public T copy(T originMap) {
    if (isImmutable()) {
      return originMap;
    }
    Map newMap = newMap(originMap);
    if (needToCopyRef) {
      fury.reference(originMap, newMap);
    }
    copyEntry(originMap, newMap);
    return onMapCopy(newMap);
  }

  @Override
  public T onMapCopy(Map map) {
    return (T) map;
  }

  @Override
  public T onMapRead(Map map) {
    return (T) map;
  }
}
