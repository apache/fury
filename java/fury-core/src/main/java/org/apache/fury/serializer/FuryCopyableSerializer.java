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

package org.apache.fury.serializer;

import org.apache.fury.Fury;
import org.apache.fury.FuryCopyable;
import org.apache.fury.memory.MemoryBuffer;

/** Fury custom copy serializer. see {@link FuryCopyable} */
public class FuryCopyableSerializer<T> extends Serializer<T> {

  private final Serializer<T> serializer;

  public FuryCopyableSerializer(Fury fury, Class<T> type, Serializer<T> serializer) {
    super(fury, type);
    this.serializer = serializer;
  }

  @Override
  public void write(MemoryBuffer buffer, T value) {
    serializer.write(buffer, value);
  }

  @Override
  public T copy(T obj) {
    return ((FuryCopyable<T>) obj).copy(fury);
  }

  @Override
  public T read(MemoryBuffer buffer) {
    return serializer.read(buffer);
  }

  /**
   * Returns {@link Fury#NOT_SUPPORT_CROSS_LANGUAGE} if the serializer doesn't support
   * cross-language serialization. Return a number in range (0, 32767) if the serializer support
   * cross-language serialization and native serialization data is the same with cross-language
   * serialization. Return a negative short in range [-32768, 0) if the serializer support
   * cross-language serialization and native serialization data is not the same with cross-language
   * serialization.
   */
  @Override
  public short getXtypeId() {
    return serializer.getXtypeId();
  }

  /** Returns a type tag used for setup type mapping between languages. */
  @Override
  public String getCrossLanguageTypeTag() {
    return serializer.getCrossLanguageTypeTag();
  }

  @Override
  public void xwrite(MemoryBuffer buffer, T value) {
    serializer.xwrite(buffer, value);
  }

  @Override
  public T xread(MemoryBuffer buffer) {
    return serializer.xread(buffer);
  }
}
