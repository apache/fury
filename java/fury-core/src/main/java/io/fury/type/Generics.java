/*
 * Copyright 2023 The Fury authors
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.type;

import io.fury.Fury;
import io.fury.memory.MemoryBuffer;
import io.fury.resolver.ClassInfo;

/**
 * Handles storage of generic type information. TODO(chaokunyang) refactor to support push multiple
 * generics more efficiently. For example, avoid push and pop Map KV generics repeatedly in the
 * loop.
 */
// Inspired by `com.esotericsoftware.kryo.util.DefaultGenerics.java`.
public class Generics {
  private final Fury fury;
  private int genericTypesSize;
  private GenericType[] genericTypes = new GenericType[16];
  // Use depth and `genericTypesSize` as index to query `genericTypes`, this
  // ensures `genericTypes` are dense, which avoid sparse array to waste memory in recursive
  // circular serialization.
  private int[] depths = new int[16];

  public Generics(Fury fury) {
    this.fury = fury;
  }

  /**
   * Sets the type that is currently being serialized. Must be balanced by {@link
   * #popGenericType()}. Between those calls, the {@link GenericType generic type} are returned by
   * {@link #nextGenericType}. Fury serialization depth should be increased after this call and
   * before {@link #nextGenericType}.
   *
   * @see Fury#writeRefoJava(MemoryBuffer, Object, ClassInfo)
   */
  public void pushGenericType(GenericType fieldType) {
    int size = genericTypesSize++;
    GenericType[] genericTypes = this.genericTypes;
    if (size == genericTypes.length) {
      genericTypes = new GenericType[genericTypes.length << 1];
      System.arraycopy(this.genericTypes, 0, genericTypes, 0, size);
      this.genericTypes = genericTypes;
      int[] depthsNew = new int[depths.length << 1];
      System.arraycopy(depths, 0, depthsNew, 0, size);
      depths = depthsNew;
    }
    genericTypes[size] = fieldType;
    depths[size] = fury.getDepth();
  }

  /**
   * Removes the generic types being tracked since the corresponding {@link
   * #pushGenericType(GenericType)}. This is safe to call even if {@link
   * #pushGenericType(GenericType)} was not called. Fury serialization depth should be decreased
   * before this call and after {@link #nextGenericType}.
   *
   * @see Fury#writeRefoJava(MemoryBuffer, Object, ClassInfo)
   */
  public void popGenericType() {
    int size = genericTypesSize;
    if (size == 0) {
      return;
    }
    size--;
    if (depths[size] < fury.getDepth()) {
      return;
    }
    genericTypes[size] = null;
    genericTypesSize = size;
  }

  /**
   * Returns the current type parameters.
   *
   * @return May be null.
   */
  public GenericType nextGenericType() {
    int index = genericTypesSize;
    if (index > 0) {
      index--;
      GenericType genericType = genericTypes[index];
      // The depth must match to prevent the types being wrong if a serializer doesn't call
      // nextGenericType.
      if (depths[index] == fury.getDepth() - 1) {
        return genericType;
      }
    }
    return null;
  }
}
