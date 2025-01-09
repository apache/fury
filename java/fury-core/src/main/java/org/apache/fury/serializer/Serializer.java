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

import javax.annotation.concurrent.NotThreadSafe;
import org.apache.fury.Fury;
import org.apache.fury.config.Language;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.type.TypeUtils;

/**
 * Serialize/deserializer objects into binary. Note that this class is designed as an abstract class
 * instead of interface to reduce virtual method call cost of {@link #needToWriteRef}.
 *
 * @param <T> type of objects being serializing/deserializing
 */
@NotThreadSafe
public abstract class Serializer<T> {
  protected final Fury fury;
  protected final Class<T> type;
  protected final boolean isJava;
  protected final boolean needToWriteRef;

  /**
   * Whether to enable circular reference of copy. Only for mutable objects, immutable objects just
   * return itself.
   */
  protected final boolean needToCopyRef;

  protected final boolean immutable;

  public void write(MemoryBuffer buffer, T value) {
    throw new UnsupportedOperationException();
  }

  public T copy(T value) {
    if (isImmutable()) {
      return value;
    }
    throw new UnsupportedOperationException(
        String.format("Copy for %s is not supported", value.getClass()));
  }

  public T read(MemoryBuffer buffer) {
    throw new UnsupportedOperationException();
  }

  public void xwrite(MemoryBuffer buffer, T value) {
    throw new UnsupportedOperationException();
  }

  public T xread(MemoryBuffer buffer) {
    throw new UnsupportedOperationException();
  }

  public Serializer(Fury fury, Class<T> type) {
    this.fury = fury;
    this.type = type;
    this.isJava = fury.getLanguage() == Language.JAVA;
    if (fury.trackingRef()) {
      needToWriteRef = !TypeUtils.isBoxed(TypeUtils.wrap(type)) || !fury.isBasicTypesRefIgnored();
    } else {
      needToWriteRef = false;
    }
    this.needToCopyRef = fury.copyTrackingRef();
    this.immutable = false;
  }

  public Serializer(Fury fury, Class<T> type, boolean immutable) {
    this.fury = fury;
    this.type = type;
    this.isJava = fury.getLanguage() == Language.JAVA;
    if (fury.trackingRef()) {
      needToWriteRef = !TypeUtils.isBoxed(TypeUtils.wrap(type)) || !fury.isBasicTypesRefIgnored();
    } else {
      needToWriteRef = false;
    }
    this.needToCopyRef = fury.copyTrackingRef() && !immutable;
    this.immutable = immutable;
  }

  public Serializer(Fury fury, Class<T> type, boolean needToWriteRef, boolean immutable) {
    this.fury = fury;
    this.type = type;
    this.isJava = fury.getLanguage() == Language.JAVA;
    this.needToWriteRef = needToWriteRef;
    this.needToCopyRef = fury.copyTrackingRef() && !immutable;
    this.immutable = immutable;
  }

  public final boolean needToWriteRef() {
    return needToWriteRef;
  }

  public final boolean needToCopyRef() {
    return needToCopyRef;
  }

  public Class<T> getType() {
    return type;
  }

  public boolean isImmutable() {
    return immutable;
  }
}
