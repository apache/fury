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
 * instead of interface to reduce virtual method call cost of {@link #needToWriteRef}/{@link
 * #getXtypeId}.
 *
 * @param <T> type of objects being serializing/deserializing
 */
@NotThreadSafe
public abstract class Serializer<T> {
  protected final Fury fury;
  protected final Class<T> type;
  protected final boolean isJava;
  protected final boolean needToWriteRef;

  public void write(MemoryBuffer buffer, T value) {
    throw new UnsupportedOperationException();
  }

  public T read(MemoryBuffer buffer) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns {@link Fury#NOT_SUPPORT_CROSS_LANGUAGE} if the serializer doesn't support
   * cross-language serialization. Return a number in range (0, 32767) if the serializer support
   * cross-language serialization and native serialization data is the same with cross-language
   * serialization. Return a negative short in range [-32768, 0) if the serializer support
   * cross-language serialization and native serialization data is not the same with cross-language
   * serialization.
   */
  public short getXtypeId() {
    return Fury.NOT_SUPPORT_CROSS_LANGUAGE;
  }

  /** Returns a type tag used for setup type mapping between languages. */
  public String getCrossLanguageTypeTag() {
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
  }

  public Serializer(Fury fury, Class<T> type, boolean needToWriteRef) {
    this.fury = fury;
    this.type = type;
    this.isJava = fury.getLanguage() == Language.JAVA;
    this.needToWriteRef = needToWriteRef;
  }

  public final boolean needToWriteRef() {
    return needToWriteRef;
  }

  public Class<T> getType() {
    return type;
  }
}
