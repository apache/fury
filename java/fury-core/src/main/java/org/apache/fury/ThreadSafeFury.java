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

package org.apache.fury;

import java.nio.ByteBuffer;
import java.util.function.Function;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.util.LoaderBinding;

/**
 * Thread safe serializer interface. {@link Fury} is not thread-safe, the implementation of this
 * interface will be thread-safe. And support switch classloader dynamically.
 *
 * @author chaokunyang
 */
public interface ThreadSafeFury {

  /**
   * Provide a context to execution operations on {@link Fury} directly and return the executed
   * result.
   */
  <R> R execute(Function<Fury, R> action);

  /** Return serialized <code>obj</code> as a byte array. */
  byte[] serialize(Object obj);

  /**
   * Serialize <code>obj</code> to a off-heap buffer specified by <code>address</code> and <code>
   * size</code>.
   */
  MemoryBuffer serialize(Object obj, long address, int size);

  /** Serialize data into buffer. */
  MemoryBuffer serialize(MemoryBuffer buffer, Object obj);

  /** Deserialize <code>obj</code> from a byte array. */
  Object deserialize(byte[] bytes);

  /**
   * Deserialize <code>obj</code> from a off-heap buffer specified by <code>address</code> and
   * <code>size</code>.
   */
  Object deserialize(long address, int size);

  /** Deserialize <code>obj</code> from a <code>buffer</code>. */
  Object deserialize(MemoryBuffer buffer);

  /** Deserialize <code>obj</code> from a {@link ByteBuffer}. */
  Object deserialize(ByteBuffer byteBuffer);

  /**
   * Serialize java object without class info, deserialization should use {@link
   * #deserializeJavaObject}.
   */
  byte[] serializeJavaObject(Object obj);

  /**
   * Serialize java object without class info, deserialization should use {@link
   * #deserializeJavaObject}.
   */
  void serializeJavaObject(MemoryBuffer buffer, Object obj);

  /**
   * Deserialize java object from binary without class info, serialization should use {@link
   * #serializeJavaObject}.
   */
  <T> T deserializeJavaObject(byte[] data, Class<T> cls);

  /**
   * Deserialize java object from binary by passing class info, serialization should use {@link
   * #serializeJavaObject}.
   */
  <T> T deserializeJavaObject(MemoryBuffer buffer, Class<T> cls);

  /**
   * Set classLoader of serializer for current thread only.
   *
   * @see LoaderBinding#setClassLoader(ClassLoader)
   */
  void setClassLoader(ClassLoader classLoader);

  /**
   * Set classLoader of serializer for current thread only.
   *
   * <p>If <code>staging</code> is true, a cached {@link Fury} instance will be returned if not
   * null, and the previous classloader and associated {@link Fury} instance won't be gc unless
   * {@link #clearClassLoader} is called explicitly. If false, and the passed <code>classLoader
   * </code> is different, a new {@link Fury} instance will be created, previous classLoader and
   * associated {@link Fury} instance will be cleared.
   *
   * @param classLoader {@link ClassLoader} for resolving unregistered class name to class
   * @param stagingType Whether cache previous classloader and associated {@link Fury} instance.
   * @see LoaderBinding#setClassLoader(ClassLoader, LoaderBinding.StagingType)
   */
  void setClassLoader(ClassLoader classLoader, LoaderBinding.StagingType stagingType);

  /** Returns classLoader of serializer for current thread. */
  ClassLoader getClassLoader();

  /**
   * Clean up classloader set by {@link #setClassLoader(ClassLoader, LoaderBinding.StagingType)},
   * <code>
   * classLoader
   * </code> won't be referenced by {@link Fury} after this call and can be gc if it's not
   * referenced by other objects.
   *
   * @see LoaderBinding#clearClassLoader(ClassLoader)
   */
  void clearClassLoader(ClassLoader loader);
}
