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

import java.io.OutputStream;
import org.apache.fury.io.FuryInputStream;
import org.apache.fury.io.FuryReadableChannel;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.serializer.BufferCallback;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.serializer.SerializerFactory;
import org.apache.fury.serializer.Serializers;

/** All Fury’s basic interface, including Fury’s basic methods. */
public interface BaseFury {

  /**
   * register class.
   *
   * @param cls class to register.
   */
  void register(Class<?> cls);

  /**
   * Register class.
   *
   * @param cls class to register.
   * @param createSerializer whether to create serializer, if true and codegen enabled, this will
   *     generate the serializer code too.
   */
  void register(Class<?> cls, boolean createSerializer);

  /** register class with given id. */
  void register(Class<?> cls, Short id);

  /**
   * Register class with specified id.
   *
   * @param cls class to register.
   * @param id id for provided class.
   * @param createSerializer whether to create serializer, if true and codegen enabled, this will
   *     generate the serializer code too.
   */
  void register(Class<?> cls, Short id, boolean createSerializer);

  /**
   * Register a Serializer.
   *
   * @param type class needed to be serialized/deserialized.
   * @param serializerClass serializer class can be created with {@link Serializers#newSerializer}.
   * @param <T> type of class.
   */
  <T> void registerSerializer(Class<T> type, Class<? extends Serializer> serializerClass);

  void registerSerializer(Class<?> type, Serializer<?> serializer);

  void setSerializerFactory(SerializerFactory serializerFactory);

  /** Return serialized <code>obj</code> as a byte array. */
  byte[] serialize(Object obj);

  /** Return serialized <code>obj</code> as a byte array. */
  byte[] serialize(Object obj, BufferCallback callback);

  /**
   * Serialize <code>obj</code> to a off-heap buffer specified by <code>address</code> and <code>
   * size</code>.
   */
  MemoryBuffer serialize(Object obj, long address, int size);

  /** Serialize data into buffer. */
  MemoryBuffer serialize(MemoryBuffer buffer, Object obj);

  /** Serialize <code>obj</code> to a <code>buffer</code>. */
  MemoryBuffer serialize(MemoryBuffer buffer, Object obj, BufferCallback callback);

  void serialize(OutputStream outputStream, Object obj);

  void serialize(OutputStream outputStream, Object obj, BufferCallback callback);

  /** Deserialize <code>obj</code> from a byte array. */
  Object deserialize(byte[] bytes);

  Object deserialize(byte[] bytes, Iterable<MemoryBuffer> outOfBandBuffers);

  /**
   * Deserialize <code>obj</code> from a off-heap buffer specified by <code>address</code> and
   * <code>size</code>.
   */
  Object deserialize(long address, int size);

  /** Deserialize <code>obj</code> from a <code>buffer</code>. */
  Object deserialize(MemoryBuffer buffer);

  Object deserialize(MemoryBuffer buffer, Iterable<MemoryBuffer> outOfBandBuffers);

  Object deserialize(FuryInputStream inputStream);

  Object deserialize(FuryInputStream inputStream, Iterable<MemoryBuffer> outOfBandBuffers);

  Object deserialize(FuryReadableChannel channel);

  Object deserialize(FuryReadableChannel channel, Iterable<MemoryBuffer> outOfBandBuffers);

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

  void serializeJavaObject(OutputStream outputStream, Object obj);

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

  <T> T deserializeJavaObject(FuryInputStream inputStream, Class<T> cls);

  <T> T deserializeJavaObject(FuryReadableChannel channel, Class<T> cls);

  /** This method is deprecated, please use {@link #serialize} instead. */
  @Deprecated
  byte[] serializeJavaObjectAndClass(Object obj);

  /** This method is deprecated, please use {@link #serialize} instead. */
  @Deprecated
  void serializeJavaObjectAndClass(MemoryBuffer buffer, Object obj);

  /** This method is deprecated, please use {@link #serialize} instead. */
  @Deprecated
  void serializeJavaObjectAndClass(OutputStream outputStream, Object obj);

  /** This method is deprecated, please use {@link #deserialize} instead. */
  @Deprecated
  Object deserializeJavaObjectAndClass(byte[] data);

  /** This method is deprecated, please use {@link #deserialize} instead. */
  @Deprecated
  Object deserializeJavaObjectAndClass(MemoryBuffer buffer);

  /** This method is deprecated, please use {@link #deserialize} instead. */
  @Deprecated
  Object deserializeJavaObjectAndClass(FuryInputStream inputStream);

  /** This method is deprecated, please use {@link #deserialize} instead. */
  @Deprecated
  Object deserializeJavaObjectAndClass(FuryReadableChannel channel);
}
