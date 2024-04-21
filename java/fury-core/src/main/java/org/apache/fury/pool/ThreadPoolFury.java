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

package org.apache.fury.pool;

import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.fury.AbstractThreadSafeFury;
import org.apache.fury.Fury;
import org.apache.fury.io.FuryInputStream;
import org.apache.fury.io.FuryReadableChannel;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.serializer.BufferCallback;
import org.apache.fury.util.LoaderBinding;

@ThreadSafe
public class ThreadPoolFury extends AbstractThreadSafeFury {

  private final FuryPooledObjectFactory furyPooledObjectFactory;
  private Consumer<Fury> factoryCallback = f -> {};

  public ThreadPoolFury(
      Function<ClassLoader, Fury> furyFactory,
      int minPoolSize,
      int maxPoolSize,
      long expireTime,
      TimeUnit timeUnit) {
    this.furyPooledObjectFactory =
        new FuryPooledObjectFactory(furyFactory, minPoolSize, maxPoolSize, expireTime, timeUnit);
  }

  @Override
  protected void processCallback(Consumer<Fury> callback) {
    factoryCallback = factoryCallback.andThen(callback);
    for (ClassLoaderFuryPooled furyPooled :
        furyPooledObjectFactory.classLoaderFuryPooledCache.asMap().values()) {
      furyPooled.allFury.keySet().forEach(callback);
      furyPooled.setFactoryCallback(factoryCallback);
    }
  }

  @Override
  public <R> R execute(Function<Fury, R> action) {
    Fury fury = null;
    try {
      fury = furyPooledObjectFactory.getFury();
      return action.apply(fury);
    } finally {
      furyPooledObjectFactory.returnFury(fury);
    }
  }

  @Override
  public byte[] serialize(Object obj) {
    return execute(fury -> fury.serialize(obj));
  }

  @Override
  public byte[] serialize(Object obj, BufferCallback callback) {
    return execute(fury -> fury.serialize(obj, callback));
  }

  @Override
  public MemoryBuffer serialize(Object obj, long address, int size) {
    return execute(fury -> fury.serialize(obj, address, size));
  }

  @Override
  public MemoryBuffer serialize(MemoryBuffer buffer, Object obj) {
    return execute(fury -> fury.serialize(buffer, obj));
  }

  @Override
  public MemoryBuffer serialize(MemoryBuffer buffer, Object obj, BufferCallback callback) {
    return execute(fury -> fury.serialize(buffer, obj, callback));
  }

  @Override
  public void serialize(OutputStream outputStream, Object obj) {
    execute(
        fury -> {
          fury.serialize(outputStream, obj);
          return null;
        });
  }

  @Override
  public void serialize(OutputStream outputStream, Object obj, BufferCallback callback) {
    execute(
        fury -> {
          fury.serialize(outputStream, obj, callback);
          return null;
        });
  }

  @Override
  public Object deserialize(byte[] bytes) {
    return execute(fury -> fury.deserialize(bytes));
  }

  @Override
  public Object deserialize(byte[] bytes, Iterable<MemoryBuffer> outOfBandBuffers) {
    return execute(fury -> fury.deserialize(bytes, outOfBandBuffers));
  }

  @Override
  public Object deserialize(long address, int size) {
    return execute(fury -> fury.deserialize(address, size));
  }

  @Override
  public Object deserialize(MemoryBuffer buffer) {
    return execute(fury -> fury.deserialize(buffer));
  }

  @Override
  public Object deserialize(ByteBuffer byteBuffer) {
    return execute(fury -> fury.deserialize(MemoryUtils.wrap(byteBuffer)));
  }

  @Override
  public Object deserialize(MemoryBuffer buffer, Iterable<MemoryBuffer> outOfBandBuffers) {
    return execute(fury -> fury.deserialize(buffer, outOfBandBuffers));
  }

  @Override
  public Object deserialize(FuryInputStream inputStream) {
    return execute(fury -> fury.deserialize(inputStream));
  }

  @Override
  public Object deserialize(FuryInputStream inputStream, Iterable<MemoryBuffer> outOfBandBuffers) {
    return execute(fury -> fury.deserialize(inputStream, outOfBandBuffers));
  }

  @Override
  public Object deserialize(FuryReadableChannel channel) {
    return execute(fury -> fury.deserialize(channel));
  }

  @Override
  public Object deserialize(FuryReadableChannel channel, Iterable<MemoryBuffer> outOfBandBuffers) {
    return execute(fury -> fury.deserialize(channel, outOfBandBuffers));
  }

  @Override
  public byte[] serializeJavaObject(Object obj) {
    return execute(fury -> fury.serializeJavaObject(obj));
  }

  @Override
  public void serializeJavaObject(MemoryBuffer buffer, Object obj) {
    execute(
        fury -> {
          fury.serializeJavaObject(buffer, obj);
          return null;
        });
  }

  @Override
  public void serializeJavaObject(OutputStream outputStream, Object obj) {
    execute(
        fury -> {
          fury.serializeJavaObject(outputStream, obj);
          return null;
        });
  }

  @Override
  public <T> T deserializeJavaObject(byte[] data, Class<T> cls) {
    return execute(fury -> fury.deserializeJavaObject(data, cls));
  }

  @Override
  public <T> T deserializeJavaObject(MemoryBuffer buffer, Class<T> cls) {
    return execute(fury -> fury.deserializeJavaObject(buffer, cls));
  }

  @Override
  public <T> T deserializeJavaObject(FuryInputStream inputStream, Class<T> cls) {
    return execute(fury -> fury.deserializeJavaObject(inputStream, cls));
  }

  @Override
  public <T> T deserializeJavaObject(FuryReadableChannel channel, Class<T> cls) {
    return execute(fury -> fury.deserializeJavaObject(channel, cls));
  }

  @Override
  public byte[] serializeJavaObjectAndClass(Object obj) {
    return execute(fury -> fury.serializeJavaObjectAndClass(obj));
  }

  @Override
  public void serializeJavaObjectAndClass(MemoryBuffer buffer, Object obj) {
    execute(
        fury -> {
          fury.serializeJavaObjectAndClass(buffer, obj);
          return null;
        });
  }

  @Override
  public void serializeJavaObjectAndClass(OutputStream outputStream, Object obj) {
    execute(
        fury -> {
          fury.serializeJavaObjectAndClass(outputStream, obj);
          return null;
        });
  }

  @Override
  public Object deserializeJavaObjectAndClass(byte[] data) {
    return execute(fury -> fury.deserializeJavaObjectAndClass(data));
  }

  @Override
  public Object deserializeJavaObjectAndClass(MemoryBuffer buffer) {
    return execute(fury -> fury.deserializeJavaObjectAndClass(buffer));
  }

  @Override
  public Object deserializeJavaObjectAndClass(FuryInputStream inputStream) {
    return execute(fury -> fury.deserializeJavaObjectAndClass(inputStream));
  }

  @Override
  public Object deserializeJavaObjectAndClass(FuryReadableChannel channel) {
    return execute(fury -> fury.deserializeJavaObjectAndClass(channel));
  }

  @Override
  public void setClassLoader(ClassLoader classLoader) {
    setClassLoader(classLoader, LoaderBinding.StagingType.SOFT_STAGING);
  }

  @Override
  public void setClassLoader(ClassLoader classLoader, LoaderBinding.StagingType stagingType) {
    furyPooledObjectFactory.setClassLoader(classLoader, stagingType);
  }

  @Override
  public ClassLoader getClassLoader() {
    return furyPooledObjectFactory.getClassLoader();
  }

  @Override
  public void clearClassLoader(ClassLoader loader) {
    furyPooledObjectFactory.clearClassLoader(loader);
  }
}
