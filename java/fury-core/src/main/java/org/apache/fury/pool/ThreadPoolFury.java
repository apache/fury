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

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.fury.AbstractThreadSafeFury;
import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
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

  public <R> R execute(Function<Fury, R> action) {
    Fury fury = null;
    try {
      fury = furyPooledObjectFactory.getFury();
      return action.apply(fury);
    } finally {
      furyPooledObjectFactory.returnFury(fury);
    }
  }

  public byte[] serialize(Object obj) {
    return execute(fury -> fury.serialize(obj));
  }

  @Override
  public MemoryBuffer serialize(Object obj, long address, int size) {
    return execute(fury -> fury.serialize(obj, address, size));
  }

  public MemoryBuffer serialize(MemoryBuffer buffer, Object obj) {
    return execute(fury -> fury.serialize(buffer, obj));
  }

  public Object deserialize(byte[] bytes) {
    return execute(fury -> fury.deserialize(bytes));
  }

  public Object deserialize(long address, int size) {
    return execute(fury -> fury.deserialize(address, size));
  }

  public Object deserialize(MemoryBuffer buffer) {
    return execute(fury -> fury.deserialize(buffer));
  }

  public Object deserialize(ByteBuffer byteBuffer) {
    return execute(fury -> fury.deserialize(MemoryUtils.wrap(byteBuffer)));
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
  public <T> T deserializeJavaObject(byte[] data, Class<T> cls) {
    return execute(fury -> fury.deserializeJavaObject(data, cls));
  }

  @Override
  public <T> T deserializeJavaObject(MemoryBuffer buffer, Class<T> cls) {
    return execute(fury -> fury.deserializeJavaObject(buffer, cls));
  }

  public void setClassLoader(ClassLoader classLoader) {
    setClassLoader(classLoader, LoaderBinding.StagingType.SOFT_STAGING);
  }

  public void setClassLoader(ClassLoader classLoader, LoaderBinding.StagingType stagingType) {
    furyPooledObjectFactory.setClassLoader(classLoader, stagingType);
  }

  public ClassLoader getClassLoader() {
    return furyPooledObjectFactory.getClassLoader();
  }

  public void clearClassLoader(ClassLoader loader) {
    furyPooledObjectFactory.clearClassLoader(loader);
  }
}
