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
import org.apache.fury.Fury;
import org.apache.fury.ThreadSafeFury;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.util.LoaderBinding;
import org.apache.fury.util.Preconditions;

@ThreadSafe
public class ThreadPoolFury implements ThreadSafeFury {

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

  public void register(Class<?> clz) {
    processCallback(fury -> fury.register(clz));
  }

  public void register(Class<?> clz, int id) {
    Preconditions.checkArgument(id < Short.MAX_VALUE);
    processCallback(fury -> fury.register(clz, (short) id));
  }

  private void processCallback(Consumer<Fury> callback) {
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
    Fury fury = null;
    try {
      fury = furyPooledObjectFactory.getFury();
      return fury.serialize(obj);
    } finally {
      furyPooledObjectFactory.returnFury(fury);
    }
  }

  @Override
  public MemoryBuffer serialize(Object obj, long address, int size) {
    Fury fury = null;
    try {
      fury = furyPooledObjectFactory.getFury();
      return fury.serialize(obj, address, size);
    } finally {
      furyPooledObjectFactory.returnFury(fury);
    }
  }

  public MemoryBuffer serialize(MemoryBuffer buffer, Object obj) {
    Fury fury = null;
    try {
      fury = furyPooledObjectFactory.getFury();
      return fury.serialize(buffer, obj);
    } finally {
      furyPooledObjectFactory.returnFury(fury);
    }
  }

  public Object deserialize(byte[] bytes) {
    Fury fury = null;
    try {
      fury = furyPooledObjectFactory.getFury();
      return fury.deserialize(bytes);
    } finally {
      furyPooledObjectFactory.returnFury(fury);
    }
  }

  public Object deserialize(long address, int size) {
    Fury fury = null;
    try {
      fury = furyPooledObjectFactory.getFury();
      return fury.deserialize(address, size);
    } finally {
      furyPooledObjectFactory.returnFury(fury);
    }
  }

  public Object deserialize(MemoryBuffer buffer) {
    Fury fury = null;
    try {
      fury = furyPooledObjectFactory.getFury();
      return fury.deserialize(buffer);
    } finally {
      furyPooledObjectFactory.returnFury(fury);
    }
  }

  public Object deserialize(ByteBuffer byteBuffer) {
    Fury fury = null;
    try {
      fury = furyPooledObjectFactory.getFury();
      return fury.deserialize(MemoryUtils.wrap(byteBuffer));
    } finally {
      furyPooledObjectFactory.returnFury(fury);
    }
  }

  @Override
  public byte[] serializeJavaObject(Object obj) {
    Fury fury = null;
    try {
      fury = furyPooledObjectFactory.getFury();
      return fury.serializeJavaObject(obj);
    } finally {
      furyPooledObjectFactory.returnFury(fury);
    }
  }

  @Override
  public void serializeJavaObject(MemoryBuffer buffer, Object obj) {
    Fury fury = null;
    try {
      fury = furyPooledObjectFactory.getFury();
      fury.serializeJavaObject(buffer, obj);
    } finally {
      furyPooledObjectFactory.returnFury(fury);
    }
  }

  @Override
  public <T> T deserializeJavaObject(byte[] data, Class<T> cls) {
    Fury fury = null;
    try {
      fury = furyPooledObjectFactory.getFury();
      return fury.deserializeJavaObject(data, cls);
    } finally {
      furyPooledObjectFactory.returnFury(fury);
    }
  }

  @Override
  public <T> T deserializeJavaObject(MemoryBuffer buffer, Class<T> cls) {
    Fury fury = null;
    try {
      fury = furyPooledObjectFactory.getFury();
      return fury.deserializeJavaObject(buffer, cls);
    } finally {
      furyPooledObjectFactory.returnFury(fury);
    }
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
