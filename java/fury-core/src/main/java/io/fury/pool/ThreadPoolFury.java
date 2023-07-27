/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.pool;

import io.fury.Fury;
import io.fury.ThreadSafeFury;
import io.fury.memory.MemoryBuffer;
import io.fury.memory.MemoryUtils;
import io.fury.util.LoaderBinding;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ThreadPoolFury implements ThreadSafeFury {

  private final FuryPooledObjectFactory furyPooledObjectFactory;

  public ThreadPoolFury(
      Function<ClassLoader, Fury> furyFactory,
      int minPoolSize,
      int maxPoolSize,
      long expireTime,
      TimeUnit timeUnit) {
    this.furyPooledObjectFactory =
        new FuryPooledObjectFactory(furyFactory, minPoolSize, maxPoolSize, expireTime, timeUnit);
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

  public Fury getCurrentFury() {
    Fury fury = null;
    try {
      fury = furyPooledObjectFactory.getFury();
      return fury;
    } finally {
      furyPooledObjectFactory.returnFury(fury);
    }
  }
}
