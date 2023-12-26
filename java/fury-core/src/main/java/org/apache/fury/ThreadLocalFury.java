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

import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.util.LoaderBinding;
import org.apache.fury.util.LoaderBinding.StagingType;
import java.nio.ByteBuffer;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A thread safe serialization entrance for {@link Fury} by binding a {@link Fury} for every thread.
 * Note that the thread shouldn't be created and destroyed frequently, otherwise the {@link Fury}
 * will be created and destroyed frequently, which is slow.
 *
 * @author chaokunyang
 */
@ThreadSafe
public class ThreadLocalFury implements ThreadSafeFury {
  private final ThreadLocal<MemoryBuffer> bufferLocal =
      ThreadLocal.withInitial(() -> MemoryUtils.buffer(32));

  private final ThreadLocal<LoaderBinding> bindingThreadLocal;

  public ThreadLocalFury(Function<ClassLoader, Fury> furyFactory) {
    bindingThreadLocal =
        ThreadLocal.withInitial(
            () -> {
              LoaderBinding binding = new LoaderBinding(furyFactory);
              binding.setClassLoader(Thread.currentThread().getContextClassLoader());
              return binding;
            });
    // 1. init and warm for current thread.
    // Fury creation took about 1~2 ms, but first creation
    // in a process load some classes which is not cheap.
    // 2. Make fury generate code at graalvm build time.
    Fury fury = bindingThreadLocal.get().get();
    ClassResolver._addGraalvmClassRegistry(
        fury.getConfig().getConfigHash(), fury.getClassResolver());
  }

  public <R> R execute(Function<Fury, R> action) {
    Fury fury = bindingThreadLocal.get().get();
    return action.apply(fury);
  }

  public byte[] serialize(Object obj) {
    MemoryBuffer buffer = bufferLocal.get();
    buffer.writerIndex(0);
    bindingThreadLocal.get().get().serialize(buffer, obj);
    return buffer.getBytes(0, buffer.writerIndex());
  }

  @Override
  public MemoryBuffer serialize(Object obj, long address, int size) {
    return bindingThreadLocal.get().get().serialize(obj, address, size);
  }

  public MemoryBuffer serialize(MemoryBuffer buffer, Object obj) {
    return bindingThreadLocal.get().get().serialize(buffer, obj);
  }

  public Object deserialize(byte[] bytes) {
    return bindingThreadLocal.get().get().deserialize(bytes);
  }

  public Object deserialize(long address, int size) {
    return bindingThreadLocal.get().get().deserialize(address, size);
  }

  public Object deserialize(MemoryBuffer buffer) {
    return bindingThreadLocal.get().get().deserialize(buffer);
  }

  public Object deserialize(ByteBuffer byteBuffer) {
    return bindingThreadLocal.get().get().deserialize(MemoryUtils.wrap(byteBuffer));
  }

  @Override
  public byte[] serializeJavaObject(Object obj) {
    return bindingThreadLocal.get().get().serializeJavaObject(obj);
  }

  @Override
  public void serializeJavaObject(MemoryBuffer buffer, Object obj) {
    bindingThreadLocal.get().get().serializeJavaObject(buffer, obj);
  }

  @Override
  public <T> T deserializeJavaObject(byte[] data, Class<T> cls) {
    return bindingThreadLocal.get().get().deserializeJavaObject(data, cls);
  }

  @Override
  public <T> T deserializeJavaObject(MemoryBuffer buffer, Class<T> cls) {
    return bindingThreadLocal.get().get().deserializeJavaObject(buffer, cls);
  }

  public void setClassLoader(ClassLoader classLoader) {
    setClassLoader(classLoader, StagingType.SOFT_STAGING);
  }

  public void setClassLoader(ClassLoader classLoader, StagingType stagingType) {
    bindingThreadLocal.get().setClassLoader(classLoader, stagingType);
  }

  public ClassLoader getClassLoader() {
    return bindingThreadLocal.get().getClassLoader();
  }

  public void clearClassLoader(ClassLoader loader) {
    bindingThreadLocal.get().clearClassLoader(loader);
  }
}
