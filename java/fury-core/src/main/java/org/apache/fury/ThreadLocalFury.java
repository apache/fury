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
import java.nio.ByteBuffer;
import java.util.WeakHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.fury.io.FuryInputStream;
import org.apache.fury.io.FuryReadableChannel;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.serializer.BufferCallback;
import org.apache.fury.util.LoaderBinding;
import org.apache.fury.util.LoaderBinding.StagingType;

/**
 * A thread safe serialization entrance for {@link Fury} by binding a {@link Fury} for every thread.
 * Note that the thread shouldn't be created and destroyed frequently, otherwise the {@link Fury}
 * will be created and destroyed frequently, which is slow.
 */
@ThreadSafe
public class ThreadLocalFury extends AbstractThreadSafeFury {
  private final ThreadLocal<MemoryBuffer> bufferLocal =
      ThreadLocal.withInitial(() -> MemoryUtils.buffer(32));

  private final ThreadLocal<LoaderBinding> bindingThreadLocal;
  private Consumer<Fury> factoryCallback;
  private final WeakHashMap<LoaderBinding, Object> allFury;

  public ThreadLocalFury(Function<ClassLoader, Fury> furyFactory) {
    factoryCallback = f -> {};
    allFury = new WeakHashMap<>();
    bindingThreadLocal =
        ThreadLocal.withInitial(
            () -> {
              LoaderBinding binding = new LoaderBinding(furyFactory);
              binding.setBindingCallback(factoryCallback);
              binding.setClassLoader(Thread.currentThread().getContextClassLoader());
              allFury.put(binding, null);
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

  @Override
  protected void processCallback(Consumer<Fury> callback) {
    factoryCallback = factoryCallback.andThen(callback);
    for (LoaderBinding binding : allFury.keySet()) {
      binding.visitAllFury(callback);
      binding.setBindingCallback(factoryCallback);
    }
  }

  @Override
  public <R> R execute(Function<Fury, R> action) {
    Fury fury = bindingThreadLocal.get().get();
    return action.apply(fury);
  }

  @Override
  public byte[] serialize(Object obj) {
    MemoryBuffer buffer = bufferLocal.get();
    buffer.writerIndex(0);
    bindingThreadLocal.get().get().serialize(buffer, obj);
    return buffer.getBytes(0, buffer.writerIndex());
  }

  @Override
  public byte[] serialize(Object obj, BufferCallback callback) {
    MemoryBuffer buffer = bufferLocal.get();
    buffer.writerIndex(0);
    bindingThreadLocal.get().get().serialize(buffer, obj, callback);
    return buffer.getBytes(0, buffer.writerIndex());
  }

  @Override
  public MemoryBuffer serialize(Object obj, long address, int size) {
    return bindingThreadLocal.get().get().serialize(obj, address, size);
  }

  @Override
  public MemoryBuffer serialize(MemoryBuffer buffer, Object obj) {
    return bindingThreadLocal.get().get().serialize(buffer, obj);
  }

  @Override
  public MemoryBuffer serialize(MemoryBuffer buffer, Object obj, BufferCallback callback) {
    return bindingThreadLocal.get().get().serialize(buffer, obj, callback);
  }

  @Override
  public void serialize(OutputStream outputStream, Object obj) {
    bindingThreadLocal.get().get().serialize(outputStream, obj);
  }

  @Override
  public void serialize(OutputStream outputStream, Object obj, BufferCallback callback) {
    bindingThreadLocal.get().get().serialize(outputStream, obj, callback);
  }

  @Override
  public Object deserialize(byte[] bytes) {
    return bindingThreadLocal.get().get().deserialize(bytes);
  }

  @Override
  public Object deserialize(byte[] bytes, Iterable<MemoryBuffer> outOfBandBuffers) {
    return bindingThreadLocal.get().get().deserialize(bytes, outOfBandBuffers);
  }

  @Override
  public Object deserialize(long address, int size) {
    return bindingThreadLocal.get().get().deserialize(address, size);
  }

  @Override
  public Object deserialize(MemoryBuffer buffer) {
    return bindingThreadLocal.get().get().deserialize(buffer);
  }

  @Override
  public Object deserialize(ByteBuffer byteBuffer) {
    return bindingThreadLocal.get().get().deserialize(MemoryUtils.wrap(byteBuffer));
  }

  @Override
  public Object deserialize(MemoryBuffer buffer, Iterable<MemoryBuffer> outOfBandBuffers) {
    return bindingThreadLocal.get().get().deserialize(buffer, outOfBandBuffers);
  }

  @Override
  public Object deserialize(FuryInputStream inputStream) {
    return bindingThreadLocal.get().get().deserialize(inputStream);
  }

  @Override
  public Object deserialize(FuryInputStream inputStream, Iterable<MemoryBuffer> outOfBandBuffers) {
    return bindingThreadLocal.get().get().deserialize(inputStream, outOfBandBuffers);
  }

  @Override
  public Object deserialize(FuryReadableChannel channel) {
    return bindingThreadLocal.get().get().deserialize(channel);
  }

  @Override
  public Object deserialize(FuryReadableChannel channel, Iterable<MemoryBuffer> outOfBandBuffers) {
    return bindingThreadLocal.get().get().deserialize(channel, outOfBandBuffers);
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
  public void serializeJavaObject(OutputStream outputStream, Object obj) {
    bindingThreadLocal.get().get().serializeJavaObject(outputStream, obj);
  }

  @Override
  public <T> T deserializeJavaObject(byte[] data, Class<T> cls) {
    return bindingThreadLocal.get().get().deserializeJavaObject(data, cls);
  }

  @Override
  public <T> T deserializeJavaObject(MemoryBuffer buffer, Class<T> cls) {
    return bindingThreadLocal.get().get().deserializeJavaObject(buffer, cls);
  }

  @Override
  public <T> T deserializeJavaObject(FuryInputStream inputStream, Class<T> cls) {
    return bindingThreadLocal.get().get().deserializeJavaObject(inputStream, cls);
  }

  @Override
  public <T> T deserializeJavaObject(FuryReadableChannel channel, Class<T> cls) {
    return bindingThreadLocal.get().get().deserializeJavaObject(channel, cls);
  }

  @Override
  public byte[] serializeJavaObjectAndClass(Object obj) {
    return bindingThreadLocal.get().get().serializeJavaObjectAndClass(obj);
  }

  @Override
  public void serializeJavaObjectAndClass(MemoryBuffer buffer, Object obj) {
    bindingThreadLocal.get().get().serializeJavaObjectAndClass(buffer, obj);
  }

  @Override
  public void serializeJavaObjectAndClass(OutputStream outputStream, Object obj) {
    bindingThreadLocal.get().get().serializeJavaObjectAndClass(outputStream, obj);
  }

  @Override
  public Object deserializeJavaObjectAndClass(byte[] data) {
    return bindingThreadLocal.get().get().deserializeJavaObjectAndClass(data);
  }

  @Override
  public Object deserializeJavaObjectAndClass(MemoryBuffer buffer) {
    return bindingThreadLocal.get().get().deserializeJavaObjectAndClass(buffer);
  }

  @Override
  public Object deserializeJavaObjectAndClass(FuryInputStream inputStream) {
    return bindingThreadLocal.get().get().deserializeJavaObjectAndClass(inputStream);
  }

  @Override
  public Object deserializeJavaObjectAndClass(FuryReadableChannel channel) {
    return bindingThreadLocal.get().get().deserializeJavaObjectAndClass(channel);
  }

  @Override
  public void setClassLoader(ClassLoader classLoader) {
    setClassLoader(classLoader, StagingType.SOFT_STAGING);
  }

  @Override
  public void setClassLoader(ClassLoader classLoader, StagingType stagingType) {
    bindingThreadLocal.get().setClassLoader(classLoader, stagingType);
  }

  @Override
  public ClassLoader getClassLoader() {
    return bindingThreadLocal.get().getClassLoader();
  }

  @Override
  public void clearClassLoader(ClassLoader loader) {
    bindingThreadLocal.get().clearClassLoader(loader);
  }
}
