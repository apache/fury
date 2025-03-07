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

import java.util.Objects;
import java.util.WeakHashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.fury.Fury;
import org.apache.fury.logging.Logger;
import org.apache.fury.logging.LoggerFactory;

/** A thread-safe object pool of {@link Fury}. */
public class ClassLoaderFuryPooled {

  private static final Logger LOG = LoggerFactory.getLogger(ClassLoaderFuryPooled.class);

  private final Function<ClassLoader, Fury> furyFactory;
  private Consumer<Fury> factoryCallback = f -> {};

  private final ClassLoader classLoader;

  /** idle Fury cache change. by : 1. init() 2. getFury() 3.returnFury() */
  private final BlockingQueue<Fury> idleCacheQueue;

  final WeakHashMap<Fury, Object> allFury = new WeakHashMap<>();

  /**
   * The number of active Fury objects in the cache.Make sure it does not exceed the maximum number
   * of object pools.
   */
  private final AtomicInteger activeCacheNumber = new AtomicInteger(0);

  /**
   * Dynamic capacity expansion and contraction The user sets the maximum number of object pools.
   * Math.max(maxPoolSize, CPU * 2)
   */
  private final int maxPoolSize;

  private final Lock lock = new ReentrantLock();

  public ClassLoaderFuryPooled(
      ClassLoader classLoader,
      Function<ClassLoader, Fury> furyFactory,
      int minPoolSize,
      int maxPoolSize) {
    Objects.requireNonNull(furyFactory);
    this.maxPoolSize = maxPoolSize;
    this.furyFactory = furyFactory;
    this.classLoader = classLoader;
    idleCacheQueue = new LinkedBlockingQueue<>(maxPoolSize);
    while (idleCacheQueue.size() < minPoolSize) {
      addFury(true);
    }
  }

  public Fury getFury() {
    if (activeCacheNumber.get() < maxPoolSize) {
      Fury fury = idleCacheQueue.poll();
      if (fury != null) {
        return fury;
      } else {
        // new Fury return directly, no need to add to queue, it will be added by returnFury()
        fury = addFury(false);
        if (fury != null) {
          return fury;
        }
      }
    }
    try {
      return idleCacheQueue.take();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void returnFury(Fury fury) {
    Objects.requireNonNull(fury);
    idleCacheQueue.offer(fury);
  }

  private Fury addFury(boolean addQueue) {
    // only activeCacheNumber increment success, can lock and create new Fury, otherwise return
    // null, and block in getFury(), wait for other thread to release idleCacheQueue.
    int after = activeCacheNumber.incrementAndGet();
    if (after > maxPoolSize) {
      activeCacheNumber.decrementAndGet();
      return null;
    }
    try {
      lock.lock();
      Fury fury = furyFactory.apply(classLoader);
      factoryCallback.accept(fury);
      allFury.put(fury, null);
      if (addQueue) {
        idleCacheQueue.add(fury);
      }
      return fury;
    } finally {
      lock.unlock();
    }
  }

  void setFactoryCallback(Consumer<Fury> factoryCallback) {
    try {
      lock.lock();
      this.factoryCallback = this.factoryCallback.andThen(factoryCallback);
      allFury.keySet().forEach(factoryCallback);
    } finally {
      lock.unlock();
    }
  }
}
