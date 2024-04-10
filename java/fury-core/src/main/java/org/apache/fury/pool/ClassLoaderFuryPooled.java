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
import java.util.Queue;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
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

  /**
   * idle Fury cache change. by : 1. getLoaderBind() 2. returnObject(LoaderBinding) 3.
   * addObjAndWarp()
   */
  private final Queue<Fury> idleCacheQueue;

  final WeakHashMap<Fury, Object> allFury = new WeakHashMap<>();

  /** active cache size's number change by : 1. getLoaderBind() 2. returnObject(LoaderBinding). */
  private final AtomicInteger activeCacheNumber = new AtomicInteger(0);

  /**
   * Dynamic capacity expansion and contraction The user sets the maximum number of object pools.
   * Math.max(maxPoolSize, CPU * 2)
   */
  private final int maxPoolSize;

  private final Lock lock = new ReentrantLock();
  private final Condition furyCondition = lock.newCondition();

  public ClassLoaderFuryPooled(
      ClassLoader classLoader,
      Function<ClassLoader, Fury> furyFactory,
      int minPoolSize,
      int maxPoolSize) {
    Objects.requireNonNull(furyFactory);
    this.maxPoolSize = maxPoolSize;
    this.furyFactory = furyFactory;
    this.classLoader = classLoader;
    idleCacheQueue = new ConcurrentLinkedQueue<>();
    while (idleCacheQueue.size() < minPoolSize) {
      addFury();
    }
  }

  public Fury getFury() {
    try {
      lock.lock();
      Fury fury = idleCacheQueue.poll();
      while (fury == null) {
        if (activeCacheNumber.get() < maxPoolSize) {
          addFury();
        } else {
          furyCondition.await();
        }
        fury = idleCacheQueue.poll();
      }
      activeCacheNumber.incrementAndGet();
      return fury;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException(e);
    } finally {
      lock.unlock();
    }
  }

  public void returnFury(Fury fury) {
    Objects.requireNonNull(fury);
    try {
      lock.lock();
      idleCacheQueue.add(fury);
      activeCacheNumber.decrementAndGet();
      furyCondition.signalAll();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
      lock.unlock();
    }
  }

  private void addFury() {
    Fury fury = furyFactory.apply(classLoader);
    factoryCallback.accept(fury);
    idleCacheQueue.add(fury);
    allFury.put(fury, null);
  }

  void setFactoryCallback(Consumer<Fury> factoryCallback) {
    this.factoryCallback = factoryCallback;
  }
}
