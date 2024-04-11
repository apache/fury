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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.fury.Fury;
import org.apache.fury.logging.Logger;
import org.apache.fury.logging.LoggerFactory;
import org.apache.fury.util.LoaderBinding;

/**
 * fury pool factory The pool is used to initialize instances of fury related objects for soft.
 * connections
 */
public class FuryPooledObjectFactory {

  private static final Logger LOG = LoggerFactory.getLogger(FuryPooledObjectFactory.class);

  private final Function<ClassLoader, Fury> furyFactory;

  /**
   * ClassLoaderFuryPooled cache, have all. ClassLoaderFuryPooled caches key:
   * WeakReference:ClassLoader value: SoftReference:ClassLoaderFuryPooled
   *
   * @see Cache
   * @see com.google.common.cache.CacheBuilder
   */
  final Cache<ClassLoader, ClassLoaderFuryPooled> classLoaderFuryPooledCache;

  /** ThreadLocal: ClassLoader. */
  private final ThreadLocal<ClassLoader> classLoaderLocal =
      ThreadLocal.withInitial(() -> Thread.currentThread().getContextClassLoader());

  /**
   * Dynamic capacity expansion and contraction The user sets the minimum number of object pools.
   */
  private final int minPoolSize;

  /**
   * Dynamic capacity expansion and contraction The user sets the maximum number of object pools.
   * Math.max(maxPoolSize, CPU * 2)
   */
  private final int maxPoolSize;

  public FuryPooledObjectFactory(
      Function<ClassLoader, Fury> furyFactory,
      int minPoolSize,
      int maxPoolSize,
      long expireTime,
      TimeUnit timeUnit) {
    this.minPoolSize = minPoolSize;
    this.maxPoolSize = maxPoolSize;
    this.furyFactory = furyFactory;
    classLoaderFuryPooledCache =
        CacheBuilder.newBuilder()
            .weakKeys()
            .softValues()
            .expireAfterAccess(expireTime, timeUnit)
            .build();
  }

  public Fury getFury() {
    try {
      ClassLoader classLoader = classLoaderLocal.get();
      ClassLoaderFuryPooled classLoaderFuryPooled =
          classLoaderFuryPooledCache.getIfPresent(classLoader);
      if (classLoaderFuryPooled == null) {
        // double check cache
        ClassLoaderFuryPooled cache = getOrAddCache(classLoader);
        return cache.getFury();
      }
      return classLoaderFuryPooled.getFury();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  public void returnFury(Fury fury) {
    try {
      ClassLoader classLoader = classLoaderLocal.get();
      ClassLoaderFuryPooled classLoaderFuryPooled =
          classLoaderFuryPooledCache.getIfPresent(classLoader);
      if (classLoaderFuryPooled == null) {
        // ifPresent will be cleared when cache expire 30's
        return;
      }
      classLoaderFuryPooled.returnFury(fury);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  /** todo setClassLoader support LoaderBinding.StagingType */
  public void setClassLoader(ClassLoader classLoader, LoaderBinding.StagingType stagingType) {
    if (classLoader == null) {
      // may be used to clear some classloader
      classLoader = Fury.class.getClassLoader();
    }
    classLoaderLocal.set(classLoader);
    getOrAddCache(classLoader);
  }

  public ClassLoader getClassLoader() {
    return classLoaderLocal.get();
  }

  public void clearClassLoader(ClassLoader loader) {
    classLoaderFuryPooledCache.invalidate(loader);
    classLoaderLocal.remove();
  }

  /** Get cache or put new added pooledFury. */
  private synchronized ClassLoaderFuryPooled getOrAddCache(ClassLoader classLoader) {
    ClassLoaderFuryPooled classLoaderFuryPooled =
        classLoaderFuryPooledCache.getIfPresent(classLoader);
    if (classLoaderFuryPooled == null) {
      classLoaderFuryPooled =
          new ClassLoaderFuryPooled(classLoader, furyFactory, minPoolSize, maxPoolSize);
      classLoaderFuryPooledCache.put(classLoader, classLoaderFuryPooled);
    }
    return classLoaderFuryPooled;
  }
}
