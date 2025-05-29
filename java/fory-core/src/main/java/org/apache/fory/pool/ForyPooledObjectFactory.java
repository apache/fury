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

package org.apache.fory.pool;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.fory.Fory;
import org.apache.fory.logging.Logger;
import org.apache.fory.logging.LoggerFactory;
import org.apache.fory.util.LoaderBinding;

/**
 * fory pool factory The pool is used to initialize instances of fory related objects for soft.
 * connections
 */
public class ForyPooledObjectFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ForyPooledObjectFactory.class);

  private final Function<ClassLoader, Fory> foryFactory;

  /**
   * ClassLoaderFuryPooled cache, have all. ClassLoaderFuryPooled caches key:
   * WeakReference:ClassLoader value: SoftReference:ClassLoaderFuryPooled
   *
   * @see Cache
   * @see com.google.common.cache.CacheBuilder
   */
  final Cache<ClassLoader, ClassLoaderFuryPooled> classLoaderFuryPooledCache;

  private volatile ClassLoader classLoader = null;

  /** ThreadLocal: ClassLoader. */
  private final ThreadLocal<ClassLoader> classLoaderLocal =
      ThreadLocal.withInitial(
          () -> {
            if (classLoader != null) {
              return classLoader;
            }
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            if (loader == null) {
              loader = Fory.class.getClassLoader();
            }
            return loader;
          });

  /**
   * Dynamic capacity expansion and contraction The user sets the minimum number of object pools.
   */
  private final int minPoolSize;

  /**
   * Dynamic capacity expansion and contraction The user sets the maximum number of object pools.
   * Math.max(maxPoolSize, CPU * 2)
   */
  private final int maxPoolSize;

  /** factoryCallback will be set in every new classLoaderFuryPooled so that can deal every fory. */
  private final Consumer<Fory> factoryCallback;

  public ForyPooledObjectFactory(
      Function<ClassLoader, Fory> foryFactory,
      int minPoolSize,
      int maxPoolSize,
      long expireTime,
      TimeUnit timeUnit,
      Consumer<Fory> factoryCallback) {
    this.minPoolSize = minPoolSize;
    this.maxPoolSize = maxPoolSize;
    this.foryFactory = foryFactory;
    this.factoryCallback = factoryCallback;
    classLoaderFuryPooledCache =
        CacheBuilder.newBuilder().expireAfterAccess(expireTime, timeUnit).build();
  }

  public ClassLoaderFuryPooled getPooledCache() {
    try {
      ClassLoader classLoader = classLoaderLocal.get();
      assert classLoader != null;
      ClassLoaderFuryPooled classLoaderFuryPooled =
          classLoaderFuryPooledCache.getIfPresent(classLoader);
      if (classLoaderFuryPooled == null) {
        // double check cache
        return getOrAddCache(classLoader);
      }
      return classLoaderFuryPooled;
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  /** todo setClassLoader support LoaderBinding.StagingType */
  public void setClassLoader(ClassLoader classLoader, LoaderBinding.StagingType stagingType) {
    if (classLoader == null) {
      // may be used to clear some classloader
      classLoader = Fory.class.getClassLoader();
    }
    this.classLoader = classLoader;
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
          new ClassLoaderFuryPooled(classLoader, foryFactory, minPoolSize, maxPoolSize);
      classLoaderFuryPooled.setFactoryCallback(factoryCallback);
      classLoaderFuryPooledCache.put(classLoader, classLoaderFuryPooled);
    }
    return classLoaderFuryPooled;
  }
}
