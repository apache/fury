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

package org.apache.fury.util;

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.fury.Fury;
import org.apache.fury.annotation.Internal;

/**
 * An util to bind {@link Fury} with {@link ClassLoader}. If {@link ClassLoader} are changed, the
 * previous bind {@link Fury} will be canceled by default, one can set different {@link StagingType}
 * to preserve previous {@link Fury} in a strong/soft referenced way.
 */
@Internal
public final class LoaderBinding {
  private final Function<ClassLoader, Fury> furyFactory;
  // `WeakHashMap` won't work here too, since `Fury` hold classes which reference `ClassLoader`,
  // which cause
  // circular reference between ClassLoader and Fury.
  private final HashMap<ClassLoader, Fury> furyMap = new HashMap<>();
  private final WeakHashMap<ClassLoader, SoftReference<Fury>> furySoftMap = new WeakHashMap<>();
  private Consumer<Fury> bindingCallback = f -> {};
  private ClassLoader loader;
  private Fury fury;

  public LoaderBinding(Function<ClassLoader, Fury> furyFactory) {
    this.furyFactory = furyFactory;
  }

  public Fury get() {
    return fury;
  }

  public void visitAllFury(Consumer<Fury> consumer) {
    if (furySoftMap.isEmpty()) {
      for (Fury f : furyMap.values()) {
        consumer.accept(f);
      }
    } else if (furyMap.isEmpty()) {
      for (SoftReference<Fury> ref : furySoftMap.values()) {
        Fury f = ref.get();
        if (f != null) {
          consumer.accept(f);
        }
      }
    } else {
      Set<Fury> furySet = new HashSet<>(furyMap.size());
      furySet.addAll(furyMap.values());
      for (SoftReference<Fury> ref : furySoftMap.values()) {
        Fury f = ref.get();
        if (f != null) {
          furySet.add(f);
        }
      }
      for (Fury f : furySet) {
        consumer.accept(f);
      }
    }
  }

  public ClassLoader getClassLoader() {
    return loader;
  }

  /**
   * Set classloader for resolving unregistered class name to class. If <code>classLoader</code> is
   * different classloader, a new {@link Fury} instance will be created. If this is not expected,
   * {@link #setClassLoader(ClassLoader, StagingType)} and {@link #clearClassLoader} should be used.
   */
  public void setClassLoader(ClassLoader classLoader) {
    setClassLoader(classLoader, StagingType.STRONG_STAGING);
  }

  /**
   * Set classloader for resolving unregistered class name to class.
   *
   * <p>If <code>staging</code> is true, a cached {@link Fury} instance will be returned if not
   * null, and the previous classloader and associated {@link Fury} instance won't be gc unless
   * {@link #clearClassLoader} is called explicitly. If false, and the passed <code>classLoader
   * </code> is different, a new {@link Fury} instance will be created, previous classLoader and
   * associated {@link Fury} instance will be cleared.
   *
   * @param classLoader {@link ClassLoader} for resolving unregistered class name to class
   * @param stagingType Whether cache previous classloader and associated {@link Fury} instance.
   */
  public void setClassLoader(ClassLoader classLoader, StagingType stagingType) {
    if (this.loader != classLoader) {
      if (classLoader == null) {
        // may be used to clear some classloader
        classLoader = Fury.class.getClassLoader();
      }
      this.loader = classLoader;
      switch (stagingType) {
        case NO_STAGING:
          fury = furyFactory.apply(classLoader);
          bindingCallback.accept(fury);
          break;
        case SOFT_STAGING:
          {
            SoftReference<Fury> furySoftReference = furySoftMap.get(classLoader);
            Fury fury = furySoftReference == null ? null : furySoftReference.get();
            if (fury == null) {
              fury = furyFactory.apply(classLoader);
              bindingCallback.accept(fury);
              furySoftMap.put(classLoader, new SoftReference<>(fury));
            }
            this.fury = fury;
            break;
          }
        case STRONG_STAGING:
          {
            Fury fury = furyMap.get(classLoader);
            if (fury == null) {
              fury = furyFactory.apply(classLoader);
              bindingCallback.accept(fury);
              furyMap.put(classLoader, fury);
            }
            this.fury = fury;
            break;
          }
        default:
          throw new IllegalArgumentException();
      }
    }
  }

  /**
   * Clean up classloader set by {@link #setClassLoader(ClassLoader, StagingType)}, <code>
   * classLoader
   * </code> won't be referenced by {@link Fury} after this call and can be gc if it's not
   * referenced by other objects.
   */
  public void clearClassLoader(ClassLoader classLoader) {
    furyMap.remove(classLoader);
    SoftReference<Fury> softReference = furySoftMap.remove(classLoader);
    if (softReference != null) {
      softReference.clear();
    }
    if (this.loader == classLoader) {
      this.loader = null;
      this.fury = null;
    }
  }

  public void register(Class<?> clz) {
    furyMap.values().forEach(fury -> fury.register(clz));
    bindingCallback = bindingCallback.andThen(fury -> fury.register(clz));
  }

  public void register(Class<?> clz, int id) {
    Preconditions.checkArgument(id < Short.MAX_VALUE);
    furyMap.values().forEach(fury -> fury.register(clz, (short) id));
    bindingCallback = bindingCallback.andThen(fury -> fury.register(clz, (short) id));
  }

  public void setBindingCallback(Consumer<Fury> bindingCallback) {
    this.bindingCallback = bindingCallback;
  }

  public enum StagingType {
    /**
     * Don't cache fury. A new {@link Fury} will be created if classloader is switched to a new one.
     */
    NO_STAGING,
    /**
     * Cache fury to a classloader using a {@link SoftReference}, so it can be gc when there is
     * memory pressure, but doesn't cause class memory leak and doesn't cause circular reference
     * between classloader and fury.
     */
    SOFT_STAGING,
    /**
     * Cache fury to a classloader using a strong reference, {@link #clearClassLoader} should be
     * invoked to clear classloader and fury. Otherwise, classloader/fury may never be gc until
     * {@link LoaderBinding} is garbage collected.
     */
    STRONG_STAGING,
  }
}
