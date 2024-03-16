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
import java.lang.ref.WeakReference;

/**
 * A weak reference which will have the longer lifetime of {@link WeakReference} and {@link
 * SoftReference}.
 *
 * @param <T> type of the referent.
 */
public class DelayedRef<T> {
  // If referent is strong reachable, this won't be null.
  private final WeakReference<T> weakRef;
  // If other components doesn't strong hold the referent, this is used
  // to cache and get the referent. But the reference will be set to null
  // when there is a memory pressure.
  private final SoftReference<T> softRef;

  public DelayedRef(T o) {
    weakRef = new WeakReference<>(o);
    softRef = new SoftReference<>(o);
  }

  public T get() {
    T t = weakRef.get();
    return t != null ? t : softRef.get();
  }
}
