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

package io.fury.resolver;

import io.fury.serializer.Serializer;

/**
 * A helper class for caching {@link ClassInfo} to reduce map loop up.
 *
 * @author chaokunyang
 */
public class ClassInfoCache {
  public ClassInfo classInfo;

  public ClassInfoCache(ClassInfo classInfo) {
    this.classInfo = classInfo;
  }

  public Serializer<?> getSerializer() {
    return classInfo.serializer;
  }

  @Override
  public String toString() {
    return "ClassInfoCache{" + classInfo + '}';
  }
}
