/*
 * Copyright 2023 The Fury authors
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import io.fury.Fury;
import io.fury.serializer.Serializer;
import io.fury.util.LoggerFactory;
import org.slf4j.Logger;

@SuppressWarnings({"rawtypes", "unchecked", "UnstableApiUsage"})
public class ClassResolver {
  private static final Logger LOG = LoggerFactory.getLogger(ClassResolver.class);

  public static final byte USE_CLASS_VALUE = 0;
  public static final byte USE_STRING_ID = 1;
  // preserve 0 as flag for class id not set in ClassInfo`
  public static final short NO_CLASS_ID = (short) 0;
  public static final short LAMBDA_STUB_ID = 1;
  public static final short JDK_PROXY_STUB_ID = 2;
  public static final short REPLACE_STUB_ID = 3;

  /** Get or create serializer for <code>cls</code>. */
  @SuppressWarnings("unchecked")
  public <T> Serializer<T> getSerializer(Class<T> cls) {
    throw new UnsupportedOperationException();
  }

  public ClassInfo newClassInfo(Class<?> cls, Serializer<?> serializer, short classId) {
    return new ClassInfo(this, cls, null, serializer, classId);
  }

  // Invoked by fury JIT.
  public ClassInfo nilClassInfo() {
    return new ClassInfo(this, null, null, null, NO_CLASS_ID);
  }

  public ClassInfoCache nilClassInfoCache() {
    return new ClassInfoCache(nilClassInfo());
  }

  public EnumStringResolver getEnumStringResolver() {
    throw new UnsupportedOperationException();
  }

  public Fury getFury() {
    throw new UnsupportedOperationException();
  }
}
