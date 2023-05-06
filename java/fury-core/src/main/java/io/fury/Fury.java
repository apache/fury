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

package io.fury;

import io.fury.memory.MemoryBuffer;
import io.fury.resolver.ClassInfo;
import io.fury.type.Type;
import io.fury.util.LoggerFactory;
import javax.annotation.concurrent.NotThreadSafe;
import org.slf4j.Logger;

/**
 * Cross-Lang Data layout: 1byte mask: 1-bit null: 0->null, 1->not null 1-bit endianness: 0->le,
 * 1->be 1-bit target lang: 0->native, 1->x_lang if x_lang, will write current process language as a
 * byte into buffer. 1-bit out-of-band serialization enable flag: 0 -> not enabled, 1 -> enabled.
 * other bits reserved.
 *
 * <p>serialize/deserialize is user API for root object serialization, write/read api is for inner
 * serialization.
 *
 * @author chaokunyang
 */
@NotThreadSafe
public final class Fury {
  private static final Logger LOG = LoggerFactory.getLogger(Fury.class);

  public static final byte NULL_FLAG = -3;
  // This flag indicates that object is a not-null value.
  // We don't use another byte to indicate REF, so that we can save one byte.
  public static final byte REF_FLAG = -2;
  // this flag indicates that the object is a non-null value.
  public static final byte NOT_NULL_VALUE_FLAG = -1;
  // this flag indicates that the object is a referencable and first read.
  public static final byte REF_VALUE_FLAG = 0;
  public static final byte NOT_SUPPORT_CROSS_LANGUAGE = 0;
  public static final short FURY_TYPE_TAG_ID = Type.FURY_TYPE_TAG.getId();

  private final boolean referenceTracking;
  private final Language language;
  private int depth;

  public Fury(FuryBuilder builder) {
    referenceTracking = false;
    language = null;
  }

  public void writeReferencableToJava(MemoryBuffer buffer, Object obj, ClassInfo classInfo) {
    throw new UnsupportedOperationException();
  }

  public Language getLanguage() {
    return language;
  }

  public boolean trackingReference() {
    return referenceTracking;
  }

  public boolean isBasicTypesReferenceIgnored() {
    return false;
  }

  public int getDepth() {
    return depth;
  }

  public void setDepth(int depth) {
    this.depth = depth;
  }

  public static FuryBuilder builder() {
    return new FuryBuilder();
  }

  public static final class FuryBuilder {
    public Fury build() {
      return new Fury(this);
    }

    public FuryBuilder withLanguage(Language language) {
      return this;
    }
  }
}
