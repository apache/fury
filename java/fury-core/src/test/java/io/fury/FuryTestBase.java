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

import io.fury.util.Platform;
import io.fury.util.ReflectionUtils;

/**
 * Fury unit test base class.
 *
 * @author chaokunyang
 */
@SuppressWarnings("unchecked")
public abstract class FuryTestBase {

  /** Update serialization depth by <code>diff</code>. */
  protected void increaseFuryDepth(Fury fury, int diff) {
    long offset = ReflectionUtils.getFieldOffset(Fury.class, "depth");
    int depth = Platform.getInt(fury, offset);
    Platform.putInt(fury, offset, depth + diff);
  }
}
