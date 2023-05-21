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

package io.fury.format.vectorized;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;

/**
 * Arrow utils.
 *
 * @author chaokunyang
 */
public class ArrowUtils {
  // RootAllocator is thread-safe, so we don't have to use thread-local.
  // FIXME JDK17: Unable to make field long java.nio.Buffer.address
  //   accessible: module java.base does not "opens java.nio" to unnamed module @405e4200
  public static RootAllocator allocator = new RootAllocator();

  public static ArrowBuf buffer(final long initialRequestSize) {
    return allocator.buffer(initialRequestSize);
  }
}
