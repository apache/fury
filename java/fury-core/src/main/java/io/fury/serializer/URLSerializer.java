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

package io.fury.serializer;

import io.fury.Fury;
import io.fury.memory.MemoryBuffer;
import io.fury.util.Platform;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Serializer for {@link URL}.
 *
 * @author chaokunyang
 */
// TODO(chaokunyang) ensure security to avoid dnslog detection.
public final class URLSerializer extends Serializer<URL> {

  public URLSerializer(Fury fury, Class<URL> type) {
    super(fury, type);
  }

  public void write(MemoryBuffer buffer, URL object) {
    fury.writeString(buffer, object.toExternalForm());
  }

  public URL read(MemoryBuffer buffer) {
    try {
      return new URL(fury.readString(buffer));
    } catch (MalformedURLException e) {
      Platform.throwException(e);
      throw new IllegalStateException("unreachable");
    }
  }
}
