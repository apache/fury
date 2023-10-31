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

package io.fury.serializer.scala;

import io.fury.Fury;
import io.fury.memory.MemoryBuffer;
import io.fury.serializer.Serializer;
import io.fury.util.Platform;
import java.lang.reflect.Field;

/**
 * Serializer for <a href="https://docs.scala-lang.org/tour/singleton-objects.html">scala
 * singleton</a>.
 *
 * @author chaokunyang
 */
// TODO(chaokunyang) add scala tests.
@SuppressWarnings("rawtypes")
public class SingletonObjectSerializer extends Serializer {
  private final long offset;

  public SingletonObjectSerializer(Fury fury, Class type) {
    super(fury, type);
    try {
      Field field = type.getDeclaredField("MODULE$");
      offset = Platform.UNSAFE.staticFieldOffset(field);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(type + " doesn't have `MODULE$` field", e);
    }
  }

  @Override
  public void write(MemoryBuffer buffer, Object value) {}

  @Override
  public Object read(MemoryBuffer buffer) {
    return Platform.getObject(type, offset);
  }
}
