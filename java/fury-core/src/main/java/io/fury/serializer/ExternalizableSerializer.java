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

package io.fury.serializer;

import io.fury.Fury;
import io.fury.io.FuryObjectInput;
import io.fury.io.FuryObjectOutput;
import io.fury.memory.MemoryBuffer;
import io.fury.util.Platform;
import io.fury.util.ReflectionUtils;
import java.io.Externalizable;
import java.io.IOException;
import java.lang.invoke.MethodHandle;

/**
 * Serializer for class implements {@link Externalizable}.
 *
 * @author chaokunyang
 */
public class ExternalizableSerializer<T extends Externalizable> extends Serializer<T> {
  private final MethodHandle constructor;
  private final FuryObjectInput objectInput;
  private final FuryObjectOutput objectOutput;

  public ExternalizableSerializer(Fury fury, Class<T> cls) {
    super(fury, cls);
    constructor = ReflectionUtils.getCtrHandle(cls, false);

    objectInput = new FuryObjectInput(fury, null);
    objectOutput = new FuryObjectOutput(fury, null);
  }

  @Override
  public void write(MemoryBuffer buffer, T value) {
    objectOutput.setBuffer(buffer);
    try {
      value.writeExternal(objectOutput);
    } catch (IOException e) {
      Platform.throwException(e);
    }
  }

  @Override
  public void xwrite(MemoryBuffer buffer, T value) {
    throw new UnsupportedOperationException("Externalizable can only be used in java");
  }

  @Override
  @SuppressWarnings("unchecked")
  public T read(MemoryBuffer buffer) {
    T t;
    if (constructor != null) {
      try {
        t = (T) constructor.invokeWithArguments();
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    } else {
      t = Platform.newInstance(type);
    }
    objectInput.setBuffer(buffer);
    try {
      t.readExternal(objectInput);
    } catch (IOException | ClassNotFoundException e) {
      Platform.throwException(e);
    }
    return t;
  }
}
