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

package org.apache.fury.serializer;

import java.io.Externalizable;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import org.apache.fury.Fury;
import org.apache.fury.io.MemoryBufferObjectInput;
import org.apache.fury.io.MemoryBufferObjectOutput;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.util.Platform;
import org.apache.fury.util.ReflectionUtils;

/** Serializer for class implements {@link Externalizable}. */
public class ExternalizableSerializer<T extends Externalizable> extends Serializer<T> {
  private final MethodHandle constructor;
  private final MemoryBufferObjectInput objectInput;
  private final MemoryBufferObjectOutput objectOutput;

  public ExternalizableSerializer(Fury fury, Class<T> cls) {
    super(fury, cls);
    constructor = ReflectionUtils.getCtrHandle(cls, false);

    objectInput = new MemoryBufferObjectInput(fury, null);
    objectOutput = new MemoryBufferObjectOutput(fury, null);
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
        t = (T) constructor.invoke();
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
