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

import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;

/**
 * Serializers for {@link Optional}, {@link OptionalInt}, {@link OptionalLong} and {@link
 * OptionalDouble}.
 */
public final class OptionalSerializers {
  public static final class OptionalSerializer extends Serializer<Optional> {

    public OptionalSerializer(Fury fury) {
      super(fury, Optional.class);
    }

    @Override
    public void write(MemoryBuffer buffer, Optional value) {
      Object nullable = value.isPresent() ? value.get() : null;
      fury.writeRef(buffer, nullable);
    }

    @Override
    public Optional read(MemoryBuffer buffer) {
      return Optional.ofNullable(fury.readRef(buffer));
    }
  }

  public static final class OptionalIntSerializer extends Serializer<OptionalInt> {
    public OptionalIntSerializer(Fury fury) {
      super(fury, OptionalInt.class);
    }

    @Override
    public void write(MemoryBuffer buffer, OptionalInt value) {
      boolean present = value.isPresent();
      buffer.writeBoolean(present);
      if (present) {
        buffer.writeInt32(value.getAsInt());
      }
    }

    @Override
    public OptionalInt read(MemoryBuffer buffer) {
      if (buffer.readBoolean()) {
        return OptionalInt.of(buffer.readInt32());
      } else {
        return OptionalInt.empty();
      }
    }
  }

  public static final class OptionalLongSerializer extends Serializer<OptionalLong> {
    public OptionalLongSerializer(Fury fury) {
      super(fury, OptionalLong.class);
    }

    @Override
    public void write(MemoryBuffer buffer, OptionalLong value) {
      boolean present = value.isPresent();
      buffer.writeBoolean(present);
      if (present) {
        buffer.writeInt64(value.getAsLong());
      }
    }

    @Override
    public OptionalLong read(MemoryBuffer buffer) {
      if (buffer.readBoolean()) {
        return OptionalLong.of(buffer.readInt64());
      } else {
        return OptionalLong.empty();
      }
    }
  }

  public static final class OptionalDoubleSerializer extends Serializer<OptionalDouble> {
    public OptionalDoubleSerializer(Fury fury) {
      super(fury, OptionalDouble.class);
    }

    @Override
    public void write(MemoryBuffer buffer, OptionalDouble value) {
      boolean present = value.isPresent();
      buffer.writeBoolean(present);
      if (present) {
        buffer.writeFloat64(value.getAsDouble());
      }
    }

    @Override
    public OptionalDouble read(MemoryBuffer buffer) {
      if (buffer.readBoolean()) {
        return OptionalDouble.of(buffer.readFloat64());
      } else {
        return OptionalDouble.empty();
      }
    }
  }

  public static void registerDefaultSerializers(Fury fury) {
    fury.registerSerializer(Optional.class, new OptionalSerializer(fury));
    fury.registerSerializer(OptionalInt.class, new OptionalIntSerializer(fury));
    fury.registerSerializer(OptionalLong.class, new OptionalLongSerializer(fury));
    fury.registerSerializer(OptionalDouble.class, new OptionalDoubleSerializer(fury));
  }
}
