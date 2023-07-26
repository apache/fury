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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.fury.Fury;
import io.fury.Language;
import io.fury.memory.MemoryBuffer;
import lombok.Data;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SerializerFactoryTest {

  @Data
  public static class A implements KryoSerializable {
    private String f1;

    @Override
    public void write(Kryo kryo, Output output) {
      output.writeString(f1);
    }

    @Override
    public void read(Kryo kryo, Input input) {
      f1 = input.readString();
    }
  }

  private static class KryoSerializer extends Serializer {
    private Kryo kryo;
    private Output output;
    private ByteBufferInput input;

    public KryoSerializer(Fury fury, Class cls) {
      super(fury, cls);
      kryo = new Kryo();
      output = new Output(64, Integer.MAX_VALUE);
      input = new ByteBufferInput();
    }

    @Override
    public void write(MemoryBuffer buffer, Object value) {
      output.clear();
      kryo.writeObject(output, value);
      buffer.writeBytes(output.getBuffer(), 0, output.position());
    }

    @Override
    public Object read(MemoryBuffer buffer) {
      input.setBuffer(buffer.sliceAsByteBuffer());
      Object o = kryo.readObject(input, type);
      buffer.readerIndex(buffer.readerIndex() + input.position());
      return o;
    }
  }

  @Test
  public void testSerializerFactory() {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(false)
            .requireClassRegistration(false)
            .build();
    fury.setSerializerFactory(
        (f, cls) -> {
          if (KryoSerializable.class.isAssignableFrom(cls)) {
            return new KryoSerializer(fury, cls);
          } else {
            return null;
          }
        });
    A a = new A();
    a.f1 = "f1";

    Object a2 = fury.deserialize(fury.serialize(a));
    Assert.assertEquals(a, a2);
  }
}
