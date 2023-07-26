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

import static org.testng.Assert.assertEquals;

import com.google.common.base.Preconditions;
import io.fury.Fury;
import io.fury.Language;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import lombok.Data;
import org.testng.annotations.Test;

public class ExternalizableSerializerTest {

  @Data
  public static class A implements Externalizable {
    private int x;
    private int y;
    private byte[] bytes;

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
      out.writeInt(x);
      out.writeInt(y);
      out.writeInt(bytes.length);
      out.write(bytes);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
      this.x = in.readInt();
      this.y = in.readInt();
      int len = in.readInt();
      byte[] arr = new byte[len];
      Preconditions.checkArgument(in.read(arr) == len);
      this.bytes = arr;
    }
  }

  @Test
  public void testExternalizable() {
    A a = new A();
    a.x = 1;
    a.y = 1;
    a.bytes = "bytes".getBytes();

    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(false)
            .requireClassRegistration(false)
            .build();
    assertEquals(a, fury.deserialize(fury.serialize(a)));
  }
}
