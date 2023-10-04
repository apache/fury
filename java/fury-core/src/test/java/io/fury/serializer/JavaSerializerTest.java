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
import io.fury.FuryTestBase;
import io.fury.config.Language;
import io.fury.memory.MemoryBuffer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamConstants;
import java.io.Serializable;
import java.nio.ByteBuffer;
import lombok.Data;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JavaSerializerTest extends FuryTestBase {

  @Data
  public static class CustomClass implements Serializable {
    public String name;
    public transient int age;

    private void writeObject(java.io.ObjectOutputStream s) throws IOException {
      s.defaultWriteObject();
      s.writeInt(age);
    }

    private void readObject(java.io.ObjectInputStream s) throws Exception {
      s.defaultReadObject();
      this.age = s.readInt();
    }
  }

  @Test
  public void testWriteObject() {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(false)
            .requireClassRegistration(false)
            .build();
    serDe(fury, new CustomClass());
  }

  @Test
  public void testJdkSerializationMagicNumber() throws Exception {
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(bas)) {
      objectOutputStream.writeObject(1.1);
      objectOutputStream.flush();
    }
    byte[] bytes = bas.toByteArray();
    Assert.assertEquals(MemoryBuffer.getShortB(bytes, 0), ObjectStreamConstants.STREAM_MAGIC);
    Assert.assertTrue(JavaSerializer.serializedByJDK(bytes));
    Assert.assertTrue(JavaSerializer.serializedByJDK(ByteBuffer.wrap(bytes), 0));
    Fury fury = Fury.builder().build();
    bytes = fury.serialize(1.1);
    Assert.assertFalse(JavaSerializer.serializedByJDK(bytes));
  }
}
