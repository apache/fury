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

package org.apache.fury;

import static org.apache.fury.io.FuryStreamReader.of;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.fury.io.FuryInputStream;
import org.apache.fury.io.FuryReadableChannel;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.test.bean.BeanA;
import org.apache.fury.util.ReflectionUtils;
import org.testng.annotations.Test;

public class StreamTest {
  @Test
  public void testBufferReset() {
    Fury fury = Fury.builder().withRefTracking(true).requireClassRegistration(false).build();
    byte[] bytes = fury.serialize(new byte[1000 * 1000]);
    checkBuffer(fury);
    // assertEquals(fury.deserialize(bytes), new byte[1000 * 1000]);
    assertEquals(fury.deserialize(of(new ByteArrayInputStream(bytes))), new byte[1000 * 1000]);

    bytes = fury.serializeJavaObject(new byte[1000 * 1000]);
    checkBuffer(fury);
    assertEquals(fury.deserializeJavaObject(bytes, byte[].class), new byte[1000 * 1000]);

    bytes = fury.serializeJavaObjectAndClass(new byte[1000 * 1000]);
    checkBuffer(fury);
    assertEquals(fury.deserializeJavaObjectAndClass(bytes), new byte[1000 * 1000]);

    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    fury.serialize(bas, new byte[1000 * 1000]);
    checkBuffer(fury);
    Object o = fury.deserialize(of(new ByteArrayInputStream(bas.toByteArray())));
    assertEquals(o, new byte[1000 * 1000]);
    assertEquals(fury.deserialize(bas.toByteArray()), new byte[1000 * 1000]);

    bas.reset();
    fury.serializeJavaObject(bas, new byte[1000 * 1000]);
    checkBuffer(fury);
    o = fury.deserializeJavaObject(of(new ByteArrayInputStream(bas.toByteArray())), byte[].class);
    assertEquals(o, new byte[1000 * 1000]);

    bas.reset();
    fury.serializeJavaObjectAndClass(bas, new byte[1000 * 1000]);
    checkBuffer(fury);
    o = fury.deserializeJavaObjectAndClass(of(new ByteArrayInputStream(bas.toByteArray())));
    assertEquals(o, new byte[1000 * 1000]);
  }

  private void checkBuffer(Fury fury) {
    Object buf = ReflectionUtils.getObjectFieldValue(fury, "buffer");
    MemoryBuffer buffer = (MemoryBuffer) buf;
    assert buffer != null;
    assertTrue(buffer.size() < 1000 * 1000);
  }

  @Test
  public void testOutputStream() throws IOException {
    Fury fury = Fury.builder().requireClassRegistration(false).build();
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    BeanA beanA = BeanA.createBeanA(2);
    fury.serialize(bas, beanA);
    fury.serialize(bas, beanA);
    bas.flush();
    ByteArrayInputStream bis = new ByteArrayInputStream(bas.toByteArray());
    FuryInputStream stream = of(bis);
    MemoryBuffer buf = MemoryBuffer.fromByteArray(bas.toByteArray());
    Object newObj = fury.deserialize(stream);
    assertEquals(newObj, beanA);
    newObj = fury.deserialize(buf);
    assertEquals(newObj, beanA);
    newObj = fury.deserialize(stream);
    assertEquals(newObj, beanA);
    newObj = fury.deserialize(buf);
    assertEquals(newObj, beanA);

    fury = Fury.builder().requireClassRegistration(false).build();
    // test reader buffer grow
    bis = new ByteArrayInputStream(bas.toByteArray());
    stream = of(bis);
    buf = MemoryBuffer.fromByteArray(bas.toByteArray());
    newObj = fury.deserialize(stream);
    assertEquals(newObj, beanA);
    newObj = fury.deserialize(buf);
    assertEquals(newObj, beanA);
    newObj = fury.deserialize(stream);
    assertEquals(newObj, beanA);
    newObj = fury.deserialize(buf);
    assertEquals(newObj, beanA);
  }

  @Test
  public void testBufferedStream() throws IOException {
    Fury fury = Fury.builder().requireClassRegistration(false).build();
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    BeanA beanA = BeanA.createBeanA(2);
    fury.serialize(bas, beanA);
    fury.serialize(bas, beanA);
    bas.flush();
    InputStream bis =
        new BufferedInputStream(new ByteArrayInputStream(bas.toByteArray())) {
          @Override
          public synchronized int read(byte[] b, int off, int len) throws IOException {
            return in.read(b, off, Math.min(len, 100));
          }
        };
    bis.mark(10);
    FuryInputStream stream = of(bis);
    Object newObj = fury.deserialize(stream);
    assertEquals(newObj, beanA);
    newObj = fury.deserialize(stream);
    assertEquals(newObj, beanA);

    fury = Fury.builder().requireClassRegistration(false).build();
    // test reader buffer grow
    bis = new ByteArrayInputStream(bas.toByteArray());
    stream = of(bis);
    MemoryBuffer buf = MemoryBuffer.fromByteArray(bas.toByteArray());
    newObj = fury.deserialize(stream);
    assertEquals(newObj, beanA);
    newObj = fury.deserialize(buf);
    assertEquals(newObj, beanA);

    newObj = fury.deserialize(stream);
    assertEquals(newObj, beanA);
    newObj = fury.deserialize(buf);
    assertEquals(newObj, beanA);
  }

  @Test
  public void testJavaOutputStream() throws IOException {
    Fury fury = Fury.builder().requireClassRegistration(false).build();
    BeanA beanA = BeanA.createBeanA(2);
    {
      ByteArrayOutputStream bas = new ByteArrayOutputStream();
      fury.serializeJavaObject(bas, beanA);
      fury.serializeJavaObject(bas, beanA);
      bas.flush();
      ByteArrayInputStream bis = new ByteArrayInputStream(bas.toByteArray());
      FuryInputStream stream = of(bis);
      MemoryBuffer buf = MemoryBuffer.fromByteArray(bas.toByteArray());
      Object newObj = fury.deserializeJavaObject(stream, BeanA.class);
      assertEquals(newObj, beanA);
      newObj = fury.deserializeJavaObject(buf, BeanA.class);
      assertEquals(newObj, beanA);
      newObj = fury.deserializeJavaObject(stream, BeanA.class);
      assertEquals(newObj, beanA);
      newObj = fury.deserializeJavaObject(buf, BeanA.class);
      assertEquals(newObj, beanA);
    }
    {
      ByteArrayOutputStream bas = new ByteArrayOutputStream();
      fury.serializeJavaObjectAndClass(bas, beanA);
      fury.serializeJavaObjectAndClass(bas, beanA);
      bas.flush();
      ByteArrayInputStream bis = new ByteArrayInputStream(bas.toByteArray());
      FuryInputStream stream = of(bis);
      MemoryBuffer buf = MemoryBuffer.fromByteArray(bas.toByteArray());
      Object newObj = fury.deserializeJavaObjectAndClass(stream);
      assertEquals(newObj, beanA);
      newObj = fury.deserializeJavaObjectAndClass(buf);
      assertEquals(newObj, beanA);
      newObj = fury.deserializeJavaObjectAndClass(stream);
      assertEquals(newObj, beanA);
      newObj = fury.deserializeJavaObjectAndClass(buf);
      assertEquals(newObj, beanA);

      fury = Fury.builder().requireClassRegistration(false).build();
      // test reader buffer grow
      bis = new ByteArrayInputStream(bas.toByteArray());
      stream = of(bis);
      buf = MemoryBuffer.fromByteArray(bas.toByteArray());
      newObj = fury.deserializeJavaObjectAndClass(stream);
      assertEquals(newObj, beanA);
      newObj = fury.deserializeJavaObjectAndClass(buf);
      assertEquals(newObj, beanA);
      newObj = fury.deserializeJavaObjectAndClass(stream);
      assertEquals(newObj, beanA);
      newObj = fury.deserializeJavaObjectAndClass(buf);
      assertEquals(newObj, beanA);
    }
  }

  @Test
  public void testReadableChannel() throws IOException {
    Fury fury = Fury.builder().requireClassRegistration(false).build();
    BeanA beanA = BeanA.createBeanA(2);
    {
      ByteArrayOutputStream bas = new ByteArrayOutputStream();
      fury.serialize(bas, beanA);

      Path tempFile = Files.createTempFile("readable_channel_test", "data_1");
      Files.write(tempFile, bas.toByteArray());

      try (FuryReadableChannel channel = of(Files.newByteChannel(tempFile))) {
        Object newObj = fury.deserialize(channel);
        assertEquals(newObj, beanA);
      } finally {
        Files.delete(tempFile);
      }
    }
    {
      ByteArrayOutputStream bas = new ByteArrayOutputStream();
      fury.serializeJavaObject(bas, beanA);

      Path tempFile = Files.createTempFile("readable_channel_test", "data_2");
      Files.write(tempFile, bas.toByteArray());

      try (FuryReadableChannel channel = of(Files.newByteChannel(tempFile))) {
        Object newObj = fury.deserializeJavaObject(channel, BeanA.class);
        assertEquals(newObj, beanA);
      } finally {
        Files.delete(tempFile);
      }
    }
    {
      ByteArrayOutputStream bas = new ByteArrayOutputStream();
      fury.serializeJavaObjectAndClass(bas, beanA);

      Path tempFile = Files.createTempFile("readable_channel_test", "data_3");
      Files.write(tempFile, bas.toByteArray());

      try (FuryReadableChannel channel = of(Files.newByteChannel(tempFile))) {
        Object newObj = fury.deserializeJavaObjectAndClass(channel);
        assertEquals(newObj, beanA);
      } finally {
        Files.delete(tempFile);
      }
    }
  }
}
