package org.apache.fury;

import static org.apache.fury.io.FuryStreamReader.of;
import static org.testng.Assert.assertEquals;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.fury.io.FuryInputStream;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.test.bean.BeanA;
import org.testng.annotations.Test;

public class StreamTest {
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
}
