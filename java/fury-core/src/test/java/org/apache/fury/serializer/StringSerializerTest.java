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

import static org.apache.fury.serializer.StringSerializer.newBytesStringZeroCopy;
import static org.testng.Assert.assertEquals;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.Data;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.collection.Tuple2;
import org.apache.fury.config.Language;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.memory.Platform;
import org.apache.fury.reflect.ReflectionUtils;
import org.apache.fury.util.MathUtils;
import org.apache.fury.util.StringUtils;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class StringSerializerTest extends FuryTestBase {
  @DataProvider(name = "stringCompress")
  public static Object[][] stringCompress() {
    return new Object[][] {{false}, {true}};
  }

  @Test
  public void testJavaStringZeroCopy() {
    if (Platform.JAVA_VERSION >= 17) {
      throw new SkipException("Skip on jdk17+");
    }
    // Ensure JavaStringZeroCopy work for CI and most development environments.
    MemoryBuffer buffer = MemoryBuffer.newHeapBuffer(32);
    for (int i = 0; i < 32; i++) {
      for (int j = 0; j < 32; j++) {
        String str = StringUtils.random(j);
        if (j % 2 == 0) {
          str += "你好"; // utf16
        }
        Assert.assertTrue(writeJavaStringZeroCopy(buffer, str));
        String newStr = readJavaStringZeroCopy(buffer);
        Assert.assertEquals(str, newStr, String.format("i %s j %s", i, j));
      }
    }
  }

  @Test(dataProvider = "furyCopyConfig")
  public void testJavaStringCopy(Fury fury) {
    for (int i = 0; i < 32; i++) {
      for (int j = 0; j < 32; j++) {
        String str = StringUtils.random(j);
        if (j % 2 == 0) {
          str += "你好"; // utf16
        }
        copyCheckWithoutSame(fury, str);
      }
    }
  }

  private static String readJavaStringZeroCopy(MemoryBuffer buffer) {
    try {
      Field valueIsBytesField =
          StringSerializer.class.getDeclaredField("STRING_VALUE_FIELD_IS_BYTES");
      valueIsBytesField.setAccessible(true);
      boolean STRING_VALUE_FIELD_IS_BYTES = (boolean) valueIsBytesField.get(null);
      Field valueIsCharsField =
          StringSerializer.class.getDeclaredField("STRING_VALUE_FIELD_IS_CHARS");
      valueIsCharsField.setAccessible(true);
      boolean STRING_VALUE_FIELD_IS_CHARS = (Boolean) valueIsCharsField.get(null);
      if (STRING_VALUE_FIELD_IS_BYTES) {
        return readJDK11String(buffer);
      } else if (STRING_VALUE_FIELD_IS_CHARS) {
        return StringSerializer.newCharsStringZeroCopy(buffer.readChars(buffer.readVarUint32()));
      }
      return null;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static String readJDK11String(MemoryBuffer buffer) {
    long header = buffer.readVarUint36Small();
    byte coder = (byte) (header & 0b11);
    int numBytes = (int) (header >>> 2);
    return newBytesStringZeroCopy(coder, buffer.readBytes(numBytes));
  }

  private static boolean writeJavaStringZeroCopy(MemoryBuffer buffer, String value) {
    try {
      Field valueIsBytesField =
          StringSerializer.class.getDeclaredField("STRING_VALUE_FIELD_IS_BYTES");
      valueIsBytesField.setAccessible(true);
      boolean STRING_VALUE_FIELD_IS_BYTES = (boolean) valueIsBytesField.get(null);
      Field valueIsCharsField =
          StringSerializer.class.getDeclaredField("STRING_VALUE_FIELD_IS_CHARS");
      valueIsCharsField.setAccessible(true);
      boolean STRING_VALUE_FIELD_IS_CHARS = (Boolean) valueIsCharsField.get(null);
      if (STRING_VALUE_FIELD_IS_BYTES) {
        StringSerializer.writeBytesString(buffer, value);
      } else if (STRING_VALUE_FIELD_IS_CHARS) {
        writeJDK8String(buffer, value);
      } else {
        return false;
      }
      return true;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static void writeJDK8String(MemoryBuffer buffer, String value) {
    final char[] chars =
        (char[]) Platform.getObject(value, ReflectionUtils.getFieldOffset(String.class, "value"));
    int numBytes = MathUtils.doubleExact(value.length());
    buffer.writePrimitiveArrayWithSize(chars, Platform.CHAR_ARRAY_OFFSET, numBytes);
  }

  @Test
  public void testJavaStringSimple() {
    Fury fury = Fury.builder().withStringCompressed(true).requireClassRegistration(false).build();
    MemoryBuffer buffer = MemoryUtils.buffer(32);
    StringSerializer serializer = new StringSerializer(fury);
    {
      String str = "str";
      serializer.writeJavaString(buffer, str);
      assertEquals(str, serializer.readJavaString(buffer));
      Assert.assertEquals(buffer.writerIndex(), buffer.readerIndex());
    }
    {
      String str = "你好, Fury";
      serializer.writeJavaString(buffer, str);
      assertEquals(str, serializer.readJavaString(buffer));
      Assert.assertEquals(buffer.writerIndex(), buffer.readerIndex());
    }
  }

  @Data
  public static class Simple {
    private String str;

    public Simple(String str) {
      this.str = str;
    }
  }

  /** Test for <a href="https://github.com/apache/fury/issues/1984">#1984</a> */
  @Test
  public void testJavaCompressedString() {
    Fury fury =
        Fury.builder()
            .withStringCompressed(true)
            .withLanguage(Language.JAVA)
            .requireClassRegistration(false)
            .build();

    Simple a =
        new Simple(
            "STG@ON DEMAND Solutions@GeoComputing Switch/ Hub@Digi Edgeport/216 – 16 port Serial Hub");

    byte[] bytes = fury.serialize(a);

    Simple b = (Simple) fury.deserialize(bytes);
    assertEquals(a, b);
  }

  @Test(dataProvider = "stringCompress")
  public void testJavaString(boolean stringCompress) {
    Fury fury =
        Fury.builder().withStringCompressed(stringCompress).requireClassRegistration(false).build();
    MemoryBuffer buffer = MemoryUtils.buffer(32);
    StringSerializer serializer = new StringSerializer(fury);

    String longStr = new String(new char[50]).replace("\0", "abc");
    buffer.writerIndex(0);
    buffer.readerIndex(0);
    serializer.writeJavaString(buffer, longStr);
    assertEquals(longStr, serializer.readJavaString(buffer));

    serDe(fury, "你好, Fury" + StringUtils.random(64));
    serDe(fury, "你好, Fury" + StringUtils.random(64));
    serDe(fury, StringUtils.random(64));
    serDe(
        fury,
        new String[] {"你好, Fury" + StringUtils.random(64), "你好, Fury" + StringUtils.random(64)});
  }

  @Test(dataProvider = "stringCompress")
  public void testJavaStringOffHeap(boolean stringCompress) {
    Fury fury =
        Fury.builder().withStringCompressed(stringCompress).requireClassRegistration(false).build();
    MemoryBuffer buffer = MemoryUtils.wrap(ByteBuffer.allocateDirect(1024));
    Object o1 = "你好, Fury" + StringUtils.random(64);
    Object o2 =
        new String[] {"你好, Fury" + StringUtils.random(64), "你好, Fury" + StringUtils.random(64)};
    fury.serialize(buffer, o1);
    fury.serialize(buffer, o2);
    assertEquals(fury.deserialize(buffer), o1);
    assertEquals(fury.deserialize(buffer), o2);
  }

  @Test
  public void testJavaStringMemoryModel() {
    BlockingQueue<Tuple2<String, byte[]>> dataQueue = new ArrayBlockingQueue<>(1024);
    ConcurrentLinkedQueue<Tuple2<String, String>> results = new ConcurrentLinkedQueue<>();
    Thread producer1 = new Thread(new DataProducer(dataQueue));
    Thread producer2 = new Thread(new DataProducer(dataQueue));
    Thread consumer1 = new Thread(new DataConsumer(dataQueue, results));
    Thread consumer2 = new Thread(new DataConsumer(dataQueue, results));
    Thread consumer3 = new Thread(new DataConsumer(dataQueue, results));
    Arrays.asList(producer1, producer2, consumer1, consumer2, consumer3).forEach(Thread::start);
    int count = DataProducer.numItems * 2;
    while (count > 0) {
      Tuple2<String, String> item = results.poll();
      if (item != null) {
        count--;
        assertEquals(item.f0, item.f1);
      }
    }
    Arrays.asList(producer1, producer2, consumer1, consumer2, consumer3).forEach(Thread::interrupt);
  }

  public static class DataProducer implements Runnable {
    static int numItems = 4 + 32 * 1024 * 2;
    private final Fury fury;
    private final BlockingQueue<Tuple2<String, byte[]>> dataQueue;

    public DataProducer(BlockingQueue<Tuple2<String, byte[]>> dataQueue) {
      this.dataQueue = dataQueue;
      this.fury =
          Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
    }

    public void run() {
      try {
        dataQueue.put(Tuple2.of("", fury.serialize("")));
        dataQueue.put(Tuple2.of("a", fury.serialize("a")));
        dataQueue.put(Tuple2.of("ab", fury.serialize("ab")));
        dataQueue.put(Tuple2.of("abc", fury.serialize("abc")));
        for (int i = 0; i < 32; i++) {
          for (int j = 0; j < 1024; j++) {
            String str = StringUtils.random(j);
            dataQueue.put(Tuple2.of(str, fury.serialize(str)));
            str = String.valueOf(i);
            dataQueue.put(Tuple2.of(str, fury.serialize(str)));
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  public static class DataConsumer implements Runnable {
    private final Fury fury;
    private final BlockingQueue<Tuple2<String, byte[]>> dataQueue;
    private final ConcurrentLinkedQueue<Tuple2<String, String>> results;

    public DataConsumer(
        BlockingQueue<Tuple2<String, byte[]>> dataQueue,
        ConcurrentLinkedQueue<Tuple2<String, String>> results) {
      this.fury =
          Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(false).build();
      this.dataQueue = dataQueue;
      this.results = results;
    }

    @Override
    public void run() {
      try {
        while (!Thread.currentThread().isInterrupted()) {
          Tuple2<String, byte[]> dataItem = dataQueue.take();
          String newStr = (String) fury.deserialize(dataItem.f1);
          results.add(Tuple2.of(dataItem.f0, newStr));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Test
  public void testCompressJava8String() {
    if (Platform.JAVA_VERSION != 8) {
      throw new SkipException("Java 8 only");
    }
    Fury fury = Fury.builder().withStringCompressed(true).requireClassRegistration(false).build();
    StringSerializer stringSerializer =
        (StringSerializer) fury.getClassResolver().getSerializer(String.class);

    String utf16Str = "你好, Fury" + StringUtils.random(64);
    char[] utf16StrChars = utf16Str.toCharArray();
    for (MemoryBuffer buffer :
        new MemoryBuffer[] {
          MemoryUtils.buffer(512), MemoryUtils.wrap(ByteBuffer.allocateDirect(512)),
        }) {
      stringSerializer.writeJavaString(buffer, utf16Str);
      assertEquals(stringSerializer.readJavaString(buffer), utf16Str);
      assertEquals(buffer.writerIndex(), buffer.readerIndex());

      String latinStr = StringUtils.random(utf16StrChars.length, 0);
      stringSerializer.writeJavaString(buffer, latinStr);
      assertEquals(stringSerializer.readJavaString(buffer), latinStr);
      assertEquals(buffer.writerIndex(), buffer.readerIndex());
    }
  }

  @Test
  public void testReadUtf8String() {
    Fury fury = Fury.builder().withStringCompressed(true).requireClassRegistration(false).build();
    for (MemoryBuffer buffer :
        new MemoryBuffer[] {
          MemoryUtils.buffer(32), MemoryUtils.wrap(ByteBuffer.allocateDirect(2048))
        }) {
      StringSerializer serializer = new StringSerializer(fury);
      serializer.write(buffer, "abc你好");
      assertEquals(serializer.read(buffer), "abc你好");
      byte[] bytes = "abc你好".getBytes(StandardCharsets.UTF_8);
      byte UTF8 = 2;
      buffer.writeVarUint64(((long) "abc你好".length() << 1) << 2 | UTF8);
      buffer.writeInt32(bytes.length);
      buffer.writeBytes(bytes);
      assertEquals(serializer.read(buffer), "abc你好");
      assertEquals(buffer.readerIndex(), buffer.writerIndex());
    }
  }
}
