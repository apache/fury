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

package org.apache.fury.benchmark;

import static org.testng.Assert.assertEquals;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import org.apache.fury.Fury;
import org.apache.fury.config.Language;
import org.apache.fury.format.encoder.Encoders;
import org.apache.fury.format.encoder.RowEncoder;
import org.apache.fury.format.row.binary.BinaryRow;
import org.apache.fury.logging.Logger;
import org.apache.fury.logging.LoggerFactory;
import org.apache.fury.test.bean.Foo;
import org.nustaq.serialization.FSTConfiguration;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class DeserializationBenchmark {

  private static final Logger LOG = LoggerFactory.getLogger(DeserializationBenchmark.class);
  private long iterNums;

  @BeforeTest
  public void setArrSize() {
    int defaultIterNums = 20000000;
    iterNums = Integer.parseInt(System.getProperty("iterNums", String.valueOf(defaultIterNums)));
    LOG.info("iterNums: " + iterNums);
  }

  // mvn test -Dtest=org.apache.fury.benchmark.DeserializationBenchmark#deserializationBenchmark
  // -DiterNums=10000000
  @Test(enabled = false)
  public void deserializationBenchmark() throws Exception {
    @SuppressWarnings("unchecked")
    Object data = Foo.create();
    testFury(data);
    testFst(data);
    testKryo(data);
  }

  private void testEncoder() {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(false)
            .requireClassRegistration(false)
            .build();
    RowEncoder<Foo> encoder = Encoders.bean(Foo.class, fury, 64);
    Foo foo = Foo.create();

    // test encoder
    BinaryRow row = encoder.toRow(foo);
    // warm
    for (int i = 0; i < iterNums; i++) {
      encoder.fromRow(row);
    }
    // test
    long startTime = System.nanoTime();
    for (int i = 0; i < iterNums; i++) {
      encoder.fromRow(row);
    }
    long duration = System.nanoTime() - startTime;
    LOG.info(
        "encoder decode\t take "
            + duration
            + " ns, "
            + duration / 1000_000
            + "ms. "
            + (double) duration / iterNums
            + "/ns, "
            + (double) duration / iterNums / 1000_000
            + "/ms\n");
  }

  private void testFury(Object obj) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(false)
            .requireClassRegistration(false)
            .build();
    fury.register(obj.getClass());
    // test fury
    fury.register(Foo.class);
    // warm
    byte[] serializedBytes = fury.serialize(obj);
    for (int i = 0; i < iterNums; i++) {
      fury.deserialize(serializedBytes);
    }
    // test
    long startTime = System.nanoTime();
    for (int i = 0; i < iterNums; i++) {
      fury.deserialize(serializedBytes);
    }
    long duration = System.nanoTime() - startTime;
    LOG.info(
        "fury decode\t take "
            + duration
            + " ns, "
            + duration / 1000_000
            + "ms. "
            + (double) duration / iterNums
            + "/ns, "
            + (double) duration / iterNums / 1000_000
            + "/ms\n");
  }

  private void testFst(Object obj) {
    FSTConfiguration fstConf = FSTConfiguration.createDefaultConfiguration();
    fstConf.setPreferSpeed(true);
    fstConf.setShareReferences(false);
    fstConf.registerClass(obj.getClass());
    byte[] fstBytes = fstConf.asByteArray(obj);
    assertEquals(fstConf.asObject(fstBytes), obj);
    Object newObj = null;
    // warm
    for (int i = 0; i < iterNums; i++) {
      newObj = fstConf.asObject(fstBytes);
    }
    // test
    long startTime = System.nanoTime();
    for (int i = 0; i < iterNums; i++) {
      newObj = fstConf.asObject(fstBytes);
    }
    System.out.println(newObj);
    long duration = System.nanoTime() - startTime;
    LOG.info(
        "fst\t take "
            + duration
            + " ns, "
            + duration / 1000_000
            + "ms. "
            + (double) duration / iterNums
            + "/ns, "
            + (double) duration / iterNums / 1000_000
            + "/ms\n");
  }

  private void testKryo(Object obj) {
    Kryo kryo = new Kryo();
    kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
    kryo.register(obj.getClass());
    // warm
    Output output = new Output(32, Integer.MAX_VALUE);
    kryo.writeObject(output, obj);
    Object newObj = null;
    Input input = new Input(output.getBuffer());
    for (int i = 0; i < iterNums; i++) {
      newObj = kryo.readObject(input, Foo.class);
      input.setBuffer(output.getBuffer());
    }
    // test
    long startTime = System.nanoTime();
    for (int i = 0; i < iterNums; i++) {
      newObj = kryo.readObject(input, Foo.class);
      input.setBuffer(output.getBuffer());
    }
    System.out.println(newObj);
    long duration = System.nanoTime() - startTime;
    LOG.info(
        "kryo decode\t take "
            + duration
            + " ns, "
            + duration / 1000_000
            + "ms. "
            + (double) duration / iterNums
            + "/ns, "
            + (double) duration / iterNums / 1000_000
            + "/ms\n");
  }

  private void testJDK(Object data) throws Exception {
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(bas)) {
      objectOutputStream.writeObject(data);
      objectOutputStream.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    Object newObj = null;
    byte[] bytes = bas.toByteArray();
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    // warm
    for (int i = 0; i < iterNums; i++) {
      bis.reset();
      try (ObjectInputStream objectInputStream = new ObjectInputStream(bis)) {
        newObj = objectInputStream.readObject();
      }
    }

    // test
    long startTime = System.nanoTime();
    for (int i = 0; i < iterNums; i++) {
      bis.reset();
      try (ObjectInputStream objectInputStream = new ObjectInputStream((bis))) {
        newObj = objectInputStream.readObject();
      }
    }
    System.out.println(System.identityHashCode(((ArrayList) newObj).get(1)));
    long duration = System.nanoTime() - startTime;
    LOG.info(
        "jdk\t {} take "
            + duration
            + " ns, "
            + duration / 1000_000
            + "ms. "
            + (double) duration / iterNums
            + "/ns, "
            + (double) duration / iterNums / 1000_000
            + "/ms\n",
        bas.size());
  }
}
