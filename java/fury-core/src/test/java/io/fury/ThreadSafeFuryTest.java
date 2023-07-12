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

package io.fury;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.fury.resolver.MetaContext;
import io.fury.serializer.Serializer;
import io.fury.test.bean.BeanA;
import io.fury.test.bean.Struct;
import io.fury.util.LoaderBinding.StagingType;
import java.util.ArrayList;
import java.util.WeakHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ThreadSafeFuryTest extends FuryTestBase {
  private volatile boolean hasException;

  @Test
  public void testPoolSerialize() {
    BeanA beanA = BeanA.createBeanA(2);
    ThreadSafeFury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .disableSecureMode()
            .withAsyncCompilationEnabled(true)
            .buildThreadSafeFuryPool(5, 10);
    for (int i = 0; i < 2000; i++) {
      new Thread(
              () -> {
                for (int j = 0; j < 10; j++) {
                  try {
                    fury.setClassLoader(beanA.getClass().getClassLoader());
                    assertEquals(fury.deserialize(fury.serialize(beanA)), beanA);
                  } catch (Exception e) {
                    hasException = true;
                    e.printStackTrace();
                  }
                }
              })
          .start();
    }
    assertFalse(hasException);
  }

  @Test
  public void testSerialize() throws Exception {
    BeanA beanA = BeanA.createBeanA(2);
    ThreadSafeFury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .disableSecureMode()
            .withAsyncCompilationEnabled(true)
            .buildThreadSafeFury();
    ExecutorService executorService = Executors.newFixedThreadPool(12);
    for (int i = 0; i < 2000; i++) {
      executorService.execute(
          () -> {
            for (int j = 0; j < 10; j++) {
              try {
                fury.setClassLoader(beanA.getClass().getClassLoader());
                assertEquals(fury.deserialize(fury.serialize(beanA)), beanA);
              } catch (Exception e) {
                hasException = true;
                e.printStackTrace();
              }
            }
          });
    }
    executorService.shutdown();
    assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));
    assertFalse(hasException);
  }

  @Test
  public void testSerializeWithMetaShare() throws InterruptedException {
    ThreadSafeFury fury1 =
        Fury.builder().withLanguage(Language.JAVA).disableSecureMode().buildThreadSafeFury();
    ThreadSafeFury fury2 =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withMetaContextShareEnabled(true)
            .disableSecureMode()
            .buildThreadSafeFury();
    BeanA beanA = BeanA.createBeanA(2);
    ExecutorService executorService = Executors.newFixedThreadPool(12);
    ConcurrentHashMap<Thread, MetaContext> metaMap = new ConcurrentHashMap<>();
    for (int i = 0; i < 2000; i++) {
      executorService.execute(
          () -> {
            for (int j = 0; j < 10; j++) {
              try {
                {
                  fury1.setClassLoader(beanA.getClass().getClassLoader());
                  byte[] serialized = fury1.execute(f -> f.serialize(beanA));
                  Object newObj = fury1.execute(f -> f.deserialize(serialized));
                  assertEquals(newObj, beanA);
                }
                {
                  fury2.setClassLoader(beanA.getClass().getClassLoader());
                  byte[] serialized =
                      fury2.execute(
                          f -> {
                            f.getSerializationContext().setMetaContext(new MetaContext());
                            return f.serialize(beanA);
                          });
                  Object newObj =
                      fury2.execute(
                          f -> {
                            f.getSerializationContext().setMetaContext(new MetaContext());
                            return f.deserialize(serialized);
                          });
                  assertEquals(newObj, beanA);
                }
                {
                  MetaContext metaContext =
                      metaMap.computeIfAbsent(Thread.currentThread(), k -> new MetaContext());
                  fury2.setClassLoader(beanA.getClass().getClassLoader());
                  byte[] serialized =
                      fury2.execute(
                          f -> {
                            f.getSerializationContext().setMetaContext(metaContext);
                            return f.serialize(beanA);
                          });
                  Object newObj =
                      fury2.execute(
                          f -> {
                            f.getSerializationContext().setMetaContext(metaContext);
                            return f.deserialize(serialized);
                          });
                  assertEquals(newObj, beanA);
                }
              } catch (Exception e) {
                hasException = true;
                e.printStackTrace();
                throw e;
              }
            }
          });
    }
    executorService.shutdown();
    assertTrue(executorService.awaitTermination(30, TimeUnit.SECONDS));
    assertFalse(hasException);
  }

  @DataProvider(name = "stagingConfig")
  public static Object[][] stagingConfig() {
    return new Object[][] {{StagingType.NO_STAGING}, {StagingType.STRONG_STAGING}};
  }

  @Test(dataProvider = "stagingConfig")
  public void testClassDuplicateName(StagingType staging) {
    ThreadSafeFury fury = Fury.builder().requireClassRegistration(false).buildThreadSafeFury();
    String className = "DuplicateStruct";

    Class<?> structClass1 = Struct.createStructClass(className, 1);
    Object struct1 = Struct.createPOJO(structClass1);
    byte[] bytes1 = fury.serialize(struct1);
    Assert.assertEquals(fury.deserialize(bytes1), struct1);
    Class<? extends Serializer> serializerClass1 =
        fury.getCurrentFury().getClassResolver().getSerializerClass(structClass1);
    Assert.assertTrue(serializerClass1.getName().contains("Codec"));

    Class<?> structClass2 = Struct.createStructClass(className, 2);
    Object struct2 = Struct.createPOJO(structClass2);
    assertEquals(
        struct2.getClass().getDeclaredFields().length,
        struct1.getClass().getDeclaredFields().length * 2);
    AtomicReference<byte[]> bytesReference = new AtomicReference<>();
    CompletableFuture.runAsync(
            () -> {
              fury.setClassLoader(structClass2.getClassLoader(), staging);
              byte[] bytes2 = fury.serialize(struct2);
              bytesReference.set(bytes2);
            })
        .join();
    byte[] bytes2 = bytesReference.get();
    fury.setClassLoader(structClass2.getClassLoader());
    Assert.assertEquals(fury.deserialize(bytes2), struct2);
    Class<? extends Serializer> serializerClass2 =
        fury.getCurrentFury().getClassResolver().getSerializerClass(structClass2);
    Assert.assertTrue(serializerClass2.getName().contains("Codec"));
    Assert.assertNotSame(serializerClass2, serializerClass1);

    byte[] newBytes1 = fury.serialize(struct1);
    CompletableFuture.runAsync(
            () -> {
              fury.setClassLoader(structClass1.getClassLoader(), staging);
              Assert.assertEquals(fury.deserialize(newBytes1), struct1);
            })
        .join();
  }

  @Test(timeOut = 60_000)
  public void testClassGC() throws Exception {
    // Can't inline `generateClassForGC` in current method, generated classes won't be gc.
    WeakHashMap<Class<?>, Boolean> map = generateClassForGC();
    while (map.size() > 0) {
      // Force an OoM
      try {
        final ArrayList<Object[]> allocations = new ArrayList<>();
        int size;
        while ((size =
                Math.min(Math.abs((int) Runtime.getRuntime().freeMemory()), Integer.MAX_VALUE))
            > 0) allocations.add(new Object[size]);
      } catch (OutOfMemoryError e) {
        System.out.println("Trigger OOM to clear LoaderBinding.furySoftMap soft references.");
      }
      System.gc();
      Thread.sleep(1000);
      System.out.printf("Wait classes %s gc.\n", map.keySet());
    }
  }

  private WeakHashMap<Class<?>, Boolean> generateClassForGC() {
    ThreadSafeFury fury = Fury.builder().requireClassRegistration(false).buildThreadSafeFury();
    String className = "DuplicateStruct";
    WeakHashMap<Class<?>, Boolean> map = new WeakHashMap<>();
    {
      Class<?> structClass1 = Struct.createStructClass(className, 1);
      Object struct1 = Struct.createPOJO(structClass1);
      byte[] bytes = fury.serialize(struct1);
      Assert.assertEquals(fury.deserialize(bytes), struct1);
      map.put(structClass1, true);
      System.out.printf(
          "structClass1 %s %s\n",
          structClass1.hashCode(), structClass1.getClassLoader().hashCode());
    }
    {
      Class<?> structClass2 = Struct.createStructClass(className, 2);
      map.put(structClass2, true);
      System.out.printf(
          "structClass2 %s %s\n ",
          structClass2.hashCode(), structClass2.getClassLoader().hashCode());
      fury.setClassLoader(structClass2.getClassLoader());
      Object struct2 = Struct.createPOJO(structClass2);
      byte[] bytes2 = fury.serialize(struct2);
      Assert.assertEquals(fury.deserialize(bytes2), struct2);
      fury.clearClassLoader(structClass2.getClassLoader());
    }
    return map;
  }
}
