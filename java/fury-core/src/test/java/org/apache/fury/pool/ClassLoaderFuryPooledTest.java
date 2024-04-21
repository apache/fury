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

package org.apache.fury.pool;

import java.util.function.Function;
import org.apache.fury.Fury;
import org.apache.fury.config.Language;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClassLoaderFuryPooledTest {

  private Function<ClassLoader, Fury> getFuryFactory() {
    return classLoader ->
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withClassLoader(classLoader)
            .requireClassRegistration(false)
            .build();
  }

  private ClassLoaderFuryPooled getPooled(int minPoolSize, int maxPoolSize) {
    return getPooled(minPoolSize, maxPoolSize, getFuryFactory());
  }

  private ClassLoaderFuryPooled getPooled(
      int minPoolSize, int maxPoolSize, Function<ClassLoader, Fury> factory) {
    return new ClassLoaderFuryPooled(
        getClass().getClassLoader(), factory, minPoolSize, maxPoolSize);
  }

  @Test
  public void testInitialIllegalPool() {
    Assert.assertThrows(
        NullPointerException.class,
        () -> {
          getPooled(1, 5, null);
        });
  }

  @Test
  public void testGetFuryNormal() {
    ClassLoaderFuryPooled pooled = getPooled(3, 5);
    Fury fury = pooled.getFury();
    Assert.assertNotNull(fury);
  }

  @Test
  public void testGetFuryWithIncreaseCapacity() {
    int minPoolSize = 4;
    ClassLoaderFuryPooled pooled = getPooled(minPoolSize, 6);
    for (int i = 0; i < minPoolSize; i++) {
      Fury fury = pooled.getFury();
      Assert.assertNotNull(fury);
    }

    try {
      pooled.setFactoryCallback(
          fury -> {
            throw new RuntimeException();
          });
      pooled.getFury();
      Assert.fail();
    } catch (RuntimeException e) {
      // Success
    }
  }

  @Test
  public void testGetFuryAwait() throws InterruptedException {
    int minPoolSize = 3;
    ClassLoaderFuryPooled pooled = getPooled(minPoolSize, 3);
    for (int i = 0; i < minPoolSize; i++) {
      Fury fury = pooled.getFury();
      Assert.assertNotNull(fury);
    }

    Thread thread = new Thread(pooled::getFury);
    thread.start();

    int timeoutMs = 3000;
    int loopCount = 10;
    int waitMs = timeoutMs / loopCount;
    while (thread.getState() != Thread.State.WAITING && loopCount > 0) {
      Thread.sleep(waitMs);
      loopCount--;
    }

    Assert.assertNotEquals(loopCount, 0);

    // loopCount != 0
    Fury fury = getFuryFactory().apply(getClass().getClassLoader());
    pooled.returnFury(fury);
    thread.join();
  }

  @Test
  public void testReturnFury() {
    Function<ClassLoader, Fury> furyFactory = getFuryFactory();
    Fury fury = furyFactory.apply(getClass().getClassLoader());
    ClassLoaderFuryPooled pooled = getPooled(4, 8, furyFactory);
    pooled.returnFury(fury);
  }

  @Test
  public void testReturnFuryForbidden() {
    ClassLoaderFuryPooled pooled = getPooled(4, 9);
    Assert.assertThrows(NullPointerException.class, () -> pooled.returnFury(null));
  }
}
