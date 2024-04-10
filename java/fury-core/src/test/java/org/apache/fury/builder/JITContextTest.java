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

package org.apache.fury.builder;

import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.List;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.Language;
import org.apache.fury.logging.Logger;
import org.apache.fury.logging.LoggerFactory;
import org.apache.fury.resolver.MetaContext;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.test.bean.BeanA;
import org.apache.fury.test.bean.BeanB;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class JITContextTest extends FuryTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(JITContextTest.class);

  @DataProvider
  public static Object[][] config1() {
    return Sets.cartesianProduct(
            ImmutableSet.of(true, false), // referenceTracking
            ImmutableSet.of(CompatibleMode.COMPATIBLE, CompatibleMode.SCHEMA_CONSISTENT))
        .stream()
        .map(List::toArray)
        .toArray(Object[][]::new);
  }

  @Test(dataProvider = "config1", timeOut = 60_000)
  public void testAsyncCompilation(boolean referenceTracking, CompatibleMode compatibleMode)
      throws InterruptedException {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTracking)
            .withCompatibleMode(compatibleMode)
            .requireClassRegistration(false)
            .withAsyncCompilation(true)
            .build();
    BeanB beanB = BeanB.createBeanB(2);
    BeanA beanA = BeanA.createBeanA(2);
    byte[] bytes1 = fury.serialize(beanB);
    byte[] bytes2 = fury.serialize(beanA);

    while (!(getSerializer(fury, BeanB.class) instanceof Generated)) {
      LOG.info("Waiting {} serializer to be jit.", BeanB.class);
      Thread.sleep(100);
    }
    while (!(getSerializer(fury, BeanA.class) instanceof Generated)) {
      LOG.info("Waiting {} serializer to be jit.", BeanA.class);
      Thread.sleep(100);
    }
    Assert.assertTrue(getSerializer(fury, BeanB.class) instanceof Generated);
    Assert.assertTrue(getSerializer(fury, BeanA.class) instanceof Generated);
    assertEquals(fury.deserialize(bytes1), beanB);
    assertEquals(fury.deserialize(bytes2), beanA);
  }

  private Serializer getSerializer(Fury fury, Class<?> cls) {
    try {
      fury.getJITContext().lock();
      Serializer<?> serializer = fury.getClassResolver().getSerializer(cls);
      return serializer;
    } finally {
      fury.getJITContext().unlock();
    }
  }

  @Test(dataProvider = "config1", timeOut = 60_000)
  public void testAsyncCompilationMetaShared(
      boolean referenceTracking, CompatibleMode compatibleMode) throws InterruptedException {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(referenceTracking)
            .withCompatibleMode(compatibleMode)
            .requireClassRegistration(false)
            .withAsyncCompilation(true)
            .build();
    BeanB beanB = BeanB.createBeanB(2);
    BeanA beanA = BeanA.createBeanA(2);
    MetaContext context = new MetaContext();
    fury.getSerializationContext().setMetaContext(context);
    byte[] bytes1 = fury.serialize(beanB);
    fury.getSerializationContext().setMetaContext(context);
    byte[] bytes2 = fury.serialize(beanA);
    while (!(getSerializer(fury, BeanB.class) instanceof Generated)) {
      LOG.info("Waiting {} serializer to be jit.", BeanB.class);
      Thread.sleep(100);
    }
    while (!(getSerializer(fury, BeanA.class) instanceof Generated)) {
      LOG.info("Waiting {} serializer to be jit.", BeanA.class);
      Thread.sleep(100);
    }
    Assert.assertTrue(getSerializer(fury, BeanB.class) instanceof Generated);
    Assert.assertTrue(getSerializer(fury, BeanA.class) instanceof Generated);
    fury.getSerializationContext().setMetaContext(context);
    assertEquals(fury.deserialize(bytes1), beanB);
    fury.getSerializationContext().setMetaContext(context);
    assertEquals(fury.deserialize(bytes2), beanA);
  }
}
