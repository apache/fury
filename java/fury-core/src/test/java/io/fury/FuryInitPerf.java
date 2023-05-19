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

import com.google.common.reflect.TypeToken;
import io.fury.resolver.MetaContext;
import io.fury.serializer.CompatibleMode;
import io.fury.test.bean.BeanA;
import io.fury.test.bean.BeanB;
import io.fury.test.bean.Foo;
import io.fury.type.TypeUtils;
import io.fury.util.LoggerFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.slf4j.Logger;
import org.testng.annotations.Test;

@Test(enabled = false)
public class FuryInitPerf {
  private static final Logger LOG = LoggerFactory.getLogger(FuryInitPerf.class);

  public void testFuryInit() {
    Fury.builder().buildThreadSafeFury();
    int num = 1000;
    List<Fury> furyList = new ArrayList<>(num);
    List<Double> durations = new ArrayList<>(num);
    LOG.info("Start create fury");
    for (int i = 0; i < num; i++) {
      long start = System.nanoTime();
      Fury fury =
          Fury.builder()
              .withLanguage(Language.JAVA)
              .withNumberCompressed(true)
              .withMetaContextShareEnabled(true)
              .withClassRegistrationRequired(false)
              .withCompatibleMode(CompatibleMode.COMPATIBLE)
              .withAsyncCompilationEnabled(true)
              .withCodegen(true)
              .build();
      double duration = (System.nanoTime() - start) / 1000_000.0;
      durations.add(duration);
      furyList.add(fury);
    }
    LOG.info("Created {} fury durations: \n{}.", num, durations);
  }

  interface Collection1 extends Collection<String> {}

  public void testGenericsInit() {
    TypeToken<?> elementType = TypeUtils.getElementType(TypeToken.of(Collection1.class));
    System.out.println(elementType);
  }

  public void testNewFurySerialization() {
    Fury.builder().buildThreadSafeFury();
    int num = 1000;
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withNumberCompressed(true)
            .withMetaContextShareEnabled(true)
            .withClassRegistrationRequired(false)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withAsyncCompilationEnabled(true)
            .withCodegen(false)
            .build();
    List<Double> durations = new ArrayList<>(num);
    LOG.info("Start create fury");
    Object o = BeanB.createBeanB(2);
    Object o2 = BeanA.createBeanA(2);
    for (int i = 0; i < 10000; i++) {
      fury.getSerializationContext().setMetaContext(new MetaContext());
      byte[] bytes = fury.serialize(Foo.create());
      fury.getSerializationContext().setMetaContext(new MetaContext());
      fury.deserialize(bytes);
    }
    for (int i = 0; i < num; i++) {
      fury =
          Fury.builder()
              .withLanguage(Language.JAVA)
              .withNumberCompressed(true)
              .withMetaContextShareEnabled(true)
              .withClassRegistrationRequired(false)
              .withCompatibleMode(CompatibleMode.COMPATIBLE)
              .withAsyncCompilationEnabled(true)
              .withCodegen(false)
              .build();
      long start = System.nanoTime();
      fury.getSerializationContext().setMetaContext(new MetaContext());
      byte[] bytes = fury.serialize(o);
      fury.getSerializationContext().setMetaContext(new MetaContext());
      fury.deserialize(bytes);
      double duration = (System.nanoTime() - start) / 1000_000.0;
      durations.add(duration);
      o = o2;
    }
    LOG.info("Serialize {} times with new fury took durations: \n{}.", num, durations);
  }
}
