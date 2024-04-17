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

import com.google.common.reflect.TypeToken;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.Language;
import org.apache.fury.logging.Logger;
import org.apache.fury.logging.LoggerFactory;
import org.apache.fury.resolver.MetaContext;
import org.apache.fury.test.bean.BeanA;
import org.apache.fury.test.bean.BeanB;
import org.apache.fury.test.bean.Foo;
import org.apache.fury.type.TypeUtils;
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
              .withMetaContextShare(true)
              .requireClassRegistration(false)
              .withCompatibleMode(CompatibleMode.COMPATIBLE)
              .withAsyncCompilation(true)
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
            .withMetaContextShare(true)
            .requireClassRegistration(false)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withAsyncCompilation(true)
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
              .withMetaContextShare(true)
              .requireClassRegistration(false)
              .withCompatibleMode(CompatibleMode.COMPATIBLE)
              .withAsyncCompilation(true)
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
