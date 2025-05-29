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

package org.apache.fory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.fory.config.CompatibleMode;
import org.apache.fory.config.Language;
import org.apache.fory.logging.Logger;
import org.apache.fory.logging.LoggerFactory;
import org.apache.fory.reflect.TypeRef;
import org.apache.fory.resolver.MetaContext;
import org.apache.fory.test.bean.BeanA;
import org.apache.fory.test.bean.BeanB;
import org.apache.fory.test.bean.Foo;
import org.apache.fory.type.TypeUtils;
import org.testng.annotations.Test;

@Test(enabled = false)
public class ForyInitPerf {
  private static final Logger LOG = LoggerFactory.getLogger(ForyInitPerf.class);

  public void testFuryInit() {
    Fory.builder().buildThreadSafeFury();
    int num = 1000;
    List<Fory> foryList = new ArrayList<>(num);
    List<Double> durations = new ArrayList<>(num);
    LOG.info("Start create fory");
    for (int i = 0; i < num; i++) {
      long start = System.nanoTime();
      Fory fory =
          Fory.builder()
              .withLanguage(Language.JAVA)
              .withNumberCompressed(true)
              .withMetaShare(true)
              .requireClassRegistration(false)
              .withCompatibleMode(CompatibleMode.COMPATIBLE)
              .withAsyncCompilation(true)
              .withCodegen(true)
              .build();
      double duration = (System.nanoTime() - start) / 1000_000.0;
      durations.add(duration);
      foryList.add(fory);
    }
    LOG.info("Created {} fory durations: \n{}.", num, durations);
  }

  interface Collection1 extends Collection<String> {}

  public void testGenericsInit() {
    TypeRef<?> elementType = TypeUtils.getElementType(TypeRef.of(Collection1.class));
    System.out.println(elementType);
  }

  public void testNewFurySerialization() {
    Fory.builder().buildThreadSafeFury();
    int num = 1000;
    Fory fory =
        Fory.builder()
            .withLanguage(Language.JAVA)
            .withNumberCompressed(true)
            .withMetaShare(true)
            .requireClassRegistration(false)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withAsyncCompilation(true)
            .withCodegen(false)
            .build();
    List<Double> durations = new ArrayList<>(num);
    LOG.info("Start create fory");
    Object o = BeanB.createBeanB(2);
    Object o2 = BeanA.createBeanA(2);
    for (int i = 0; i < 10000; i++) {
      fory.getSerializationContext().setMetaContext(new MetaContext());
      byte[] bytes = fory.serialize(Foo.create());
      fory.getSerializationContext().setMetaContext(new MetaContext());
      fory.deserialize(bytes);
    }
    for (int i = 0; i < num; i++) {
      fory =
          Fory.builder()
              .withLanguage(Language.JAVA)
              .withNumberCompressed(true)
              .withMetaShare(true)
              .requireClassRegistration(false)
              .withCompatibleMode(CompatibleMode.COMPATIBLE)
              .withAsyncCompilation(true)
              .withCodegen(false)
              .build();
      long start = System.nanoTime();
      fory.getSerializationContext().setMetaContext(new MetaContext());
      byte[] bytes = fory.serialize(o);
      fory.getSerializationContext().setMetaContext(new MetaContext());
      fory.deserialize(bytes);
      double duration = (System.nanoTime() - start) / 1000_000.0;
      durations.add(duration);
      o = o2;
    }
    LOG.info("Serialize {} times with new fory took durations: \n{}.", num, durations);
  }
}
