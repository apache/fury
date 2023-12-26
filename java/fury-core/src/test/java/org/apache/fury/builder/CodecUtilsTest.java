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

import org.apache.fury.Fury;
import org.apache.fury.config.Language;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.test.bean.BeanA;
import org.testng.annotations.Test;

public class CodecUtilsTest {

  @SuppressWarnings("unchecked")
  @Test
  public void loadOrGenObjectCodecClass() throws Exception {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(false)
            .requireClassRegistration(false)
            .build();
    Class<?> seqCodecClass = fury.getClassResolver().getSerializerClass(BeanA.class);
    Generated.GeneratedSerializer serializer =
        seqCodecClass
            .asSubclass(Generated.GeneratedSerializer.class)
            .getConstructor(Fury.class, Class.class)
            .newInstance(fury, BeanA.class);
    MemoryBuffer buffer = MemoryUtils.buffer(32);
    BeanA beanA = BeanA.createBeanA(2);
    serializer.write(buffer, beanA);
    byte[] bytes = buffer.getBytes(0, buffer.writerIndex());
    Object obj = serializer.read(MemoryUtils.wrap(bytes));
    assertEquals(obj, beanA);
  }
}
