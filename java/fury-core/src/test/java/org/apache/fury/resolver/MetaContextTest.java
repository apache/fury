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

package org.apache.fury.resolver;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.fury.Fury;
import org.apache.fury.FuryTestBase;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.Language;
import org.apache.fury.test.bean.BeanA;
import org.apache.fury.test.bean.BeanB;
import org.apache.fury.test.bean.Foo;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MetaContextTest extends FuryTestBase {
  @Test
  public void testShareClassName() {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .withMetaShare(true)
            .requireClassRegistration(false)
            .build();
    for (Object o : new Object[] {Foo.create(), BeanB.createBeanB(2), BeanA.createBeanA(2)}) {
      checkMetaShared(fury, o);
    }
  }

  @Test(dataProvider = "enableCodegen")
  public void testShareClassDefCompatible(boolean enableCodegen) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .withMetaShare(true)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withCodegen(enableCodegen)
            .requireClassRegistration(false)
            .build();
    for (Object o : new Object[] {Foo.create(), BeanB.createBeanB(2), BeanA.createBeanA(2)}) {
      checkMetaShared(fury, o);
    }
  }

  private void checkMetaShared(Fury fury, Object o) {
    MetaContext context = new MetaContext();
    fury.getSerializationContext().setMetaContext(context);
    byte[] bytes = fury.serialize(o);
    fury.getSerializationContext().setMetaContext(context);
    Assert.assertEquals(fury.deserialize(bytes), o);
    fury.getSerializationContext().setMetaContext(context);
    byte[] bytes1 = fury.serialize(o);
    Assert.assertTrue(bytes1.length < bytes.length);
    fury.getSerializationContext().setMetaContext(context);
    Assert.assertEquals(fury.deserialize(bytes1), o);
    fury.getSerializationContext().setMetaContext(new MetaContext());
    Assert.assertEquals(fury.serialize(o), bytes);
    Assert.assertThrows(NullPointerException.class, () -> fury.serialize(o));
  }

  // final InnerPojo will be taken as non-final for writing class def.
  @Data
  @AllArgsConstructor
  public static final class InnerPojo {
    public Integer integer;
  }

  @Data
  @AllArgsConstructor
  public static class OuterPojo {
    public List<InnerPojo> list;
  }

  @Test(dataProvider = "enableCodegen")
  public void testFinalTypeWriteMeta(boolean enableCodegen) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .withMetaShare(true)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .withCodegen(enableCodegen)
            .requireClassRegistration(false)
            .build();
    OuterPojo outerPojo =
        new OuterPojo(new ArrayList<>(ImmutableList.of(new InnerPojo(1), new InnerPojo(2))));
    checkMetaShared(fury, outerPojo);
  }
}
