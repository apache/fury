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

package org.apache.fory.serializer;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.fory.Fory;
import org.apache.fory.ForyTestBase;
import org.apache.fory.config.CompatibleMode;
import org.apache.fory.config.ForyBuilder;
import org.apache.fory.reflect.ReflectionUtils;
import org.apache.fory.test.bean.Foo;
import org.testng.Assert;
import org.testng.annotations.Test;

// Most scoped meta share tests are located in CompatibleTest module.
public class ScopedMetaShareTest extends ForyTestBase {

  // Test registration doesn't skip write class meta
  @Test
  public void testRegister() throws Exception {
    Supplier<ForyBuilder> builder =
        () ->
            builder()
                .withCodegen(true)
                .withCompatibleMode(CompatibleMode.COMPATIBLE)
                .withScopedMetaShare(true);
    Object foo = Foo.create();
    Class<?> fooClass = Foo.createCompatibleClass1();
    Object newFoo = fooClass.newInstance();
    ReflectionUtils.unsafeCopy(foo, newFoo);
    Fory fory = builder.get().build();
    fory.register(Foo.class);
    Fory newFury = builder.get().withClassLoader(fooClass.getClassLoader()).build();
    newFury.register(fooClass);
    {
      byte[] foo1Bytes = newFury.serialize(newFoo);
      Object deserialized = fory.deserialize(foo1Bytes);
      Assert.assertEquals(deserialized.getClass(), Foo.class);
      Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(deserialized, newFoo));
      byte[] fooBytes = fory.serialize(deserialized);
      Assert.assertTrue(ReflectionUtils.objectFieldsEquals(newFury.deserialize(fooBytes), newFoo));
    }
    {
      byte[] bytes1 = fory.serialize(foo);
      Object o1 = newFury.deserialize(bytes1);
      Assert.assertTrue(ReflectionUtils.objectCommonFieldsEquals(o1, foo));
      Object o2 = fory.deserialize(newFury.serialize(o1));
      List<String> fields =
          Arrays.stream(fooClass.getDeclaredFields())
              .map(f -> f.getDeclaringClass().getSimpleName() + f.getName())
              .collect(Collectors.toList());
      Assert.assertTrue(ReflectionUtils.objectFieldsEquals(new HashSet<>(fields), o2, foo));
    }
    {
      Object o3 = fory.deserialize(newFury.serialize(foo));
      Assert.assertTrue(ReflectionUtils.objectFieldsEquals(o3, foo));
    }
  }
}
