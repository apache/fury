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

package org.apache.fory.reflect;

import lombok.AllArgsConstructor;
import org.apache.fory.reflect.FieldAccessor.GeneratedAccessor;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FieldAccessorTest {
  @AllArgsConstructor
  private static final class TestStruct {
    private int f1;
    private boolean f2;
    private String f3;
  }

  @Test
  public void testGeneratedAccessor() throws Exception {
    TestStruct struct = new TestStruct(10, true, "str");
    GeneratedAccessor f1 = new GeneratedAccessor(TestStruct.class.getDeclaredField("f1"));
    Assert.assertEquals(f1.get(struct), 10);
    f1.set(struct, 20);
    Assert.assertEquals(f1.get(struct), 20);
    GeneratedAccessor f2 = new GeneratedAccessor(TestStruct.class.getDeclaredField("f2"));
    Assert.assertEquals(f2.get(struct), true);
    f2.set(struct, false);
    Assert.assertEquals(f2.get(struct), false);
    GeneratedAccessor f3 = new GeneratedAccessor(TestStruct.class.getDeclaredField("f3"));
    Assert.assertEquals(f3.get(struct), "str");
    f3.set(struct, "a");
    Assert.assertEquals(f3.get(struct), "a");
  }
}
