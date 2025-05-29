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

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class UnsafeFieldAccessorTest {

  class A {
    int f1 = 1;
    int f2 = 2;
  }

  class B extends A {
    int f1 = 2;
  }

  @Test
  public void testRepeatedFields() {
    assertEquals(new UnsafeFieldAccessor(A.class, "f1").getInt(new A()), 1);
    assertEquals(new UnsafeFieldAccessor(A.class, "f2").getInt(new A()), 2);
    assertEquals(new UnsafeFieldAccessor(B.class, "f1").getInt(new B()), 2);
    assertEquals(new UnsafeFieldAccessor(B.class, "f2").getInt(new B()), 2);
  }
}
