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

import org.testng.Assert;
import org.testng.annotations.Test;

public class ClassIdAllocatorTest {
  @Test
  public void testInvalidConstructorArguments() {
    Assert.assertThrows(
        NullPointerException.class,
        () -> {
          new ClassIdAllocator(null, null);
        });
  }

  @Test
  public void testInnerClass() {
    ClassIdAllocator allocator = new ClassIdAllocator((cls) -> true, (classId) -> true);
    int innerRegisteredCount = 10;
    for (int i = 0; i < innerRegisteredCount; i++) {
      allocator.notifyRegistrationEnd();
    }

    allocator.markInternalRegistrationEnd();
    // Inner Class
    Assert.assertTrue(allocator.isInnerClass((short) innerRegisteredCount));
    Assert.assertTrue(allocator.isInnerClass((short) (innerRegisteredCount - 1)));
    Assert.assertTrue(allocator.isInnerClass((short) 1));

    // Not Inner Class
    Assert.assertFalse(allocator.isInnerClass(null));
    Assert.assertFalse(allocator.isInnerClass(ClassIdAllocator.BuiltinClassId.NO_CLASS_ID));
    Assert.assertFalse(allocator.isInnerClass((short) 12));
  }

  @Test
  public void testAllocateClassId() {
    final int registeredClassId = 3;
    ClassIdAllocator allocator =
        new ClassIdAllocator((cls) -> false, (classId) -> classId <= registeredClassId);
    Assert.assertEquals(allocator.allocateClassId(this.getClass()), 4);
  }
}
