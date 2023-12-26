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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.util.StringUtils;
import org.testng.annotations.Test;

public class EnumStringResolverTest {

  @Test
  public void testWriteEnumString() {
    MemoryBuffer buffer = MemoryUtils.buffer(32);
    String str = StringUtils.random(128, 0);
    EnumStringResolver stringResolver = new EnumStringResolver();
    for (int i = 0; i < 128; i++) {
      stringResolver.writeEnumString(buffer, str);
    }
    for (int i = 0; i < 128; i++) {
      String enumString = stringResolver.readEnumString(buffer);
      assertEquals(enumString.hashCode(), str.hashCode());
      assertEquals(enumString.getBytes(), str.getBytes());
    }
    assertTrue(buffer.writerIndex() < str.getBytes().length + 128 * 4);
  }
}
