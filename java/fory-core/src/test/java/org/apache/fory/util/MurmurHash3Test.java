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

package org.apache.fory.util;

import com.google.common.hash.Hashing;
import java.util.Random;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MurmurHash3Test {

  @Test
  public void testMurmurhash3_x64_128() {
    Random random = new Random(17);
    for (int i = 0; i < 128; i++) {
      byte[] bytes = new byte[i];
      random.nextBytes(bytes);
      long hashCode1 = MurmurHash3.murmurhash3_x64_128(bytes, 0, bytes.length, 47)[0];
      long hashCode2 = Hashing.murmur3_128(47).hashBytes(bytes).asLong();
      Assert.assertEquals(hashCode1, hashCode2);
    }
  }
}
