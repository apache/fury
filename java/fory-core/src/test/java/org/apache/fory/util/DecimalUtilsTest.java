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

import org.testng.Assert;
import org.testng.annotations.Test;

public class DecimalUtilsTest {

  @Test
  public void minBytesForPrecision() {
    for (int precision = 0; precision <= DecimalUtils.MAX_PRECISION - 2; precision++) {
      int bytes = DecimalUtils.minBytesForPrecision(precision);
      Assert.assertTrue(bytes >= 1);
      Assert.assertTrue(Math.pow(2, Math.max(0, (bytes - 1) * 8 - 1)) <= Math.pow(10, precision));
      Assert.assertTrue(Math.pow(2, bytes * 8 - 1) >= Math.pow(10, precision));
    }
  }
}
