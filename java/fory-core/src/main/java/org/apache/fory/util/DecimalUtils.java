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

public class DecimalUtils {
  public static final int DECIMAL_BYTE_LENGTH = 32;
  public static final int MAX_PRECISION = 38;

  public static final int MAX_SCALE = 18;

  public static final int MAX_COMPACT_PRECISION = 18;

  private static final int[] minBytesForPrecision;

  static {
    minBytesForPrecision = new int[MAX_PRECISION + 1];
    for (int precision = 0; precision < minBytesForPrecision.length; precision++) {
      // 2^(8numBytes -1) >= 10^precision
      // (8numBytes -1) * ln2 >= precision * ln10
      // numBytes >= (precision * ln10/ln2 + 1)/8
      double tmp = (precision * Math.log(10) / Math.log(2) + 1) / 8;
      int numBytes = (int) Math.ceil(tmp);
      minBytesForPrecision[precision] = numBytes;
    }
  }

  public static int minBytesForPrecision(int precision) {
    Preconditions.checkArgument(0 <= precision && precision <= MAX_PRECISION);
    return minBytesForPrecision[precision];
  }
}
