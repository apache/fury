/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.benchmark.data;

import io.fury.util.StringUtils;

public class Data {
  // array
  public int[] ints = {
    0,
    1,
    2,
    3,
    4,
    5,
    63,
    64,
    65,
    127,
    128,
    129,
    4000,
    5000,
    6000,
    16000,
    32000,
    256000,
    1024000,
    -1,
    -2,
    -3,
    -4,
    Integer.MIN_VALUE,
    Integer.MAX_VALUE
  };
  public long[] longs = {
    0,
    1,
    2,
    3,
    4,
    5,
    63,
    64,
    65,
    127,
    128,
    129,
    4000,
    5000,
    6000,
    16000,
    32000,
    256000,
    1024000,
    -1,
    -2,
    -3,
    -4,
    Integer.MIN_VALUE,
    Integer.MAX_VALUE,
    Long.MIN_VALUE,
    Long.MAX_VALUE,
    9999999999L
  };

  // string
  public String str = "abc0123456789";
  public String longStr = newLongStr();

  public static String newLongStr() {
    String strLengthStr = System.getenv("LONG_STR_LENGTH");
    int strLength = 1024;
    if (StringUtils.isNotBlank(strLengthStr)) {
      strLength = Integer.parseInt(strLengthStr);
    }
    return StringUtils.random(strLength);
  }
}
