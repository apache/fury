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

package org.apache.fory.test.bean;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

public class ArraysData implements Serializable {
  public boolean[] booleans;
  public byte[] bytes;
  public int[] ints;
  public long[] longs;
  public double[] doubles;

  public ArraysData() {}

  public ArraysData(int arrLength) {
    booleans = new boolean[arrLength];
    bytes = new byte[arrLength];
    ints = new int[arrLength];
    longs = new long[arrLength];
    doubles = new double[arrLength];
    Random random = new Random();
    random.nextBytes(bytes);
    for (int i = 0; i < arrLength; i++) {
      booleans[i] = random.nextBoolean();
      ints[i] = random.nextInt();
      longs[i] = random.nextLong();
      doubles[i] = random.nextDouble();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ArraysData data = (ArraysData) o;
    return Arrays.equals(booleans, data.booleans)
        && Arrays.equals(bytes, data.bytes)
        && Arrays.equals(ints, data.ints)
        && Arrays.equals(longs, data.longs)
        && Arrays.equals(doubles, data.doubles);
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(booleans);
    result = 31 * result + Arrays.hashCode(bytes);
    result = 31 * result + Arrays.hashCode(ints);
    result = 31 * result + Arrays.hashCode(longs);
    result = 31 * result + Arrays.hashCode(doubles);
    return result;
  }
}
