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

package org.apache.fury.format.row.binary;

import java.util.Random;
import org.apache.fury.format.row.binary.writer.BinaryArrayWriter;
import org.apache.fury.format.type.DataTypes;
import org.apache.fury.logging.Logger;
import org.apache.fury.logging.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BinaryArrayTest {
  private static final Logger LOG = LoggerFactory.getLogger(BinaryArrayTest.class);

  @Test
  public void fromPrimitiveArray() {
    int[] arr = new int[] {1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2};
    BinaryArray.fromPrimitiveArray(arr);
    BinaryArrayWriter writer = new BinaryArrayWriter(DataTypes.PRIMITIVE_INT_ARRAY_FIELD);
    writer.reset(arr.length);
    writer.fromPrimitiveArray(arr);
    writer.toArray();
  }

  private int elem;

  @Test(enabled = false)
  public void testAccessPerf() {
    int length = 10000;
    int[] arr = new int[length];
    Random random = new Random();
    for (int i = 0; i < length; i++) {
      arr[i] = random.nextInt();
    }
    BinaryArray binaryArray = BinaryArray.fromPrimitiveArray(arr);
    int iterNums = 100_000;

    // warm
    for (int i = 0; i < iterNums; i++) {
      for (int j = 0; j < length; j++) {
        elem = arr[j];
      }
    }
    // test array
    long startTime = System.nanoTime();
    for (int i = 0; i < iterNums; i++) {
      for (int j = 0; j < length; j++) {
        elem = arr[j];
      }
    }
    long duration = System.nanoTime() - startTime;
    LOG.info("access array take " + duration + "ns, " + duration / 1000_000 + " ms\n");

    for (int i = 0; i < iterNums; i++) {
      for (int j = 0; j < length; j++) {
        elem = binaryArray.getInt32(j);
      }
    }
    // test binary array
    startTime = System.nanoTime();
    for (int i = 0; i < iterNums; i++) {
      for (int j = 0; j < length; j++) {
        elem = binaryArray.getInt32(j);
      }
    }
    duration = System.nanoTime() - startTime;
    LOG.info("access BinaryArray take " + duration + "ns, " + duration / 1000_000 + " ms\n");
  }

  @Test
  public void getDimensionsTest() {
    {
      int[] arr = new int[] {1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2};
      int[] dimensions = BinaryArray.getDimensions(BinaryArray.fromPrimitiveArray(arr), 1);
      Assert.assertEquals(dimensions, new int[] {arr.length});
    }

    {
      BinaryArrayWriter writer =
          new BinaryArrayWriter(DataTypes.arrayField(DataTypes.PRIMITIVE_INT_ARRAY_FIELD));
      writer.reset(4);
      int[] a = new int[] {1, 2, 1};
      writer.setNullAt(0);
      writer.setNullAt(1);
      writer.write(2, BinaryArray.fromPrimitiveArray(a));
      writer.write(3, BinaryArray.fromPrimitiveArray(a));
      BinaryArray array = writer.toArray();

      int[] dimensions = BinaryArray.getDimensions(array, 2);
      Assert.assertEquals(dimensions, new int[] {4, 3});
    }
  }
}
