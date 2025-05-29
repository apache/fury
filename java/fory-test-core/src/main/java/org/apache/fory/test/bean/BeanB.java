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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import lombok.Data;

@Data
public final class BeanB implements Serializable {
  private short f1;
  private Integer f2;
  private long f3;
  private Float f4;
  private double f5;
  private int[] intArr;
  private List<Integer> intList;

  /** Create Object. */
  public static BeanB createBeanB(int arrSize) {
    Random rnd = new Random(37);
    BeanB beanB = new BeanB();
    beanB.setF1((short) rnd.nextInt());
    beanB.setF2(rnd.nextInt());
    beanB.setF3(rnd.nextLong());
    beanB.setF4(rnd.nextFloat());
    beanB.setF5(rnd.nextDouble());

    if (arrSize > 0) {
      {
        int[] arr = new int[arrSize];
        for (int i = 0; i < arr.length; i++) {
          arr[i] = rnd.nextInt();
        }
        beanB.setIntArr(arr);
      }
      {
        List<Integer> integers = new ArrayList<>();
        for (int i = 0; i < arrSize; i++) {
          integers.add(rnd.nextInt());
        }
        beanB.setIntList(integers);
      }
    }
    return beanB;
  }
}
