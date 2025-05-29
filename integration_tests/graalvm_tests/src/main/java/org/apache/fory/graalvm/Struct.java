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

package org.apache.fory.graalvm;

import java.io.Serializable;
import java.util.Objects;

public class Struct implements Serializable {
  public int f1;
  public long f2;
  public float f3;
  public double f4;
  public int f5;
  public long f6;
  public float f7;
  public double f8;
  public int f9;
  public long f10;
  public float f11;
  public double f12;

  public static Struct create() {
    Struct struct = new Struct();
    struct.f1 = 10;
    struct.f2 = 123456788;
    struct.f3 = 1 / 5f;
    struct.f4 = 1 / 7d;
    struct.f5 = 11;
    struct.f6 = 123456789;
    struct.f7 = 2 / 5f;
    struct.f8 = 2 / 7d;
    struct.f9 = 12;
    struct.f10 = 123456788;
    struct.f11 = 3 / 5f;
    struct.f12 = 3 / 7d;
    return struct;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Struct struct = (Struct) o;
    return f1 == struct.f1
        && f2 == struct.f2
        && Float.compare(f3, struct.f3) == 0
        && Double.compare(f4, struct.f4) == 0
        && f5 == struct.f5
        && f6 == struct.f6
        && Float.compare(f7, struct.f7) == 0
        && Double.compare(f8, struct.f8) == 0
        && f9 == struct.f9
        && f10 == struct.f10
        && Float.compare(f11, struct.f11) == 0
        && Double.compare(f12, struct.f12) == 0;
  }

  @Override
  public int hashCode() {
    return Objects.hash(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12);
  }
}
