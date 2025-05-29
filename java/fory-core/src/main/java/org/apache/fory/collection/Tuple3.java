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

package org.apache.fory.collection;

import java.io.Serializable;
import java.util.Objects;

public class Tuple3<T0, T1, T2> implements Serializable {
  private static final long serialVersionUID = 1L;

  /** Field 0 of the tuple. */
  public final T0 f0;

  /** Field 1 of the tuple. */
  public final T1 f1;

  /** Field 2 of the tuple. */
  public final T2 f2;

  /** Creates a new tuple where all fields are null. */
  public Tuple3() {
    this(null, null, null);
  }

  /**
   * Creates a new tuple and assigns the given values to the tuple's fields, with field value final.
   * In case field value is nonFinal, use {@link MutableTuple3}
   *
   * @param value0 The value for field 0
   * @param value1 The value for field 1
   * @param value2 The value for field 2
   */
  public Tuple3(T0 value0, T1 value1, T2 value2) {
    this.f0 = value0;
    this.f1 = value1;
    this.f2 = value2;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Tuple3<?, ?, ?> tuple3 = (Tuple3<?, ?, ?>) o;
    return Objects.equals(f0, tuple3.f0)
        && Objects.equals(f1, tuple3.f1)
        && Objects.equals(f2, tuple3.f2);
  }

  @Override
  public int hashCode() {
    return Objects.hash(f0, f1, f2);
  }

  public static <T0, T1, T2> Tuple3<T0, T1, T2> of(T0 value0, T1 value1, T2 value2) {
    return new Tuple3<>(value0, value1, value2);
  }

  @Override
  public String toString() {
    return "Tuple3(" + f0 + ", " + f1 + ", " + f2 + ')';
  }
}
