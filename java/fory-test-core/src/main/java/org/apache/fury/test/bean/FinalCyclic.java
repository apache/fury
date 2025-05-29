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

import java.util.Objects;

public final class FinalCyclic {
  public FinalCyclic cyclic;
  public String f1;

  @SuppressWarnings("EqualsWrongThing")
  @Override
  public boolean equals(Object object) {
    if (this == object) {
      return true;
    }
    if (object == null || getClass() != object.getClass()) {
      return false;
    }
    FinalCyclic cyclic1 = (FinalCyclic) object;
    if (cyclic != this) {
      return Objects.equals(cyclic, cyclic1.cyclic) && Objects.equals(f1, cyclic1.f1);
    } else {
      return cyclic1.cyclic == cyclic1 && Objects.equals(f1, cyclic1.f1);
    }
  }

  @Override
  public int hashCode() {
    if (cyclic != this) {
      return Objects.hash(cyclic, f1);
    } else {
      return f1.hashCode();
    }
  }

  /** Create Object. */
  public static FinalCyclic create(boolean circular) {
    FinalCyclic cyclic = new FinalCyclic();
    cyclic.f1 = "str";
    if (circular) {
      cyclic.cyclic = cyclic;
    }
    return cyclic;
  }
}
