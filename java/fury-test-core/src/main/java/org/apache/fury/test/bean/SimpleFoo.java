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

package org.apache.fury.test.bean;

import java.util.Objects;

public class SimpleFoo {
  public int f1;
  public String f2;

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SimpleFoo foo = (SimpleFoo) o;
    return f1 == foo.f1 && Objects.equals(f2, foo.f2);
  }

  @Override
  public int hashCode() {
    return Objects.hash(f1, f2);
  }

  @Override
  public String toString() {
    return "SimpleFoo{" + "f1=" + f1 + ", f2='" + f2 + '\'' + '}';
  }

  public static SimpleFoo create() {
    SimpleFoo foo = new SimpleFoo();
    foo.f1 = 10;
    foo.f2 = "str";
    return foo;
  }
}
