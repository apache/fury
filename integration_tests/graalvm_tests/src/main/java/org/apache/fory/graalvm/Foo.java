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
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Foo implements Serializable {
  int f1;
  String f2;
  List<String> f3;
  Map<String, Long> f4;

  public Foo(int f1, String f2, List<String> f3, Map<String, Long> f4) {
    this.f1 = f1;
    this.f2 = f2;
    this.f3 = f3;
    this.f4 = f4;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Foo foo = (Foo) o;
    return f1 == foo.f1
        && Objects.equals(f2, foo.f2)
        && Objects.equals(f3, foo.f3)
        && Objects.equals(f4, foo.f4);
  }

  @Override
  public int hashCode() {
    return Objects.hash(f1, f2, f3, f4);
  }

  @Override
  public String toString() {
    return "Foo{" + "f1=" + f1 + ", f2='" + f2 + '\'' + ", f3=" + f3 + ", f4=" + f4 + '}';
  }
}
