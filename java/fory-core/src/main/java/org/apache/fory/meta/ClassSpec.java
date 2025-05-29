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

package org.apache.fory.meta;

import org.apache.fory.type.TypeUtils;

public class ClassSpec {
  public final String entireClassName;

  /** Whether current class is enum of component is enum if current class is array. */
  public final boolean isEnum;

  public final boolean isArray;
  public final int dimension;

  public ClassSpec(Class<?> cls) {
    this.entireClassName = cls.getName();
    isEnum = cls.isEnum();
    isArray = cls.isArray();
    dimension = isArray ? TypeUtils.getArrayDimensions(cls) : 0;
  }

  public ClassSpec(String entireClassName, boolean isEnum, boolean isArray, int dimension) {
    this.entireClassName = entireClassName;
    this.isEnum = isEnum;
    this.isArray = isArray;
    this.dimension = dimension;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ClassSpec classSpec = (ClassSpec) o;
    return entireClassName.equals(classSpec.entireClassName);
  }

  @Override
  public int hashCode() {
    return entireClassName.hashCode();
  }
}
