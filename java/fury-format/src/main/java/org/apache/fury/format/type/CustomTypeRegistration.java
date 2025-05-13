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

package org.apache.fury.format.type;

import java.util.Objects;

public class CustomTypeRegistration {
  private final Class<?> beanType;
  private final Class<?> fieldType;

  public CustomTypeRegistration(final Class<?> beanType, final Class<?> fieldType) {
    this.beanType = beanType;
    this.fieldType = fieldType;
  }

  public Class<?> getBeanType() {
    return beanType;
  }

  public Class<?> getFieldType() {
    return fieldType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(beanType, fieldType);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final CustomTypeRegistration other = (CustomTypeRegistration) obj;
    return Objects.equals(beanType, other.beanType) && Objects.equals(fieldType, other.fieldType);
  }

  @Override
  public String toString() {
    return "CustomTypeRegistration [beanType=" + beanType + ", fieldType=" + fieldType + "]";
  }
}
