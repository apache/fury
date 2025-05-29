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

package org.apache.fory.util.record;

import java.lang.reflect.Method;
import java.lang.reflect.Type;

/**
 * A RecordComponent provides information about, and dynamic access to, a component of a record
 * class.
 *
 * <p>See more details in java.lang.reflect.RecordComponent.
 */
public final class RecordComponent {
  // declaring class
  private final Class<?> declaringRecord;
  private final String name;
  private final Class<?> type;
  private final Type genericType;
  private final Method accessor;
  private final Object getter;

  public RecordComponent(
      Class<?> declaringRecord,
      String name,
      Class<?> type,
      Type genericType,
      Method accessor,
      Object getter) {
    this.declaringRecord = declaringRecord;
    this.name = name;
    this.type = type;
    this.genericType = genericType;
    this.accessor = accessor;
    this.getter = getter;
  }

  /**
   * Returns the record class which declares this record component.
   *
   * @return The record class declaring this record component.
   */
  public Class<?> getDeclaringRecord() {
    return declaringRecord;
  }

  public String getName() {
    return name;
  }

  /**
   * Returns a {@code Class} that identifies the declared type for this record component.
   *
   * @return a {@code Class} identifying the declared type of the component represented by this
   *     record component
   */
  public Class<?> getType() {
    return type;
  }

  /**
   * Returns a {@code Type} object that represents the declared type for this record component.
   *
   * <p>If the declared type of the record component is a parameterized type, the {@code Type}
   * object returned reflects the actual type arguments used in the source code.
   *
   * <p>If the type of the underlying record component is a type variable or a parameterized type,
   * it is created. Otherwise, it is resolved.
   *
   * @return a {@code Type} object that represents the declared type for this record component
   */
  public Type getGenericType() {
    return genericType;
  }

  /**
   * Returns a {@code Method} that represents the accessor for this record component.
   *
   * @return a {@code Method} that represents the accessor for this record component
   */
  public Method getAccessor() {
    return accessor;
  }

  public Object getGetter() {
    return getter;
  }
}
