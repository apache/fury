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

package io.fury.format.row;

import io.fury.memory.MemoryBuffer;
import java.math.BigDecimal;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Getter methods for row format. {@link #isNullAt(int)} must be checked before attempting to
 * retrieve a nullable value.
 */
public interface Getters {

  boolean isNullAt(int ordinal);

  boolean getBoolean(int ordinal);

  byte getByte(int ordinal);

  short getShort(int ordinal);

  int getInt(int ordinal);

  long getLong(int ordinal);

  float getFloat(int ordinal);

  double getDouble(int ordinal);

  BigDecimal getDecimal(int ordinal);

  int getDate(int ordinal);

  long getTimestamp(int ordinal);

  String getString(int ordinal);

  byte[] getBinary(int ordinal);

  MemoryBuffer getBuffer(int ordinal);

  Row getStruct(int ordinal);

  ArrayData getArray(int ordinal);

  MapData getMap(int ordinal);

  default Object get(int ordinal, Field field) {
    return field.getType().accept(new ValueVisitor(this)).apply(ordinal);
  }
}
