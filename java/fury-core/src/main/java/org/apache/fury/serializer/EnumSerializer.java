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

package org.apache.fury.serializer;

import java.util.Arrays;
import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.util.Preconditions;

@SuppressWarnings("rawtypes")
public final class EnumSerializer extends Serializer<Enum> {
  private final Enum[] enumConstants;

  public EnumSerializer(Fury fury, Class<Enum> cls) {
    super(fury, cls, false);
    if (cls.isEnum()) {
      enumConstants = cls.getEnumConstants();
    } else {
      Preconditions.checkArgument(Enum.class.isAssignableFrom(cls) && cls != Enum.class);
      @SuppressWarnings("unchecked")
      Class<Enum> enclosingClass = (Class<Enum>) cls.getEnclosingClass();
      Preconditions.checkNotNull(enclosingClass);
      Preconditions.checkArgument(enclosingClass.isEnum());
      enumConstants = enclosingClass.getEnumConstants();
    }
  }

  @Override
  public void write(MemoryBuffer buffer, Enum value) {
    buffer.writeVarUint32Small7(value.ordinal());
  }

  @Override
  public Enum read(MemoryBuffer buffer) {
    int value = buffer.readVarUint32Small7();
    if (value >= enumConstants.length) {
      return handleNonexistentEnumValue(value);
    }
    return enumConstants[value];
  }

  private Enum handleNonexistentEnumValue(int value) {
    if (fury.getConfig().deserializeNonexistentEnumValueAsNull()) {
      return null;
    } else {
      throw new IllegalArgumentException(
          String.format("Enum ordinal %s not in %s", value, Arrays.toString(enumConstants)));
    }
  }
}
