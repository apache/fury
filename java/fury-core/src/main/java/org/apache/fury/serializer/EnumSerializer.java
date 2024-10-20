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
import java.util.HashMap;
import java.util.Map;
import org.apache.fury.Fury;
import org.apache.fury.config.Language;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.meta.MetaString;
import org.apache.fury.resolver.MetaStringResolver;
import org.apache.fury.util.Preconditions;

@SuppressWarnings("rawtypes")
public class EnumSerializer extends ImmutableSerializer<Enum> {
  private static final MetaStringResolver META_STRING_RESOLVER = new MetaStringResolver();
  private final Enum[] enumConstants;
  private final Map<String, Enum> enumStringRepresentation;

  public EnumSerializer(Fury fury, Class<Enum> cls) {
    super(fury, cls, false);

    enumStringRepresentation = new HashMap<>();

    if (cls.isEnum()) {
      enumConstants = cls.getEnumConstants();
      for (Enum e : enumConstants) {
        enumStringRepresentation.put(e.name(), e);
      }
    } else {
      Preconditions.checkArgument(Enum.class.isAssignableFrom(cls) && cls != Enum.class);
      @SuppressWarnings("unchecked")
      Class<Enum> enclosingClass = (Class<Enum>) cls.getEnclosingClass();
      Preconditions.checkNotNull(enclosingClass);
      Preconditions.checkArgument(enclosingClass.isEnum());
      enumConstants = enclosingClass.getEnumConstants();
      for (Enum e : enumConstants) {
        enumStringRepresentation.put(e.name(), e);
      }
    }
  }

  @Override
  public void write(MemoryBuffer buffer, Enum value) {
    if (fury.getConfig().treatEnumAsString()) {
//      if (fury.getConfig().getLanguage() != Language.JAVA) {
//        throw new UnsupportedOperationException("treatEnumAsString can only be used in java");
//      }
      META_STRING_RESOLVER.writeMetaStringBytesFromString(
          buffer, value.name(), MetaString.Encoding.UTF_8);
    } else {
      buffer.writeVarUint32Small7(value.ordinal());
    }
  }

  @Override
  public Enum read(MemoryBuffer buffer) {
    if (fury.getConfig().treatEnumAsString()) {
//      if (fury.getConfig().getLanguage() != Language.JAVA) {
//        throw new UnsupportedOperationException("treatEnumAsString can only be used in java");
//      }
      String metaStringBytes = META_STRING_RESOLVER.readMetaString(buffer);
      Enum e = enumStringRepresentation.get(metaStringBytes);
      if (e != null) {
        return e;
      }
      return handleNonexistentEnumValue(metaStringBytes);
    } else {
      int value = buffer.readVarUint32Small7();
      if (value >= enumConstants.length) {
        return handleNonexistentEnumValue(value);
      }
      return enumConstants[value];
    }
  }

  private Enum handleNonexistentEnumValue(int value) {
    if (fury.getConfig().deserializeNonexistentEnumValueAsNull()) {
      return null;
    } else {
      throw new IllegalArgumentException(
          String.format("Enum ordinal %s not in %s", value, Arrays.toString(enumConstants)));
    }
  }

  private Enum handleNonexistentEnumValue(String value) {
    if (fury.getConfig().deserializeNonexistentEnumValueAsNull()) {
      return null;
    } else {
      throw new IllegalArgumentException(
          String.format("Enum string %s not in %s", value, Arrays.toString(enumConstants)));
    }
  }
}
