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

import org.apache.fury.Fury;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.resolver.EnumStringResolver;
import org.apache.fury.util.Preconditions;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;


@SuppressWarnings("rawtypes")
public final class EnumNameSerializer extends ImmutableSerializer<Enum> {

  private final Map<String, Enum> enumConstantsMap;
  private final EnumStringResolver enumStringResolver;


  public EnumNameSerializer(Fury fury, Class<Enum> cls) {
    super(fury, cls, false);
    this.enumStringResolver = new EnumStringResolver();
    if (cls.isEnum()) {
      enumConstantsMap = Arrays.stream(cls.getEnumConstants())
              .collect(Collectors.toMap(Enum::name, Function.identity()));
    } else {
      Preconditions.checkArgument(Enum.class.isAssignableFrom(cls) && cls != Enum.class);
      @SuppressWarnings("unchecked")
      Class<Enum> enclosingClass = (Class<Enum>) cls.getEnclosingClass();
      Preconditions.checkNotNull(enclosingClass);
      Preconditions.checkArgument(enclosingClass.isEnum());
      enumConstantsMap = Arrays.stream(enclosingClass.getEnumConstants())
              .collect(Collectors.toMap(Enum::name, Function.identity()));
    }
  }

  @Override
  public void write(MemoryBuffer buffer, Enum value) {
    enumStringResolver.writeMetaStringBytes(buffer, value);
  }

  @Override
  public Enum read(MemoryBuffer buffer) {
    String key = enumStringResolver.readMetaString(buffer);
    if (!enumConstantsMap.containsKey(key)) {
      return handleNonexistentEnumValue(key);
    }
    return enumConstantsMap.get(key);
  }

  private Enum handleNonexistentEnumValue(String value) {
    if (fury.getConfig().deserializeNonexistentEnumValueAsNull()) {
      return null;
    } else {
      throw new IllegalArgumentException(
          String.format("Enum ordinal %s not in %s", value, Objects.nonNull(enumConstantsMap) ? enumConstantsMap.toString() : null));
    }
  }
}
