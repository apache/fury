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
import org.apache.fury.meta.MetaString;
import org.apache.fury.resolver.MetaStringBytes;
import org.apache.fury.resolver.MetaStringResolver;
import org.apache.fury.util.Preconditions;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.fury.meta.Encoders.GENERIC_ENCODER;


@SuppressWarnings("rawtypes")
public final class EnumByNameSerializer extends ImmutableSerializer<Enum> {

  private final Map<MetaStringBytes, Enum> enumMap;
  private final MetaStringResolver metaStringResolver;


  public EnumByNameSerializer(Fury fury, Class<Enum> cls) {
    super(fury, cls, false);
    this.metaStringResolver = new MetaStringResolver();
    if (cls.isEnum()) {
        enumMap = Arrays.stream(cls.getEnumConstants())
                .filter(Objects::nonNull)
                .map(e -> {
                    MetaStringBytes stringBytes =
                            this.metaStringResolver.getOrCreateMetaStringBytes(GENERIC_ENCODER.encode(e.name(), MetaString.Encoding.UTF_8));
                    return new AbstractMap.SimpleEntry<>(stringBytes, e);
                })
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    } else {
      Preconditions.checkArgument(Enum.class.isAssignableFrom(cls) && cls != Enum.class);
      @SuppressWarnings("unchecked")
      Class<Enum> enclosingClass = (Class<Enum>) cls.getEnclosingClass();
      Preconditions.checkNotNull(enclosingClass);
      Preconditions.checkArgument(enclosingClass.isEnum());
      enumMap = Arrays.stream(enclosingClass.getEnumConstants())
              .filter(Objects::nonNull)
                .map(e -> {
                    MetaStringBytes stringBytes =
                            this.metaStringResolver.getOrCreateMetaStringBytes(GENERIC_ENCODER.encode(e.name(), MetaString.Encoding.UTF_8));
                    return new AbstractMap.SimpleEntry<>(stringBytes, e);
                })
                .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }
  }

  @Override
  public void write(MemoryBuffer buffer, Enum value) {
      MetaStringBytes metaStringBytes = metaStringResolver.getOrCreateMetaStringBytes(
              GENERIC_ENCODER.encode(value.name(), MetaString.Encoding.UTF_8));
      metaStringResolver.writeMetaStringBytes(buffer, metaStringBytes);
  }

  @Override
  public Enum read(MemoryBuffer buffer) {
      MetaStringBytes metaStringBytes = metaStringResolver.readMetaStringBytes(buffer);
      if (!enumMap.containsKey(metaStringBytes)) {
        return handleNonexistentEnumValue(metaStringBytes);
      }
      return enumMap.get(metaStringBytes);
  }

    private Enum handleNonexistentEnumValue(MetaStringBytes metaStringBytes) {
        if (fury.getConfig().deserializeNonexistentEnumValueAsNull()) {
            return null;
        } else {
            throw new IllegalArgumentException(
                    String.format("MetaStringBytes %s not in %s", metaStringBytes, null == enumMap ? null : enumMap.toString()));
        }
    }
}
