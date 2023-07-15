/*
 * Copyright 2023 The Fury authors
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.serializer;

import static org.testng.Assert.*;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.fury.Fury;
import io.fury.FuryTestBase;
import io.fury.Language;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GuavaSerializersTest extends FuryTestBase {

  @Test
  public void testImmutableListSerializer() {
    serDe(getJavaFury(), ImmutableList.of(1, 2));
    Assert.assertEquals(
        getJavaFury().getClassResolver().getSerializerClass(ImmutableList.of(1, 2).getClass()),
        GuavaSerializers.ImmutableListSerializer.class);
  }

  @Test
  public void testImmutableMapSerializer() {
    serDe(getJavaFury(), ImmutableMap.of("k1", 1, "k2", 2));
    Assert.assertEquals(
        getJavaFury()
            .getClassResolver()
            .getSerializerClass(ImmutableMap.of("k1", 1, "k2", 2).getClass()),
        GuavaSerializers.ImmutableMapSerializer.class);
  }

  @Test
  public void tesXlangSerialize() {
    Fury fury = Fury.builder().withLanguage(Language.XLANG).build();
    serDe(fury, ImmutableBiMap.of());
    serDe(fury, ImmutableBiMap.of(1, 2));
    serDe(fury, ImmutableBiMap.of(1, 2, 3, 4));

    serDe(fury, ImmutableList.of());
    serDe(fury, ImmutableList.of(1));
    serDe(fury, ImmutableList.of(1, 2));

    serDe(fury, ImmutableSet.of());
    serDe(fury, ImmutableSet.of(1));
    serDe(fury, ImmutableSet.of(1, 2, 3, 4));
  }
}
