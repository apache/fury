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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import io.fury.Fury;
import io.fury.FuryTestBase;
import io.fury.Language;
import io.fury.type.GenericType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.TreeSet;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CollectionSerializersTest extends FuryTestBase {
  @Test(dataProvider = "referenceTrackingConfig")
  public void testBasicList(boolean referenceTrackingConfig) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withReferenceTracking(referenceTrackingConfig)
            .disableSecureMode()
            .build();
    List<String> data = new ArrayList<>(ImmutableList.of("a", "b", "c"));
    serDeCheckSerializer(fury, data, "ArrayList");
    serDeCheckSerializer(fury, Arrays.asList("a", "b", "c"), "ArraysAsList");
    serDeCheckSerializer(fury, new HashSet<>(data), "HashSet");
    serDeCheckSerializer(fury, new LinkedHashSet<>(data), "LinkedHashSet");
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testCollectionGenerics(boolean referenceTrackingConfig) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withReferenceTracking(referenceTrackingConfig)
            .disableSecureMode()
            .build();
    List<String> data = new ArrayList<>(ImmutableList.of("a", "b", "c"));
    byte[] bytes1 = fury.serialize(data);
    fury.getGenerics().pushGenericType(GenericType.build(new TypeToken<List<String>>() {}));
    byte[] bytes2 = fury.serialize(data);
    Assert.assertTrue(bytes1.length > bytes2.length);
    System.out.println(fury.deserialize(bytes2));
    fury.getGenerics().popGenericType();
    Assert.assertThrows(RuntimeException.class, () -> fury.deserialize(bytes2));
  }

  @Test(dataProvider = "referenceTrackingConfig")
  public void testSortedSet(boolean referenceTrackingConfig) {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withReferenceTracking(referenceTrackingConfig)
            .disableSecureMode()
            .build();
    TreeSet<String> data = new TreeSet<>(ImmutableSet.of("a", "b", "c"));
    serDeCheckSerializer(fury, data, "SortedSet");
    byte[] bytes1 = fury.serialize(data);
    fury.getGenerics().pushGenericType(GenericType.build(new TypeToken<List<String>>() {}));
    byte[] bytes2 = fury.serialize(data);
    Assert.assertTrue(bytes1.length > bytes2.length);
    System.out.println(fury.deserialize(bytes2));
    fury.getGenerics().popGenericType();
    Assert.assertThrows(RuntimeException.class, () -> fury.deserialize(bytes2));
  }

  @Test
  public void testEmptyCollection() {
    Fury fury = Fury.builder().withLanguage(Language.JAVA).build();
    serDeCheckSerializer(fury, Collections.EMPTY_LIST, "EmptyListSerializer");
    serDeCheckSerializer(fury, Collections.emptySortedSet(), "EmptySortedSetSerializer");
    serDeCheckSerializer(fury, Collections.EMPTY_SET, "EmptySetSerializer");
  }
}
