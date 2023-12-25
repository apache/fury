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

package io.fury.integration_tests;

import static io.fury.integration_tests.TestUtils.serDeCheck;

import io.fury.Fury;
import io.fury.test.bean.CollectionFields;
import io.fury.test.bean.MapFields;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ImmutableCollectionSerializersTest {

  @DataProvider
  public static Object[][] codegen() {
    return new Object[][] {{false}, {true}};
  }

  @Test(dataProvider = "codegen")
  public void testImmutableCollections(boolean codegen) {
    Fury fury = Fury.builder().withCodegen(codegen).build();
    serDeCheck(fury, List.of());
    serDeCheck(fury, List.of("A"));
    serDeCheck(fury, List.of("A", "B"));
    serDeCheck(fury, List.of("A", "B", "C"));
    serDeCheck(fury, List.of("A", "B", "C", "D"));
    serDeCheck(fury, List.of("A", "B", 1, 2));
    serDeCheck(fury, Set.of());
    serDeCheck(fury, Set.of("A"));
    serDeCheck(fury, Set.of("A", "B"));
    serDeCheck(fury, Set.of("A", "B", "C"));
    serDeCheck(fury, Set.of("A", "B", "C", "D"));
    serDeCheck(fury, Set.of("A", "B", 1, 2));
    serDeCheck(fury, Map.of());
    serDeCheck(fury, Map.of("A", "B"));
    serDeCheck(fury, Map.of("A", "B", 1, 2));
  }

  @Test(dataProvider = "codegen")
  public void testImmutableCollectionStruct(boolean codegen) {
    Fury fury = Fury.builder().withCodegen(codegen).build();
    fury.register(MapFields.class);
    MapFields mapFields = new MapFields();
    mapFields.map = Map.of();
    mapFields.map2 = Map.of("k", 1);
    mapFields.mapKeyFinal = Map.of("k", 1);
    mapFields.mapValueFinal = Map.of("k", 2, "k2", 2, "k3", 3);
    mapFields.emptyMap = Map.of();
    mapFields.singletonMap = Map.of(1, 2);
    serDeCheck(fury, mapFields);
  }

  @Test
  public void testImmutableMapStruct() {
    Fury fury = Fury.builder().build();
    fury.register(CollectionFields.class);
    CollectionFields collectionFields = new CollectionFields();
    collectionFields.collection = List.of();
    collectionFields.collection2 = List.of(1);
    collectionFields.collection3 = List.of(1, 2, 3, 4);
    collectionFields.randomAccessList = List.of("1", "2");
    collectionFields.randomAccessList2 = List.of("1", "2", "3", "4");
    collectionFields.set = Set.of();
    collectionFields.set2 = Set.of("1", "2");
    collectionFields.set3 = Set.of("1", "2", "3", "4");
    collectionFields.map = Map.of("1", "2");
    collectionFields.map2 = Map.of("1", "2", "3", "4");
    serDeCheck(fury, collectionFields);
  }
}
