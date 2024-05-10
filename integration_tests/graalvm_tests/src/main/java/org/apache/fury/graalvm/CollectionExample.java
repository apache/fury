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

package org.apache.fury.graalvm;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.fury.Fury;
import org.apache.fury.util.Preconditions;

public class CollectionExample {
  static Fury fury;

  static {
    fury = Fury.builder().requireClassRegistration(true).build();
  }

  static void test(Fury fury) {
    final Map<String, String> unmodifiableMap = Map.of("k1", "v1", "k2", "v2");
    Preconditions.checkArgument(
        unmodifiableMap.equals(fury.deserialize(fury.serialize(unmodifiableMap))));
    System.out.println(unmodifiableMap);
    final List<Integer> arrayasList = Arrays.asList(1, 2, 3);
    Preconditions.checkArgument(arrayasList.equals(fury.deserialize(fury.serialize(arrayasList))));
    System.out.println(arrayasList);
    final Set<String> setFromMap = Collections.newSetFromMap(new ConcurrentHashMap<>());
    setFromMap.add("a");
    setFromMap.add("b");
    setFromMap.add("c");
    Preconditions.checkArgument(setFromMap.equals(fury.deserialize(fury.serialize(setFromMap))));
    System.out.println(setFromMap);
  }

  public static void main(String[] args) {
    test(fury);
    System.out.println("Collection succeed");
  }
}
