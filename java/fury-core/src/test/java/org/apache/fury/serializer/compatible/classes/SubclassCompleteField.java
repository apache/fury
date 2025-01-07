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

package org.apache.fury.serializer.compatible.classes;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class SubclassCompleteField {
  private boolean privateBoolean = true;
  private int privateInt = 10;
  private String privateString = "notNull";
  private Map<String, String> privateMap;
  private List<String> privateList;

  private boolean privateBoolean2 = true;
  private int privateInt2 = 10;
  private String privateString2 = "notNull";
  private Map<String, String> privateMap2;
  private List<String> privateList2;

  public SubclassCompleteField() {
    privateMap = new HashMap<>();
    privateMap.put("a", "b");
    privateList = new ArrayList<>();
    privateList.add("a");

    privateMap2 = new HashMap<>();
    privateMap2.put("a", "b");
    privateList2 = new ArrayList<>();
    privateList2.add("a");
  }
}
