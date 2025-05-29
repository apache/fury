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

package org.apache.fory.util.record;

import java.util.List;

/** Record build information. */
public class RecordInfo {
  private final int[] recordComponentsIndex;
  private final Object[] recordComponentsDefaultValues;
  private final Object[] recordComponents;

  public RecordInfo(Class<?> cls, List<String> fieldNames) {
    recordComponentsDefaultValues = RecordUtils.buildRecordComponentDefaultValues(cls);
    recordComponentsIndex = RecordUtils.buildRecordComponentMapping(cls, fieldNames);
    assert recordComponentsIndex != null;
    recordComponents = new Object[recordComponentsIndex.length];
  }

  public int[] getRecordComponentsIndex() {
    return recordComponentsIndex;
  }

  public Object[] getRecordComponentsDefaultValues() {
    return recordComponentsDefaultValues;
  }

  public Object[] getRecordComponents() {
    return recordComponents;
  }
}
