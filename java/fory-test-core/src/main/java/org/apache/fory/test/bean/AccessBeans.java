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

package org.apache.fory.test.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

public class AccessBeans {
  @Data
  @AllArgsConstructor
  private static class PrivateClass {
    public int f1;
    int f2;
    private int f3;
  }

  @Data
  @AllArgsConstructor
  private static final class FinalPrivateClass {
    public int f1;
    int f2;
    private int f3;
  }

  @Data
  @AllArgsConstructor
  static class DefaultLevelClass {
    public int f1;
    int f2;
    private int f3;
  }

  @Data
  @AllArgsConstructor
  public static class PublicClass {
    public int f1;
    int f2;
    private int f3;
    private DefaultLevelClass f4;
    private PrivateClass f5;
    private FinalPrivateClass f6;
  }

  public static PrivateClass createPrivateClassObject() {
    return new PrivateClass(1, 2, 3);
  }

  public static FinalPrivateClass createPrivateFinalClassObject() {
    return new FinalPrivateClass(1, 2, 3);
  }

  public static DefaultLevelClass createDefaultLevelClassObject() {
    return new DefaultLevelClass(4, 5, 6);
  }

  public static PublicClass createPublicClassObject() {
    return new PublicClass(
        1,
        2,
        3,
        createDefaultLevelClassObject(),
        createPrivateClassObject(),
        createPrivateFinalClassObject());
  }
}
