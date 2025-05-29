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

package org.apache.fory.integration_tests;

public class Records {
  private record PrivateRecord(int f1) {}

  public record PublicRecord(int f1, PrivateRecord f2) {}

  public static Object createPrivateRecord(int f1) {
    return new PrivateRecord(f1);
  }

  public static Object createPublicRecord(int f1, Object privateObject) {
    return new PublicRecord(f1, (PrivateRecord) privateObject);
  }
}
