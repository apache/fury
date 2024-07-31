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

package org.apache.fury.serializer.collection;

public class MapFlags {

  /** Whether track key ref. */
  public static int TRACKING_KEY_REF = 0b1;

  /** Whether key has null. */
  public static int KEY_HAS_NULL = 0b10;

  //    /**
  //     * Whether key is not declare type.
  //     */
  //    public static int KEY_NOT_DECL_TYPE = 0b100;

  /** Whether keys type are different. */
  public static int KEY_NOT_SAME_TYPE = 0b100;

  /** Whether track value ref. */
  public static int TRACKING_VALUE_REF = 0b1000;

  /** Whether value has null. */
  public static int VALUE_HAS_NULL = 0b10000;

  /** Whether value is not declare type. */
  //    public static int VALUE_NOT_DECL_TYPE = 0b1000000;

  /** Whether values type are different. */
  public static int VALUE_NOT_SAME_TYPE = 0b100000;
}
