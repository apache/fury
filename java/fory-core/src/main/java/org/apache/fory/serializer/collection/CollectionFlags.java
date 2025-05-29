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

package org.apache.fory.serializer.collection;

/**
 * Default unset bitmap flags.
 *
 * <ul>
 *   <li>TRACKING_REF: false
 *   <li>HAS_NULL: false
 *   <li>NOT_DECL_ELEMENT_TYPE: false
 *   <li>NOT_SAME_TYPE: false
 * </ul>
 */
public class CollectionFlags {
  /** Whether track elements ref. */
  public static int TRACKING_REF = 0b1;

  /** Whether collection has null. */
  public static int HAS_NULL = 0b10;

  /** Whether collection elements type is not declare type. */
  public static int NOT_DECL_ELEMENT_TYPE = 0b100;

  /** Whether collection elements type different. */
  public static int NOT_SAME_TYPE = 0b1000;
}
