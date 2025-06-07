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

package org.apache.fory.serializer.kotlin

import kotlin.random.Random

internal object KotlinToJavaClass {
  // Collections
  val ArrayDequeClass = ArrayDeque::class.java
  val EmptyListClass = emptyList<Any>().javaClass
  val EmptySetClass = emptySet<Any>().javaClass
  val EmptyMapClass = emptyMap<Any, Any>().javaClass

  // Unsigned
  val UByteClass = UByte::class.java
  val UShortClass = UShort::class.java
  val UIntClass = UInt::class.java
  val ULongClass = ULong::class.java

  // Random
  val RandomInternalClass = Random(1)::class.java
  val RandomDefaultClass = Random.Default::class.java
  val RandomSerializedClass = Class.forName("kotlin.random.Random\$Default\$Serialized")

  // Regex
  val RegexSerializedClass = Class.forName("kotlin.text.Regex\$Serialized")
}
