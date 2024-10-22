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

package org.apache.fury.serializer.scala

import org.apache.fury.Fury
import org.apache.fury.config.Language
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.NumericRange

class RangeTest extends AnyWordSpec with Matchers {
  def fury: Fury = {
    val fury = Fury.builder()
      .withLanguage(Language.JAVA)
      .withRefTracking(true)
      .withScalaOptimizationEnabled(true)
      .requireClassRegistration(true)
      .suppressClassRegistrationWarnings(false).build()
    ScalaSerializers.registerSerializers(fury)
    fury
  }

  "fury scala range support" should {
    "serialize/deserialize range object" in {
      val v = Range.inclusive(1, 10)
      fury.deserialize(fury.serialize(v)) shouldEqual v
      (fury.serialize(v).length < 8) shouldBe true
      val v1 = Range.apply(1, 10)
      fury.deserialize(fury.serialize(v1)) shouldEqual v1
      (fury.serialize(v1).length < 8) shouldBe true
    }
    "serialize/deserialize numeric range object" in {
      val v = NumericRange.inclusive(1, 10, 1)
      fury.deserialize(fury.serialize(v)) shouldEqual v
      (fury.serialize(v).length < 12) shouldBe true
      val v1 = NumericRange.apply(1, 10, 1)
      fury.deserialize(fury.serialize(v1)) shouldEqual v1
      (fury.serialize(v1).length < 12) shouldBe true
    }
  }
}
