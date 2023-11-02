/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.serializer

import io.fury.Fury
import io.fury.config.Language
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TupleTest extends AnyWordSpec with Matchers {
  val fury: Fury = Fury.builder()
    .withLanguage(Language.JAVA)
    .withRefTracking(true)
    .withScalaOptimizationEnabled(true)
    .requireClassRegistration(false).build()

  "fury scala collection support" should {
    "serialize/deserialize Seq" in {
      val seq = Seq(100, 10000L)
      fury.deserialize(fury.serialize(seq)) shouldEqual seq
    }
    "serialize/deserialize List" in {
      val list = List(100, 10000L)
      fury.deserialize(fury.serialize(list)) shouldEqual list
    }
    "serialize/deserialize Set" in {
      val set = Set(100, 10000L)
      fury.deserialize(fury.serialize(set)) shouldEqual set
    }
  }
  "fury scala map support" should {
    "serialize/deserialize Map" in {
      val map = Map("a" -> 100, "b" -> 10000L)
      fury.deserialize(fury.serialize(map)) shouldEqual map
    }
  }
}
