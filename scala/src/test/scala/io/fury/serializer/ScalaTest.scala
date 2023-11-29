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

package object SomePackageObject {
  case class SomeClass(value: Int)
}

class ScalaTest extends AnyWordSpec with Matchers {
  "fury scala support" should {
    "serialize/deserialize package object" in {
      val fury = Fury.builder()
        .withLanguage(Language.JAVA)
        .withRefTracking(true)
        .withScalaOptimizationEnabled(true)
        .requireClassRegistration(false).build()

      val p = SomePackageObject.SomeClass(1)
      fury.deserialize(fury.serialize(p)) shouldEqual p
    }
  }
  "serialize/deserialize package object in app" in {
    PkgObjectMain.main(Array())
  }
}


package object PkgObject {
  case class Id(value: Int)
}

// Test for https://github.com/alipay/fury/issues/1165
object PkgObjectMain extends App {

  val fury = Fury
    .builder()
    .withScalaOptimizationEnabled(true)
    .requireClassRegistration(false)
    .withRefTracking(true)
    .build()

  import PkgObject._

  case class SomeClass(v: Id)
  val o1 = SomeClass(Id(1))
  val o2 = fury.deserialize(fury.serialize(o1))
  if (o1 != o2) {
    throw new RuntimeException(s"$o1 is not equal to $o2")
  }
}
