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
import org.apache.fury.config.CompatibleMode
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import zio.Chunk

import scala.collection.immutable.ArraySeq

object SingletonObject {
  case object Query

  case class ArraySeqQuery(c: ArraySeq[Query.type])

  case class ArrayQuery(c: Array[Query.type])

  case class CaseChunk(c: Chunk[Int])
}

class CompatibleSingleObjectSerializerTest extends AnyWordSpec with Matchers {
  def fury: Fury = {
    org.apache.fury.Fury
      .builder()
      .withScalaOptimizationEnabled(true)
      .requireClassRegistration(false)
      .withRefTracking(true)
      .withCompatibleMode(CompatibleMode.COMPATIBLE)
      .suppressClassRegistrationWarnings(false)
      .build()
  }

  "fury scala object support" should {
    "serialize/deserialize" in {
      fury.deserialize(fury.serialize(singleton)) shouldBe singleton
      fury.deserialize(fury.serialize(Pair(singleton, singleton))) shouldEqual Pair(singleton, singleton)
    }
    "nested type serialization in object type" in {
      val x = A.B.C("hello, world!")
      val bytes = fury.serialize(x)
      fury.deserialize(bytes) shouldEqual A.B.C("hello, world!")
    }
    "testArraySeqQuery" in {
      val o = SingletonObject.ArraySeqQuery(ArraySeq(SingletonObject.Query))
      fury.deserialize(
        fury.serialize(
          o)) shouldEqual o
    }
    "testArrayQuery" in {
      val o = SingletonObject.ArrayQuery(Array(SingletonObject.Query))
      fury.deserialize(fury.serialize(o)).getClass shouldEqual o.getClass
    }
    "testCaseChunk" in {
      val o = SingletonObject.CaseChunk(Chunk(1))
      fury.deserialize(fury.serialize(o)) shouldEqual o
    }
  }
}
