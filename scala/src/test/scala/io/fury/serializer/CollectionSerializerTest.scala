package io.fury.serializer

import io.fury.Fury
import io.fury.config.Language
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CollectionSerializerTest extends AnyWordSpec with Matchers {
  val fury: Fury = Fury.builder()
    .withLanguage(Language.JAVA)
    .withRefTracking(true)
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
