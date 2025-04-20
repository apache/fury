---
title: Scala Serialization Guide
sidebar_position: 4
id: scala_guide
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

Fury supports all scala object serialization:

- `case` class serialization supported
- `pojo/bean` class serialization supported
- `object` singleton serialization supported
- `collection` serialization supported
- other types such as `tuple/either` and basic types are all supported too.

Scala 2 and 3 are both supported.

## Install

To add a dependency on Fury scala for scala 2 with sbt, use the following:

```sbt
libraryDependencies += "org.apache.fury" % "fury-scala_2.13" % "0.10.1"
```

To add a dependency on Fury scala for scala 3 with sbt, use the following:

```sbt
libraryDependencies += "org.apache.fury" % "fury-scala_3" % "0.10.1"
```

## Quict Start

```scala
case class Person(name: String, id: Long, github: String)
case class Point(x : Int, y : Int, z : Int)

object ScalaExample {
  val fury: Fury = Fury.builder().withScalaOptimizationEnabled(true).build()
  // Register optimized fury serializers for scala
  ScalaSerializers.registerSerializers(fury)
  fury.register(classOf[Person])
  fury.register(classOf[Point])

  def main(args: Array[String]): Unit = {
    val p = Person("Shawn Yang", 1, "https://github.com/chaokunyang")
    println(fury.deserialize(fury.serialize(p)))
    println(fury.deserialize(fury.serialize(Point(1, 2, 3))))
  }
}
```

## Fury creation

When using fury for scala serialization, you should create fury at least with following options:

```scala
import org.apache.fury.Fury
import org.apache.fury.serializer.scala.ScalaSerializers

val fury = Fury.builder().withScalaOptimizationEnabled(true).build()

// Register optimized fury serializers for scala
ScalaSerializers.registerSerializers(fury)
```

Depending on the object types you serialize, you may need to register some scala internal types:

```scala
fury.register(Class.forName("scala.Enumeration.Val"))
```

If you want to avoid such registration, you can disable class registration by `FuryBuilder#requireClassRegistration(false)`.
Note that this option allow to deserialize objects unknown types, more flexible but may be insecure if the classes contains malicious code.

And circular references are common in scala, `Reference tracking` should be enabled by `FuryBuilder#withRefTracking(true)`. If you don't enable reference tracking, [StackOverflowError](https://github.com/apache/fury/issues/1032) may happen for some scala versions when serializing scala Enumeration.

Note that fury instance should be shared between multiple serialization, the creation of fury instance is not cheap.

If you use shared fury instance across multiple threads, you should create `ThreadSafeFury` instead by `FuryBuilder#buildThreadSafeFury()` instead.

## Serialize case object

```scala
case class Person(github: String, age: Int, id: Long)
val p = Person("https://github.com/chaokunyang", 18, 1)
println(fury.deserialize(fury.serialize(p)))
println(fury.deserializeJavaObject(fury.serializeJavaObject(p)))
```

## Serialize pojo

```scala
class Foo(f1: Int, f2: String) {
  override def toString: String = s"Foo($f1, $f2)"
}
println(fury.deserialize(fury.serialize(Foo(1, "chaokunyang"))))
```

## Serialize object singleton

```scala
object singleton {
}
val o1 = fury.deserialize(fury.serialize(singleton))
val o2 = fury.deserialize(fury.serialize(singleton))
println(o1 == o2)
```

## Serialize collection

```scala
val seq = Seq(1,2)
val list = List("a", "b")
val map = Map("a" -> 1, "b" -> 2)
println(fury.deserialize(fury.serialize(seq)))
println(fury.deserialize(fury.serialize(list)))
println(fury.deserialize(fury.serialize(map)))
```

## Serialize Tuple

```scala
val tuple = Tuple2(100, 10000L)
println(fury.deserialize(fury.serialize(tuple)))
val tuple = Tuple4(100, 10000L, 10000L, "str")
println(fury.deserialize(fury.serialize(tuple)))
```

## Serialize Enum

### Scala3 Enum

```scala
enum Color { case Red, Green, Blue }
println(fury.deserialize(fury.serialize(Color.Green)))
```

### Scala2 Enum

```scala
object ColorEnum extends Enumeration {
  type ColorEnum = Value
  val Red, Green, Blue = Value
}
println(fury.deserialize(fury.serialize(ColorEnum.Green)))
```

## Serialize Option

```scala
val opt: Option[Long] = Some(100)
println(fury.deserialize(fury.serialize(opt)))
val opt1: Option[Long] = None
println(fury.deserialize(fury.serialize(opt1)))
```
