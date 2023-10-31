<!-- fury_frontmatter --
title: Scala Serialization Guide
order: 4
-- fury_frontmatter -->

# Scala serialization
Fury supports all scala object serialization:
- `case` class serialization supported
- `pojo/bean` class serialization supported
- `object` singleton serialization supported
- `collection` serialization supported
- other types such as `tuple/either` and basic types are all supported too.

## Fury creation
When using fury for scala serialization, you should create fury at least with following options:
```scala
val fury = Fury.builder()
  .withScalaOptimizationEnabled(true)
  .requireClassRegistration(false)
  .withRefTracking(true)
  .build()
```
Otherwise if you serialize some scala types such as `collection/Enumeration`, you will need to register some scala internal types:
```scala
fury.register(Class.forName("scala.collection.generic.DefaultSerializationProxy"))
fury.register(Class.forName("scala.Enumeration.Val"))
```
And circular references are common in scala, `Reference tracking` should be enabled by `FuryBuilder#withRefTracking(true)`. If you don't enable reference tracking, [StackOverflowError](https://github.com/alipay/fury/issues/1032) may happen for some scala versions when serializing scala Enumeration.

Note that fury instance should be shared between multiple serialization, the creation of fury instance is not cheap.

If you use shared fury instance across multiple threads, you should create `ThreadSafeFury` instead by `FuryBuilder#buildThreadSafeFury()` instead.

## Serialize case object
```scala
case class Person(name: String, age: Int, id: Long)
println(fury.deserialize(fury.serialize(Person("chaokunyang", 28, 1))))
println(fury.deserializeJavaObject(fury.serializeJavaObject(Person("chaokunyang", 28, 1))))
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

# Performance
Scala `pojo/bean/case/object` are supported by fury jit well, the performance is as good as fury java.

Scala collections and generics doesn't follow java collection framework, and is not fully integrated with Fury JIT. The performance won't be as good as fury collections serialization for java.

The execution for scala collections will invoke Java serialization API `writeObject/readObject/writeReplace/readResolve/readObjectNoData/Externalizable` with fury `ObjectStream` implementation. Although `io.fury.serializer.ObjectStreamSerializer` is much faster than JDK `ObjectOutputStream/ObjectInputStream`, but it still doesn't know how use scala collection generics.

In future we may provide jit support for scala collections to
get better performance, see https://github.com/alipay/fury/issues/682.
