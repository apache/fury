<!-- fury_frontmatter --
title: Scala Serialization Guide
order: 4
-- fury_frontmatter -->

# Scala object graph serialization
Fury supports all scala object serialization by keeping compatible with JDK serialization API: `writeObject/readObject/writeReplace/readResolve/readObjectNoData/Externalizable`:
- `case` class serialization supported
- `object` singleton serialization supported
- `collection` serialization supported
- other types such as `tuple/either` and basic types are all supported too.

## Fury creation
When using fury for scala serialization, you should create fury by disabling class registration:
```scala
val fury = Fury.builder().requireClassRegistration(false).build()
```
Otherwise if you serialize some scala types such as `object/Enumeration`, you will need to register some scala internal types, such as:
```scala
fury.register(classOf[ModuleSerializationProxy])
fury.register(Class.forName("scala.Enumeration.Val"))
```
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
Scala collections and generics doesn't follow java collection framework, and is not fully integrated with Fury JIT, 
the execution will invoke JDK serialization API with fury `ObjectStream` implementation for scala collections, 
the performance won't be as good as fury collections serialization for java. In future we may provide jit support for scala collections to
get better performance, see https://github.com/alipay/fury/issues/682.

`case` object are supported by fury jit well, the performance is as good as fury java.