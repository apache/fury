package io.fury.graalvm;

import io.fury.Fury;
import io.fury.memory.MemoryBuffer;

import java.io.FilePermission;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Hello {
  static Fury fury;

  static {
    fury = Fury.builder().requireClassRegistration(true).build();
    fury.register(Foo.class);
    // Generate serializer code.
    fury.getClassResolver().getSerializer(Foo.class);
  }

  public static void main(String[] args) {
    System.out.println("main================");
    Foo foo = new Foo(10, "abc", List.of("str1", "str2"), Map.of("k1", 10L, "k2", 20L));
    byte[] bytes = fury.serialize(foo);
    System.out.println(fury.getClassResolver().getSerializer(Foo.class));
    Object o = fury.deserialize(bytes);
    System.out.println(foo);
    System.out.println(o);
    System.out.println(foo.equals(o));
  }
}
