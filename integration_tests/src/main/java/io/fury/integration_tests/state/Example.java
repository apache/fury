package io.fury.integration_tests.state;

import java.util.List;
import java.util.Map;

public class Example {
  public static class Foo {
    String f1;
    Map<String, Integer> f2;
  }

  public static class Bar {
    Foo f1;
    String f2;
    List<Foo> f3;
    Map<Integer, Foo> f4;
    Integer f5;
    Long f6;
    Float f7;
    Double f8;
    short[] f9;
    List<Long> f10;
  }
}
