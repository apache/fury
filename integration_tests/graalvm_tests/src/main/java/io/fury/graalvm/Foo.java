package io.fury.graalvm;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class Foo {
  int f1;
  String f2;
  List<String> f3;
  Map<String, Long> f4;

  public Foo(int f1, String f2, List<String> f3, Map<String, Long> f4) {
    this.f1 = f1;
    this.f2 = f2;
    this.f3 = f3;
    this.f4 = f4;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Foo foo = (Foo) o;
    return f1 == foo.f1 && Objects.equals(f2, foo.f2) && Objects.equals(f3, foo.f3)
      && Objects.equals(f4, foo.f4);
  }

  @Override
  public int hashCode() {
    return Objects.hash(f1, f2, f3, f4);
  }

  @Override
  public String toString() {
    return "Foo{" +
      "f1=" + f1 +
      ", f2='" + f2 + '\'' +
      ", f3=" + f3 +
      ", f4=" + f4 +
      '}';
  }
}
