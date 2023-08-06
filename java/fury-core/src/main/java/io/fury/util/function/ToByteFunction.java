package io.fury.util.function;

@FunctionalInterface
public interface ToByteFunction<T> {
  byte applyAsByte(T value);
}
