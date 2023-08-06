package io.fury.util.function;

@FunctionalInterface
public interface ToCharFunction<T> {
  char applyAsChar(T value);
}
