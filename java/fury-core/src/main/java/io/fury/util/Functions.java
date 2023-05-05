/*
 * Copyright 2023 The Fury authors
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.util;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Utility for lambda functions.
 *
 * @author chaokunyang
 */
public class Functions {
  /** Returns true if the specified class is a lambda. */
  public static boolean isLambda(Class<?> clz) {
    Preconditions.checkNotNull(clz);
    return clz.getName().indexOf('/') >= 0;
  }

  public static List<Object> extractCapturedVariables(Serializable closure) {
    return extractCapturedVariables(closure, o -> true);
  }

  public static List<Object> extractCapturedVariables(
      Serializable closure, Predicate<Object> predicate) {
    Preconditions.checkArgument(Functions.isLambda(closure.getClass()));
    Method writeReplace = ReflectionUtils.findMethods(closure.getClass(), "writeReplace").get(0);
    writeReplace.setAccessible(true);
    SerializedLambda serializedLambda;
    try {
      serializedLambda = (SerializedLambda) writeReplace.invoke(closure);
    } catch (Exception e) {
      throw new IllegalStateException();
    }
    List<Object> variables = new ArrayList<>();
    for (int i = 0; i < serializedLambda.getCapturedArgCount(); i++) {
      Object capturedArg = serializedLambda.getCapturedArg(i);
      if (predicate.test(capturedArg)) {
        variables.add(capturedArg);
      }
    }
    return variables;
  }

  public interface SerializableBiFunction<T, U, R> extends BiFunction<T, U, R>, Serializable {}

  public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {}

  public interface SerializableSupplier<T> extends Supplier<T>, Serializable {}

  public interface SerializableTriFunction<A, B, C, R>
      extends TriFunction<A, B, C, R>, Serializable {}

  @FunctionalInterface
  public interface TriFunction<A, B, C, R> {

    R apply(A a, B b, C c);

    default <V> TriFunction<A, B, C, V> andThen(Function<? super R, ? extends V> after) {
      Preconditions.checkNotNull(after);
      return (A a, B b, C c) -> after.apply(apply(a, b, c));
    }
  }
}
