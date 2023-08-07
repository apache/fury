/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.util.function;

import com.google.common.base.Preconditions;
import java.util.function.Function;

/**
 * Triple function.
 *
 * @author chaokunyang
 */
@FunctionalInterface
public interface TriFunction<A, B, C, R> {

  R apply(A a, B b, C c);

  default <V> TriFunction<A, B, C, V> andThen(Function<? super R, ? extends V> after) {
    Preconditions.checkNotNull(after);
    return (A a, B b, C c) -> after.apply(apply(a, b, c));
  }
}
