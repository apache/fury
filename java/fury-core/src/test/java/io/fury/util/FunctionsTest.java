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

package io.fury.util;

import io.fury.util.function.Functions;
import io.fury.util.function.SerializableFunction;
import io.fury.util.function.SerializableTriFunction;
import io.fury.util.function.TriFunction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.Function;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FunctionsTest {

  @Test
  public void testIsLambda() {
    Consumer<?> f1 = System.out::print;
    Function<Integer, Long> f2 = x -> x * 2L;
    Assert.assertTrue(Functions.isLambda(f1.getClass()));
    Assert.assertTrue(Functions.isLambda(f2.getClass()));
  }

  @Test
  public void testExtractCapturedVariables() {
    String s = "abc";
    SerializableFunction<String, String> f = x -> s;
    Assert.assertEquals(Functions.extractCapturedVariables(f), Collections.singletonList(s));
    Assert.assertEquals(
        Functions.extractCapturedVariables(f, x -> x.getClass() == Object.class),
        new ArrayList<>());
  }

  @Test
  public void testTriFunction() {
    TriFunction<Integer, Integer, Integer, String> f1 = (a1, a2, a3) -> "" + a1 + a2 + a3;
    Assert.assertEquals(f1.apply(1, 2, 3), "123");
    SerializableTriFunction<Integer, Integer, Integer, String> f2 = f1::apply;
    Assert.assertEquals(f2.apply(1, 2, 3), "123");
    TriFunction<Integer, Integer, Integer, String> f3 = f2.andThen(x -> x + "abc");
    Assert.assertEquals(f3.apply(1, 2, 3), "123abc");
  }
}
