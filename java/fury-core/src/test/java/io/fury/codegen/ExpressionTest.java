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

package io.fury.codegen;

import static io.fury.type.TypeUtils.PRIMITIVE_SHORT_TYPE;

import org.testng.Assert;
import org.testng.annotations.Test;

public class ExpressionTest {

  @Test
  public void testIfExpression() {
    {
      String code =
          new Expression.If(
                  ExpressionUtils.eq(
                      Expression.Literal.ofInt(1),
                      new Expression.Reference("classId", PRIMITIVE_SHORT_TYPE, false)),
                  new Expression.Return(Expression.Literal.True),
                  new Expression.Return(Expression.Literal.False))
              .genCode(new CodegenContext())
              .code();
      String expected =
          "if ((1 == classId)) {\n"
              + "    return true;\n"
              + "} else {\n"
              + "    return false;\n"
              + "}";
      Assert.assertEquals(code, expected);
    }
    {
      String code =
          new Expression.If(
                  ExpressionUtils.eq(
                      Expression.Literal.ofInt(1),
                      new Expression.Reference("classId", PRIMITIVE_SHORT_TYPE, false)),
                  Expression.Literal.True,
                  Expression.Literal.False)
              .genCode(new CodegenContext())
              .code();
      String expected =
          "boolean value;\n"
              + "if ((1 == classId)) {\n"
              + "    value = true;\n"
              + "} else {\n"
              + "    value = false;\n"
              + "}\n";
      Assert.assertEquals(code, expected);
    }
  }
}
