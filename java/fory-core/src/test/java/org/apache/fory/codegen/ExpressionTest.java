/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.fory.codegen;

import static org.apache.fory.codegen.ExpressionUtils.neq;
import static org.apache.fory.codegen.ExpressionUtils.or;
import static org.apache.fory.type.TypeUtils.PRIMITIVE_SHORT_TYPE;
import static org.testng.Assert.assertNull;

import org.apache.fory.codegen.Code.ExprCode;
import org.apache.fory.codegen.Expression.ListExpression;
import org.apache.fory.codegen.Expression.Literal;
import org.apache.fory.codegen.Expression.LogicalOr;
import org.apache.fory.codegen.Expression.Reference;
import org.apache.fory.codegen.Expression.Return;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ExpressionTest {

  @Test
  public void testIfExpression() {
    {
      String code =
          new Expression.If(
                  ExpressionUtils.eq(
                      Literal.ofInt(1), new Reference("classId", PRIMITIVE_SHORT_TYPE, false)),
                  new Return(Literal.True),
                  new Return(Literal.False))
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
                      Literal.ofInt(1), new Reference("classId", PRIMITIVE_SHORT_TYPE, false)),
                  Literal.True,
                  Literal.False)
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

  @Test
  public void testListExpression() {
    {
      ListExpression exp = new ListExpression();
      String code = exp.genCode(new CodegenContext()).code();
      assertNull(code);
    }
  }

  @Test
  public void testMultipleOr() {
    CodegenContext ctx = new CodegenContext();
    LogicalOr or =
        or(
            Literal.ofBoolean(false),
            neq(Literal.ofInt(3), Literal.ofInt(4)),
            neq(Literal.ofInt(5), Literal.ofInt(6)));
    ExprCode exprCode = or.genCode(ctx);
    Assert.assertEquals(exprCode.value().code(), "((false || (3 != 4)) || (5 != 6))");
  }
}
