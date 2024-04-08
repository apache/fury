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

package org.apache.fury.codegen;

import static org.apache.fury.codegen.Expression.Arithmetic;
import static org.apache.fury.codegen.Expression.Comparator;
import static org.apache.fury.codegen.Expression.IsNull;
import static org.apache.fury.codegen.Expression.Literal;
import static org.apache.fury.codegen.Expression.NewArray;
import static org.apache.fury.codegen.Expression.Not;
import static org.apache.fury.codegen.Expression.StaticInvoke;
import static org.apache.fury.type.TypeUtils.getRawType;

import com.google.common.reflect.TypeToken;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.fury.codegen.Expression.Cast;
import org.apache.fury.codegen.Expression.Null;
import org.apache.fury.util.Preconditions;
import org.apache.fury.util.StringUtils;
import org.apache.fury.util.function.Functions;

/** Expression utils to create expression and code in a more convenient way. */
@SuppressWarnings("UnstableApiUsage")
public class ExpressionUtils {

  public static Expression newObjectArray(Expression... expressions) {
    return new NewArray(TypeToken.of(Object[].class), expressions);
  }

  public static Expression valueOf(TypeToken<?> type, Expression value) {
    return new StaticInvoke(getRawType(type), "valueOf", type, false, value);
  }

  public static IsNull isNull(Expression target) {
    return new IsNull(target);
  }

  public static Expression notNull(Expression target) {
    return new Not(new IsNull(target));
  }

  public static Expression eqNull(Expression target) {
    Preconditions.checkArgument(!target.type().isPrimitive());
    return eq(target, new Null(target.type()));
  }

  public static Not not(Expression target) {
    return new Not(target);
  }

  public static Literal nullValue(TypeToken<?> type) {
    return new Literal(null, type);
  }

  public static Comparator eq(Expression left, Expression right) {
    return eq(left, right, true);
  }

  public static Comparator eq(Expression left, Expression right, boolean inline) {
    return new Comparator("==", left, right, inline);
  }

  public static Comparator eq(Expression left, Expression right, String valuePrefix) {
    Comparator comparator = new Comparator("==", left, right, false);
    comparator.valuePrefix = valuePrefix;
    return comparator;
  }

  public static Comparator neq(Expression left, Expression right) {
    return neq(left, right, true);
  }

  public static Comparator neq(Expression left, Expression right, String valuePrefix) {
    Comparator comparator = new Comparator("!=", left, right, false);
    comparator.valuePrefix = valuePrefix;
    return comparator;
  }

  public static Comparator neq(Expression left, Expression right, boolean inline) {
    return new Comparator("!=", left, right, inline);
  }

  public static Comparator egt(Expression left, Expression right) {
    return new Comparator(">=", left, right, true);
  }

  public static Comparator egt(Expression left, Expression right, String valuePrefix) {
    Comparator comparator = new Comparator(">=", left, right, false);
    comparator.valuePrefix = valuePrefix;
    return comparator;
  }

  public static Comparator gt(Expression left, Expression right) {
    Comparator comparator = new Comparator(">", left, right, true);
    return comparator;
  }

  public static Comparator lessThan(Expression left, Expression right) {
    return new Comparator("<", left, right, true);
  }

  public static Arithmetic add(Expression left, Expression right) {
    return new Arithmetic(true, "+", left, right);
  }

  public static Arithmetic add(Expression left, Expression right, String valuePrefix) {
    Arithmetic arithmetic = new Arithmetic(true, "+", left, right);
    arithmetic.valuePrefix = valuePrefix;
    return arithmetic;
  }

  public static Arithmetic subtract(Expression left, Expression right) {
    return new Arithmetic(true, "-", left, right);
  }

  public static Arithmetic subtract(Expression left, Expression right, String valuePrefix) {
    Arithmetic arithmetic = new Arithmetic(true, "-", left, right);
    arithmetic.valuePrefix = valuePrefix;
    return arithmetic;
  }

  public static Cast cast(Expression value, TypeToken<?> typeToken) {
    return new Cast(value, typeToken);
  }

  public static Expression inline(Expression expression) {
    return inline(expression, true);
  }

  private static Expression inline(Expression expression, boolean inline) {
    if (expression instanceof Expression.Inlineable) {
      ((Expression.Inlineable) (expression)).inline(inline);
    }
    return expression;
  }

  public static Expression uninline(Expression expression) {
    return inline(expression, false);
  }

  public static StaticInvoke invokeStaticInline(
      Class<?> staticObject, String functionName, TypeToken<?> type, Expression... arguments) {
    return new StaticInvoke(staticObject, functionName, "", type, false, true, arguments);
  }

  static String callFunc(
      String type,
      String resultVal,
      String target,
      String functionName,
      String args,
      boolean needTryCatch) {
    if (needTryCatch) {
      return StringUtils.format(
          "${type} ${value};\n"
              + "try {\n"
              + "   ${value} = ${target}.${functionName}(${args});\n"
              + "} catch (Exception e) {\n"
              + "   throw new RuntimeException(e);\n"
              + "}",
          "type",
          type,
          "value",
          resultVal,
          "target",
          target,
          "functionName",
          functionName,
          "args",
          args);
    } else {
      return StringUtils.format(
          "${type} ${value} = ${target}.${functionName}(${args});",
          "type",
          type,
          "value",
          resultVal,
          "target",
          target,
          "functionName",
          functionName,
          "args",
          args);
    }
  }

  static String callFunc(String target, String functionName, String args, boolean needTryCatch) {
    if (needTryCatch) {
      return StringUtils.format(
          "try {\n"
              + "   ${target}.${functionName}(${args});\n"
              + "} catch (Exception e) {\n"
              + "   throw new RuntimeException(e);\n"
              + "}",
          "target",
          target,
          "functionName",
          functionName,
          "args",
          args);
    } else {
      return StringUtils.format(
          "${target}.${functionName}(${args});",
          "target",
          target,
          "functionName",
          functionName,
          "args",
          args);
    }
  }

  public static List<Expression> extractCapturedExpressions(Serializable closure) {
    List<Expression> expressions = new ArrayList<>();
    Functions.extractCapturedVariables(
        closure,
        capturedArg -> {
          if (capturedArg instanceof Expression) {
            // FIXME may need to check list/container values types?
            expressions.add((Expression) capturedArg);
          } else if (capturedArg instanceof Expression[]) {
            Collections.addAll(Arrays.asList((Expression[]) capturedArg));
          }
          return false;
        });
    return expressions;
  }
}
