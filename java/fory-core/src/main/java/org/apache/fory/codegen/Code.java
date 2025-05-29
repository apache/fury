/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fory.codegen;

import static org.apache.fory.codegen.Code.LiteralValue.FalseLiteral;
import static org.apache.fory.codegen.Code.LiteralValue.TrueLiteral;

import java.util.Objects;

// Derived from
// https://github.com/apache/spark/blob/ea3061beedf7dc10f14e8de27d540dbcc5894fe7/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/codegen/javaCode.scala

/** Class for represented generated code in codegen. */
public interface Code {
  /**
   * The code for a sequence of statements to evaluate the expression in a scope. If no code needs
   * to be evaluated, or expression is already evaluated in a scope ( see {@link
   * Expression#genCode(CodegenContext)}), thus `isNull` and `value` are already existed, the code
   * should be null.
   */
  class ExprCode {
    private final String code;
    private final ExprValue isNull;
    private final ExprValue value;

    public ExprCode(String code) {
      this(code, null, null);
    }

    public ExprCode(ExprValue isNull, ExprValue value) {
      this(null, isNull, value);
    }

    /**
     * Create an `ExprCode`.
     *
     * @param code The sequence of statements required to evaluate the expression. It should be
     *     null, if `isNull` and `value` are already existed, or no code needed to evaluate them
     *     (literals).
     * @param isNull A term that holds a boolean value representing whether the expression evaluated
     *     to null.
     * @param value A term for a (possibly primitive) value of the result of the evaluation. Not
     *     valid if `isNull` is set to `true`.
     */
    public ExprCode(String code, ExprValue isNull, ExprValue value) {
      this.code = code;
      this.isNull = isNull;
      this.value = value;
    }

    public String code() {
      return code;
    }

    public ExprValue isNull() {
      return isNull;
    }

    public ExprValue value() {
      return value;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("ExprCode(");
      if (code != null) {
        sb.append("code=\"").append('\n').append(code).append("\n\", ");
      }
      sb.append("isNull=").append(isNull);
      sb.append(", value=").append(value);
      sb.append(')');
      return sb.toString();
    }
  }

  /** Fragments of java code. */
  abstract class JavaCode {

    abstract String code();

    @Override
    public String toString() {
      return code();
    }
  }

  /** A typed java fragment that must be a valid java expression. */
  abstract class ExprValue extends JavaCode {

    private final Class<?> javaType;

    public ExprValue(Class<?> javaType) {
      this.javaType = javaType;
    }

    Class<?> javaType() {
      return javaType;
    }

    boolean isPrimitive() {
      return javaType.isPrimitive();
    }
  }

  /** A java expression fragment. */
  class SimpleExprValue extends ExprValue {

    private final String expr;

    public SimpleExprValue(Class<?> javaType, String expr) {
      super(javaType);
      this.expr = expr;
    }

    @Override
    String code() {
      return String.format("(%s)", expr);
    }
  }

  /** A local variable java expression. */
  class VariableValue extends ExprValue {
    private final String variableName;

    public VariableValue(Class<?> javaType, String variableName) {
      super(javaType);
      this.variableName = variableName;
    }

    @Override
    String code() {
      return variableName;
    }
  }

  /** A literal java expression. */
  class LiteralValue extends ExprValue {
    public static LiteralValue TrueLiteral = new LiteralValue(boolean.class, "true");
    public static LiteralValue FalseLiteral = new LiteralValue(boolean.class, "false");

    private final String value;

    public LiteralValue(Object value) {
      super(value.getClass());
      this.value = value.toString();
    }

    public LiteralValue(Class<?> javaType, String value) {
      super(javaType);
      this.value = value;
    }

    @Override
    String code() {
      return value;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      LiteralValue that = (LiteralValue) o;
      return this.javaType() == that.javaType() && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(value, javaType());
    }
  }

  // ########################## utils ##########################
  static ExprValue exprValue(Class<?> type, String code) {
    return new SimpleExprValue(type, code);
  }

  static ExprValue variable(Class<?> type, String name) {
    return new VariableValue(type, name);
  }

  static ExprValue isNullVariable(String name) {
    return new VariableValue(boolean.class, name);
  }

  static ExprValue literal(Class<?> type, String value) {
    if (type == Boolean.class || type == boolean.class) {
      if ("true".equals(value)) {
        return TrueLiteral;
      } else if ("false".equals(value)) {
        return FalseLiteral;
      } else {
        throw new IllegalArgumentException(value);
      }
    } else {
      return new LiteralValue(type, value);
    }
  }
}
