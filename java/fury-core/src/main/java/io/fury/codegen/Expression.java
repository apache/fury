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

import static io.fury.codegen.Code.ExprCode;

import com.google.common.reflect.TypeToken;

/**
 * An expression represents a piece of code evaluation logic which can be generated to valid java
 * code. Expression can be used to compose complex code logic.
 *
 * <p>TODO refactor expression into Expression and Stmt with a common class Node. Expression will
 * have a value, the stmt won't have value. If/While/For/doWhile/ForEach are statements instead of
 * expression.
 *
 * @author chaokunyang
 */
@SuppressWarnings("UnstableApiUsage")
public interface Expression {

  /**
   * Returns the Class<?> of the result of evaluating this expression. It is invalid to query the
   * type of unresolved expression (i.e., when `resolved` == false).
   */
  TypeToken<?> type();

  /**
   * If expression is already generated in this context, returned exprCode won't contains code, so
   * we can reuse/elimination expression code.
   */
  default ExprCode genCode(CodegenContext ctx) {
    // Ctx already contains expression code, which means that the code to evaluate it has already
    // been added before. In that case, we just reuse it.
    ExprCode reuseExprCode = ctx.exprState.get(this);
    if (reuseExprCode != null) {
      return reuseExprCode;
    } else {
      ExprCode genCode = doGenCode(ctx);
      ctx.exprState.put(this, new ExprCode(genCode.isNull(), genCode.value()));
      return genCode;
    }
  }

  /**
   * Used when Expression is requested to doGenCode.
   *
   * @param ctx a [[CodegenContext]]
   * @return an [[ExprCode]] containing the Java source code to generate the given expression
   */
  ExprCode doGenCode(CodegenContext ctx);

  default boolean nullable() {
    return false;
  }

  // ###########################################################
  // ####################### Expressions #######################
  // ###########################################################
}
