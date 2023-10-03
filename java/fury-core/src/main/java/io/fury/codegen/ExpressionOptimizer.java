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

package io.fury.codegen;

import static io.fury.type.TypeUtils.PRIMITIVE_VOID_TYPE;
import static io.fury.type.TypeUtils.getRawType;

import com.google.common.base.Preconditions;
import io.fury.codegen.Expression.Reference;
import io.fury.util.function.SerializableSupplier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Optimizer to generate expression logic in a new method and return an {@link
 * io.fury.codegen.Expression} to invoke the new generated method.
 *
 * @author chaokunyang
 */
public class ExpressionOptimizer {

  public static Expression invokeGenerated(
      CodegenContext ctx,
      SerializableSupplier<Expression> groupExpressionsGenerator,
      String methodPrefix) {
    List<Expression> cutPoint =
        ExpressionUtils.extractCapturedExpressions(groupExpressionsGenerator);
    return invokeGenerated(
        ctx, new HashSet<>(cutPoint), groupExpressionsGenerator.get(), methodPrefix, false);
  }

  public static Expression invokeGenerated(
      CodegenContext ctx,
      SerializableSupplier<Expression> groupExpressionsGenerator,
      String methodPrefix,
      boolean inlineInvoke) {
    List<Expression> cutPoint =
        ExpressionUtils.extractCapturedExpressions(groupExpressionsGenerator);
    return invokeGenerated(
        ctx, new HashSet<>(cutPoint), groupExpressionsGenerator.get(), methodPrefix, inlineInvoke);
  }

  public static Expression invokeGenerated(
      CodegenContext ctx,
      Set<Expression> cutPoint,
      Expression groupExpressions,
      String methodPrefix,
      boolean inlineInvoke) {
    return invokeGenerated(
        ctx,
        new LinkedHashSet<>(cutPoint),
        groupExpressions,
        "private",
        methodPrefix,
        inlineInvoke);
  }

  /**
   * The caller should ensure `cutPoint` expressions are included in the parent expressions,
   * otherwise some expressions may be lost.
   */
  public static Expression invokeGenerated(
      CodegenContext ctx,
      LinkedHashSet<Expression> cutPoint,
      Expression groupExpressions,
      String modifier,
      String methodPrefix,
      boolean inlineInvoke) {
    LinkedHashMap<Expression, Reference> cutExprMap = new LinkedHashMap<>();
    for (Expression expression : cutPoint) {
      if (expression == null) {
        continue;
      }
      Preconditions.checkArgument(
          expression.type() != PRIMITIVE_VOID_TYPE, "Cut on block is not supported currently.");
      String param = ctx.newName(getRawType(expression.type()));
      cutExprMap.put(expression, new Reference(param, expression.type()));
    }
    // iterate groupExpressions dag to update cutoff point to `Reference`.
    new ExpressionVisitor()
        .traverseExpression(
            groupExpressions,
            exprSite -> {
              if (cutPoint.contains((exprSite.current))) {
                Reference newExpr = cutExprMap.get(exprSite.current);
                if (exprSite.current != newExpr) {
                  exprSite.update(newExpr);
                }
                return false;
              } else {
                return true;
              }
            });
    // copy variable names so that to avoid new variable name conflict with generated class
    // instance field name.
    CodegenContext codegenContext = new CodegenContext(ctx.getValNames(), ctx.getImports());
    for (Reference reference : cutExprMap.values()) {
      Preconditions.checkArgument(codegenContext.containName(reference.name()));
    }
    String methodName = ctx.newName(methodPrefix);
    String code = groupExpressions.genCode(codegenContext).code();
    code = codegenContext.optimizeMethodCode(code);
    ArrayList<Object> formalParams = new ArrayList<>();
    ArrayList<Expression> actualParams = new ArrayList<>();
    for (Map.Entry<Expression, Reference> entry : cutExprMap.entrySet()) {
      Expression expr = entry.getKey();
      Reference ref = entry.getValue();
      formalParams.add(getRawType(ref.type()));
      formalParams.add(ref.name());
      actualParams.add(expr);
    }
    ctx.addMethod(
        modifier, methodName, code, getRawType(groupExpressions.type()), formalParams.toArray());
    if (inlineInvoke) {
      return Expression.Invoke.inlineInvoke(
          new Reference("this"),
          methodName,
          groupExpressions.type(),
          false,
          actualParams.toArray(new Expression[0]));
    } else {
      return new Expression.Invoke(
          new Reference("this"),
          methodName,
          "",
          groupExpressions.type(),
          false,
          false,
          actualParams.toArray(new Expression[0]));
    }
  }
}
