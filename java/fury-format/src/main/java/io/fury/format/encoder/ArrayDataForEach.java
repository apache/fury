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

package io.fury.format.encoder;

import static io.fury.type.TypeUtils.getRawType;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import io.fury.annotation.Internal;
import io.fury.codegen.ClosureVisitable;
import io.fury.codegen.Code;
import io.fury.codegen.CodeGenerator;
import io.fury.codegen.CodegenContext;
import io.fury.codegen.Expression;
import io.fury.format.row.binary.BinaryArray;
import io.fury.format.row.binary.BinaryUtils;
import io.fury.type.TypeUtils;
import io.fury.util.StringUtils;
import io.fury.util.function.Functions;

/**
 * Expression for iterate {@link io.fury.format.row.ArrayData} with specified not null element
 * action expression and null element action expression.
 *
 * @author chaokunyang
 */
@Internal
public class ArrayDataForEach implements Expression {
  private final Expression inputArrayData;
  private final String accessMethod;
  private final TypeToken<?> elemType;

  @ClosureVisitable
  private final Functions.SerializableBiFunction<Expression, Expression, Expression> notNullAction;

  @ClosureVisitable private final Functions.SerializableFunction<Expression, Expression> nullAction;

  /**
   * inputArrayData.type() must be multi-dimension array or Collection, not allowed to be primitive
   * array
   */
  public ArrayDataForEach(
      Expression inputArrayData,
      TypeToken<?> elemType,
      Functions.SerializableBiFunction<Expression, Expression, Expression> notNullAction) {
    this(inputArrayData, elemType, notNullAction, null);
  }

  /**
   * inputArrayData.type() must be multi-dimension array or Collection, not allowed to be primitive
   * array
   */
  public ArrayDataForEach(
      Expression inputArrayData,
      TypeToken<?> elemType,
      Functions.SerializableBiFunction<Expression, Expression, Expression> notNullAction,
      Functions.SerializableFunction<Expression, Expression> nullAction) {
    Preconditions.checkArgument(getRawType(inputArrayData.type()) == BinaryArray.class);
    this.inputArrayData = inputArrayData;
    this.accessMethod = BinaryUtils.getElemAccessMethodName(elemType);
    this.elemType = BinaryUtils.getElemReturnType(elemType);
    this.notNullAction = notNullAction;
    this.nullAction = nullAction;
  }

  @Override
  public TypeToken<?> type() {
    return TypeUtils.PRIMITIVE_VOID_TYPE;
  }

  @Override
  public Code.ExprCode doGenCode(CodegenContext ctx) {
    StringBuilder codeBuilder = new StringBuilder();
    Code.ExprCode targetExprCode = inputArrayData.genCode(ctx);
    if (StringUtils.isNotBlank(targetExprCode.code())) {
      codeBuilder.append(targetExprCode.code()).append("\n");
    }
    String[] freshNames = ctx.newNames("i", "elemValue", "len");
    String i = freshNames[0];
    String elemValue = freshNames[1];
    String len = freshNames[2];
    // elemValue is only used in notNullAction, so set elemValueRef'nullable to false.
    Reference elemValueRef = new Reference(elemValue, elemType);
    Code.ExprCode notNullElemExprCode =
        notNullAction.apply(new Literal(i), elemValueRef).genCode(ctx);
    if (nullAction == null) {
      String code =
          StringUtils.format(
              ""
                  + "int ${len} = ${arr}.numElements();\n"
                  + "int ${i} = 0;\n"
                  + "while (${i} < ${len}) {\n"
                  + "    if (!${arr}.isNullAt(${i})) {\n"
                  + "        ${elemType} ${elemValue} = ${arr}.${method}(${i});\n"
                  + "        ${notNullElemExprCode}\n"
                  + "    }\n"
                  + "    ${i}++;\n"
                  + "}",
              "arr",
              targetExprCode.value(),
              "len",
              len,
              "i",
              i,
              "elemType",
              ctx.type(elemType),
              "elemValue",
              elemValue,
              "method",
              accessMethod,
              "notNullElemExprCode",
              CodeGenerator.alignIndent(notNullElemExprCode.code(), 8));
      codeBuilder.append(code);
    } else {
      Code.ExprCode nullExprCode = nullAction.apply(new Literal(i)).genCode(ctx);
      String code =
          StringUtils.format(
              ""
                  + "int ${len} = ${arr}.numElements();\n"
                  + "int ${i} = 0;\n"
                  + "while (${i} < ${len}) {\n"
                  + "    if (!${arr}.isNullAt(${i})) {\n"
                  + "        ${elemType} ${elemValue} = ${arr}.${method}(${i});\n"
                  + "        ${notNullElemExprCode}\n"
                  + "    } else {\n"
                  + "        ${nullElemExprCode}\n"
                  + "    }\n"
                  + "    ${i}++;\n"
                  + "}",
              "arr",
              targetExprCode.value(),
              "len",
              len,
              "i",
              i,
              "elemType",
              ctx.type(elemType),
              "elemValue",
              elemValue,
              "method",
              accessMethod,
              "notNullElemExprCode",
              CodeGenerator.alignIndent(notNullElemExprCode.code(), 8),
              "nullElemExprCode",
              CodeGenerator.alignIndent(nullExprCode.code(), 8));
      codeBuilder.append(code);
    }
    return new Code.ExprCode(codeBuilder.toString(), null, null);
  }
}
