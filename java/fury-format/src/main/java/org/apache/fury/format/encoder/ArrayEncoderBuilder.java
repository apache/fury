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

package org.apache.fury.format.encoder;

import static org.apache.fury.type.TypeUtils.CLASS_TYPE;
import static org.apache.fury.type.TypeUtils.getRawType;

import com.google.common.reflect.TypeToken;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.fury.Fury;
import org.apache.fury.codegen.CodeGenerator;
import org.apache.fury.codegen.CodegenContext;
import org.apache.fury.codegen.Expression;
import org.apache.fury.codegen.ExpressionUtils;
import org.apache.fury.format.row.binary.BinaryArray;
import org.apache.fury.format.row.binary.writer.BinaryArrayWriter;
import org.apache.fury.format.type.TypeInference;
import org.apache.fury.logging.Logger;
import org.apache.fury.logging.LoggerFactory;
import org.apache.fury.type.TypeUtils;
import org.apache.fury.util.StringUtils;

/** Expression builder for building jit array encoder class. */
public class ArrayEncoderBuilder extends BaseBinaryEncoderBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(ArrayEncoderBuilder.class);
  private static final String FIELD_NAME = "field";
  private static final String ROOT_ARRAY_NAME = "array";
  private static final String ROOT_ARRAY_WRITER_NAME = "arrayWriter";

  private static final TypeToken<Field> ARROW_FIELD_TYPE = TypeToken.of(Field.class);
  private final TypeToken<?> arrayToken;

  public ArrayEncoderBuilder(Class<?> arrayCls, Class<?> beanClass) {
    this(TypeToken.of(arrayCls), TypeToken.of(beanClass));
  }

  public ArrayEncoderBuilder(TypeToken<?> clsType, TypeToken<?> beanType) {
    super(new CodegenContext(), beanType);
    arrayToken = clsType;
    ctx.reserveName(ROOT_ARRAY_WRITER_NAME);
    ctx.reserveName(ROOT_ARRAY_NAME);

    // add array class field
    Expression.Literal clsExpr = new Expression.Literal(getRawType(arrayToken), CLASS_TYPE);
    ctx.addField(true, Class.class.getName(), "arrayClass", clsExpr);
  }

  @Override
  public String genCode() {
    ctx.setPackage(CodeGenerator.getPackage(beanClass));
    String className = codecClassName(beanClass, TypeInference.inferTypeName(arrayToken));
    ctx.setClassName(className);
    // don't addImport(arrayClass), because user class may name collide.
    // janino don't support generics, so GeneratedCodec has no generics
    ctx.implementsInterfaces(ctx.type(GeneratedArrayEncoder.class));

    String constructorCode =
        StringUtils.format(
            "${field} = (${fieldType})${references}[0];\n"
                + "${arrayWriter} = (${arrayWriterType})${references}[1];\n"
                + "${fury} = (${furyType})${references}[2];\n",
            "references",
            REFERENCES_NAME,
            "field",
            FIELD_NAME,
            "fieldType",
            ctx.type(Field.class),
            "arrayWriter",
            ROOT_ARRAY_WRITER_NAME,
            "arrayWriterType",
            ctx.type(BinaryArrayWriter.class),
            "fury",
            FURY_NAME,
            "furyType",
            ctx.type(Fury.class));
    ctx.addField(ctx.type(Field.class), FIELD_NAME);
    ctx.addField(ctx.type(BinaryArrayWriter.class), ROOT_ARRAY_WRITER_NAME);
    ctx.addField(ctx.type(Fury.class), FURY_NAME);

    Expression encodeExpr = buildEncodeExpression();
    String encodeCode = encodeExpr.genCode(ctx).code();
    ctx.overrideMethod("toArray", encodeCode, BinaryArray.class, Object.class, ROOT_OBJECT_NAME);
    Expression decodeExpr = buildDecodeExpression();
    String decodeCode = decodeExpr.genCode(ctx).code();
    ctx.overrideMethod("fromArray", decodeCode, Object.class, BinaryArray.class, ROOT_ARRAY_NAME);

    ctx.addConstructor(constructorCode, Object[].class, REFERENCES_NAME);

    long startTime = System.nanoTime();
    String code = ctx.genCode();
    long durationMs = (System.nanoTime() - startTime) / 1000_000;
    LOG.info("Generate array codec for class {} take {} us", beanClass, durationMs);
    return code;
  }

  /**
   * Returns an expression that serialize java bean of type {@link ArrayEncoderBuilder#beanClass} as
   * a <code>row</code>.
   */
  @Override
  public Expression buildEncodeExpression() {
    Expression.Reference arrayWriter =
        new Expression.Reference(ROOT_ARRAY_WRITER_NAME, arrayWriterTypeToken, false);
    Expression.ListExpression expressions = new Expression.ListExpression();

    Expression.Reference inputObject =
        new Expression.Reference(ROOT_OBJECT_NAME, TypeUtils.COLLECTION_TYPE, false);
    Expression.Cast array =
        new Expression.Cast(inputObject, arrayToken, ctx.newName(getRawType(arrayToken)));
    expressions.add(array);

    Expression.Reference fieldExpr = new Expression.Reference(FIELD_NAME, ARROW_FIELD_TYPE, false);
    Expression listExpression = serializeForArray(array, arrayWriter, arrayToken, fieldExpr);

    expressions.add(listExpression);

    expressions.add(
        new Expression.Return(
            new Expression.Invoke(arrayWriter, "toArray", TypeToken.of(BinaryArray.class))));
    return expressions;
  }

  /**
   * Returns an expression that deserialize <code>row</code> as a java bean of type {@link
   * ArrayEncoderBuilder#beanClass}.
   */
  public Expression buildDecodeExpression() {
    Expression.ListExpression expressions = new Expression.ListExpression();
    Expression collection = newCollection(arrayToken);
    Expression.Reference arrayRef =
        new Expression.Reference(ROOT_ARRAY_NAME, binaryArrayTypeToken, false);

    Expression value =
        deserializeForCollection(arrayRef, collection, TypeUtils.getElementType(arrayToken));
    expressions.add(value);
    expressions.add(new Expression.Return(collection));
    return expressions;
  }

  private Expression deserializeForCollection(
      Expression arrayData, Expression collection, TypeToken<?> elemType) {
    ArrayDataForEach addElemsOp =
        new ArrayDataForEach(
            arrayData,
            elemType,
            (i, value) -> new Expression.Invoke(collection, "add", deserializeFor(value, elemType)),
            i -> new Expression.Invoke(collection, "add", ExpressionUtils.nullValue(elemType)));
    return new Expression.ListExpression(collection, addElemsOp, collection);
  }
}
