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

import com.google.common.base.CaseFormat;
import com.google.common.reflect.TypeToken;
import java.lang.reflect.Modifier;
import java.util.SortedMap;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.fury.Fury;
import org.apache.fury.builder.CodecBuilder;
import org.apache.fury.codegen.CodeGenerator;
import org.apache.fury.codegen.CodegenContext;
import org.apache.fury.codegen.Expression;
import org.apache.fury.codegen.Expression.Literal;
import org.apache.fury.codegen.Expression.Reference;
import org.apache.fury.codegen.ExpressionUtils;
import org.apache.fury.format.row.ArrayData;
import org.apache.fury.format.row.MapData;
import org.apache.fury.format.row.Row;
import org.apache.fury.format.row.binary.BinaryArray;
import org.apache.fury.format.row.binary.BinaryMap;
import org.apache.fury.format.row.binary.BinaryRow;
import org.apache.fury.format.row.binary.BinaryUtils;
import org.apache.fury.format.row.binary.writer.BinaryRowWriter;
import org.apache.fury.format.type.DataTypes;
import org.apache.fury.format.type.TypeInference;
import org.apache.fury.logging.Logger;
import org.apache.fury.logging.LoggerFactory;
import org.apache.fury.type.Descriptor;
import org.apache.fury.type.TypeUtils;
import org.apache.fury.util.GraalvmSupport;
import org.apache.fury.util.Preconditions;
import org.apache.fury.util.StringUtils;

/** Expression builder for building jit row encoder class. */
@SuppressWarnings("UnstableApiUsage")
public class RowEncoderBuilder extends BaseBinaryEncoderBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(RowEncoderBuilder.class);
  static final String SCHEMA_NAME = "schema";
  static final String ROOT_ROW_NAME = "row";
  static final String ROOT_ROW_WRITER_NAME = "rowWriter";

  private final SortedMap<String, Descriptor> descriptorsMap;
  private final Schema schema;
  protected static final String BEAN_CLASS_NAME = "beanClass";
  protected Reference beanClassRef = new Reference(BEAN_CLASS_NAME, CLASS_TYPE);

  public RowEncoderBuilder(Class<?> beanClass) {
    this(TypeToken.of(beanClass));
  }

  public RowEncoderBuilder(TypeToken<?> beanType) {
    super(new CodegenContext(), beanType);
    Preconditions.checkArgument(TypeUtils.isBean(beanType));
    this.schema = TypeInference.inferSchema(getRawType(beanType));
    this.descriptorsMap = Descriptor.getDescriptorsMap(beanClass);
    ctx.reserveName(ROOT_ROW_WRITER_NAME);
    ctx.reserveName(SCHEMA_NAME);
    ctx.reserveName(ROOT_ROW_NAME);
    ctx.reserveName(BEAN_CLASS_NAME);
    Expression clsExpr;
    if (Modifier.isPublic(beanClass.getModifiers())) {
      clsExpr = Literal.ofClass(beanClass);
    } else {
      // non-public class is not accessible in other class.
      clsExpr =
          new Expression.StaticInvoke(
              Class.class, "forName", CLASS_TYPE, false, Literal.ofClass(beanClass));
    }
    ctx.addField(Class.class, "beanClass", clsExpr);
    ctx.addImports(Field.class, Schema.class);
    ctx.addImports(Row.class, ArrayData.class, MapData.class);
    ctx.addImports(BinaryRow.class, BinaryArray.class, BinaryMap.class);
  }

  @Override
  protected String codecSuffix() {
    return "RowCodec";
  }

  @Override
  public String genCode() {
    ctx.setPackage(CodeGenerator.getPackage(beanClass));
    String className = codecClassName(beanClass);
    ctx.setClassName(className);
    // don't addImport(beanClass), because user class may name collide.
    // janino don't support generics, so GeneratedCodec has no generics
    ctx.implementsInterfaces(ctx.type(GeneratedRowEncoder.class));
    String constructorCode =
        StringUtils.format(
            "${schema} = (${schemaType})${references}[0];\n"
                + "${rowWriter} = (${rowWriterType})${references}[1];\n"
                + "${fury} = (${furyType})${references}[2];\n",
            "references",
            REFERENCES_NAME,
            "schema",
            SCHEMA_NAME,
            "schemaType",
            ctx.type(Schema.class),
            "rowWriter",
            ROOT_ROW_WRITER_NAME,
            "rowWriterType",
            ctx.type(BinaryRowWriter.class),
            "fury",
            FURY_NAME,
            "furyType",
            ctx.type(Fury.class));
    ctx.addField(ctx.type(Schema.class), SCHEMA_NAME);
    ctx.addField(ctx.type(BinaryRowWriter.class), ROOT_ROW_WRITER_NAME);
    ctx.addField(ctx.type(Fury.class), FURY_NAME);

    Expression encodeExpr = buildEncodeExpression();
    Expression decodeExpr = buildDecodeExpression();
    String encodeCode = encodeExpr.genCode(ctx).code();
    String decodeCode = decodeExpr.genCode(ctx).code();
    ctx.overrideMethod("toRow", encodeCode, BinaryRow.class, Object.class, ROOT_OBJECT_NAME);
    // T fromRow(BinaryRow row);
    ctx.overrideMethod("fromRow", decodeCode, Object.class, BinaryRow.class, ROOT_ROW_NAME);
    ctx.addConstructor(constructorCode, Object[].class, REFERENCES_NAME);

    long startTime = System.nanoTime();
    String code = ctx.genCode();
    long durationMs = (System.nanoTime() - startTime) / 1000;
    LOG.info("Generate codec for class {} take {} us", beanClass, durationMs);
    return code;
  }

  /**
   * Returns an expression that serialize java bean of type {@link CodecBuilder#beanClass} as a
   * <code>row</code>.
   */
  @Override
  public Expression buildEncodeExpression() {
    Reference inputObject = new Reference(ROOT_OBJECT_NAME, TypeUtils.OBJECT_TYPE, false);
    Reference writer = new Reference(ROOT_ROW_WRITER_NAME, rowWriterTypeToken, false);
    Reference schemaExpr = new Reference(SCHEMA_NAME, schemaTypeToken, false);

    int numFields = schema.getFields().size();
    Expression.ListExpression expressions = new Expression.ListExpression();
    Expression.Cast bean = new Expression.Cast(inputObject, beanType, ctx.newName(beanClass));
    // schema field's name must correspond to descriptor's name.
    for (int i = 0; i < numFields; i++) {
      Descriptor d = getDescriptorByFieldName(schema.getFields().get(i).getName());
      Preconditions.checkNotNull(d);
      TypeToken<?> fieldType = d.getTypeToken();
      Expression fieldValue = getFieldValue(bean, d);
      Literal ordinal = Literal.ofInt(i);
      Expression.StaticInvoke field =
          new Expression.StaticInvoke(
              DataTypes.class, "fieldOfSchema", ARROW_FIELD_TYPE, false, schemaExpr, ordinal);
      Expression fieldExpr = serializeFor(ordinal, fieldValue, writer, fieldType, field);
      expressions.add(fieldExpr);
    }
    expressions.add(
        new Expression.Return(
            new Expression.Invoke(writer, "getRow", TypeToken.of(BinaryRow.class))));
    return expressions;
  }

  /**
   * Returns an expression that deserialize <code>row</code> as a java bean of type {@link
   * CodecBuilder#beanClass}.
   */
  public Expression buildDecodeExpression() {
    Reference row = new Reference(ROOT_ROW_NAME, binaryRowTypeToken, false);
    Expression bean = newBean();

    int numFields = schema.getFields().size();
    Expression.ListExpression expressions = new Expression.ListExpression();
    expressions.add(bean);
    // schema field's name must correspond to descriptor's name.
    for (int i = 0; i < numFields; i++) {
      Literal ordinal = Literal.ofInt(i);
      Descriptor d = getDescriptorByFieldName(schema.getFields().get(i).getName());
      TypeToken<?> fieldType = d.getTypeToken();
      Expression.Invoke isNullAt =
          new Expression.Invoke(row, "isNullAt", TypeUtils.PRIMITIVE_BOOLEAN_TYPE, ordinal);
      String columnAccessMethodName = BinaryUtils.getElemAccessMethodName(fieldType);
      TypeToken<?> colType = BinaryUtils.getElemReturnType(fieldType);
      Expression.Invoke columnValue =
          new Expression.Invoke(
              row,
              columnAccessMethodName,
              ctx.newName(getRawType(colType)),
              colType,
              false,
              ordinal);
      Expression value = deserializeFor(columnValue, fieldType);
      Expression setActionExpr = setFieldValue(bean, d, value);
      Expression action = new Expression.If(ExpressionUtils.not(isNullAt), setActionExpr);
      expressions.add(action);
    }

    expressions.add(new Expression.Return(bean));
    return expressions;
  }

  private Descriptor getDescriptorByFieldName(String fieldName) {
    String name = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, fieldName);
    return descriptorsMap.get(name);
  }

  @Override
  protected Expression beanClassExpr() {
    if (GraalvmSupport.isGraalBuildtime()) {
      return staticBeanClassExpr();
    }
    return beanClassRef;
  }
}
