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
import org.apache.fury.reflect.TypeRef;
import org.apache.fury.type.Descriptor;
import org.apache.fury.type.TypeResolutionContext;
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

  private final String className;
  private final SortedMap<String, Descriptor> descriptorsMap;
  private final Schema schema;
  protected static final String BEAN_CLASS_NAME = "beanClass";
  protected Reference beanClassRef = new Reference(BEAN_CLASS_NAME, CLASS_TYPE);
  private final CodegenContext generatedBeanImpl;
  private final String generatedBeanImplName;

  public RowEncoderBuilder(Class<?> beanClass) {
    this(TypeRef.of(beanClass));
  }

  public RowEncoderBuilder(TypeRef<?> beanType) {
    super(new CodegenContext(), beanType);
    Preconditions.checkArgument(
        beanClass.isInterface() || TypeUtils.isBean(beanType.getType(), customTypeHandler));
    className = codecClassName(beanClass);
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
              Class.class, "forName", CLASS_TYPE, false, Literal.ofString(ctx.type(beanClass)));
    }
    ctx.addField(Class.class, "beanClass", clsExpr);
    ctx.addImports(Field.class, Schema.class);
    ctx.addImports(Row.class, ArrayData.class, MapData.class);
    ctx.addImports(BinaryRow.class, BinaryArray.class, BinaryMap.class);
    if (beanClass.isInterface()) {
      generatedBeanImplName = beanClass.getSimpleName() + "GeneratedImpl";
      generatedBeanImpl = buildImplClass();
    } else {
      generatedBeanImplName = null;
      generatedBeanImpl = null;
    }
  }

  @Override
  protected String codecSuffix() {
    return "RowCodec";
  }

  @Override
  public String genCode() {
    ctx.setPackage(CodeGenerator.getPackage(beanClass));
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
    // It would be nice if Expression let us write inner classes
    if (generatedBeanImpl != null) {
      int insertPoint = code.lastIndexOf('}');
      code =
          code.substring(0, insertPoint)
              + generatedBeanImpl.genCode()
              + code.substring(insertPoint);
    }
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
      TypeRef<?> fieldType = d.getTypeRef();
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
            new Expression.Invoke(writer, "getRow", TypeRef.of(BinaryRow.class))));
    return expressions;
  }

  /**
   * Returns an expression that deserialize <code>row</code> as a java bean of type {@link
   * CodecBuilder#beanClass}.
   */
  @Override
  public Expression buildDecodeExpression() {
    Reference row = new Reference(ROOT_ROW_NAME, binaryRowTypeToken, false);

    addDecoderMethods();

    if (generatedBeanImpl != null) {
      return new Expression.Return(
          new Expression.Reference("new " + generatedBeanImplName + "(row)"));
    }
    Expression bean = newBean();

    int numFields = schema.getFields().size();
    Expression.ListExpression expressions = new Expression.ListExpression();
    expressions.add(bean);
    // schema field's name must correspond to descriptor's name.
    for (int i = 0; i < numFields; i++) {
      Literal ordinal = Literal.ofInt(i);
      Descriptor d = getDescriptorByFieldName(schema.getFields().get(i).getName());
      TypeRef<?> fieldType = d.getTypeRef();
      Expression.Invoke isNullAt =
          new Expression.Invoke(row, "isNullAt", TypeUtils.PRIMITIVE_BOOLEAN_TYPE, ordinal);
      Expression value =
          new Expression.Variable(
              "decoded" + i, new Expression.Reference("decode" + i + "(row)", fieldType));
      Expression setActionExpr = setFieldValue(bean, d, value);
      Expression action = new Expression.If(ExpressionUtils.not(isNullAt), setActionExpr);
      expressions.add(action);
    }

    expressions.add(new Expression.Return(bean));
    return expressions;
  }

  private void addDecoderMethods() {
    Reference row = new Reference(ROOT_ROW_NAME, binaryRowTypeToken, false);
    int numFields = schema.getFields().size();
    for (int i = 0; i < numFields; i++) {
      Literal ordinal = Literal.ofInt(i);
      Descriptor d = getDescriptorByFieldName(schema.getFields().get(i).getName());
      TypeRef<?> fieldType = d.getTypeRef();
      Class<?> rawFieldType = fieldType.getRawType();
      TypeResolutionContext fieldCtx;
      if (beanClass.isInterface() && rawFieldType.isInterface()) {
        fieldCtx = typeCtx.withSynthesizedBeanType(rawFieldType);
      } else {
        fieldCtx = typeCtx;
      }
      CustomCodec<?, ?> customEncoder = customTypeHandler.findCodec(beanClass, rawFieldType);
      TypeRef<?> columnAccessType;
      if (customEncoder == null) {
        columnAccessType = fieldType;
      } else {
        columnAccessType = TypeRef.of(customEncoder.encodedType());
      }
      String columnAccessMethodName =
          BinaryUtils.getElemAccessMethodName(columnAccessType, fieldCtx);
      TypeRef<?> colType = BinaryUtils.getElemReturnType(columnAccessType, fieldCtx);
      Expression.Invoke columnValue =
          new Expression.Invoke(
              row,
              columnAccessMethodName,
              ctx.newName(getRawType(colType)),
              colType,
              false,
              ordinal);
      final Expression value =
          new Expression.Return(deserializeFor(columnValue, fieldType, fieldCtx));
      ctx.addMethod(
          "decode" + i,
          value.doGenCode(ctx).code(),
          fieldType.getRawType(),
          BinaryRow.class,
          ROOT_ROW_NAME);
    }
  }

  private CodegenContext buildImplClass() {
    Reference row = new Reference(ROOT_ROW_NAME, binaryRowTypeToken, false);
    CodegenContext implClass = new CodegenContext();
    implClass.setClassModifiers("final");
    implClass.setClassName(generatedBeanImplName);
    implClass.implementsInterfaces(implClass.type(beanClass));
    implClass.addField(true, implClass.type(BinaryRow.class), "row", null);
    implClass.addConstructor("this.row = row;", BinaryRow.class, "row");

    int numFields = schema.getFields().size();
    for (int i = 0; i < numFields; i++) {
      Literal ordinal = Literal.ofInt(i);
      Descriptor d = getDescriptorByFieldName(schema.getFields().get(i).getName());
      TypeRef<?> fieldType = d.getTypeRef();

      Expression.Reference decodeValue =
          new Expression.Reference("decode" + i + "(row)", fieldType);
      Expression getterImpl;
      if (fieldType.isPrimitive()) {
        getterImpl = new Expression.Return(decodeValue);
      } else {
        String fieldName = "f" + i + "_" + d.getName();
        implClass.addField(fieldType.getRawType(), fieldName);

        Expression fieldRef = new Expression.Reference(fieldName, fieldType, true);
        Expression storeValue =
            new Expression.SetField(new Expression.Reference("this"), fieldName, decodeValue);
        Expression loadIfFieldIsNull =
            new Expression.If(new Expression.IsNull(fieldRef), storeValue);
        Expression assigner;

        if (d.isNullable()) {
          Expression isNotNullAt =
              new Expression.Not(
                  new Expression.Invoke(
                      row, "isNullAt", TypeUtils.PRIMITIVE_BOOLEAN_TYPE, ordinal));
          assigner = new Expression.If(isNotNullAt, loadIfFieldIsNull);
        } else {
          assigner = loadIfFieldIsNull;
        }
        getterImpl = new Expression.ListExpression(assigner, new Expression.Return(fieldRef));
      }
      implClass.addMethod(
          d.getName(), getterImpl.genCode(implClass).code(), fieldType.getRawType());
    }

    return implClass;
  }

  private Descriptor getDescriptorByFieldName(String fieldName) {
    String name = StringUtils.lowerUnderscoreToLowerCamelCase(fieldName);
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
