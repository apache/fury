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

package org.apache.fury.builder;

import static org.apache.fury.builder.Generated.GeneratedMetaSharedSerializer.SERIALIZER_FIELD_NAME;

import com.google.common.reflect.TypeToken;
import java.util.Collection;
import java.util.SortedMap;
import org.apache.fury.Fury;
import org.apache.fury.builder.Generated.GeneratedMetaSharedSerializer;
import org.apache.fury.codegen.CodeGenerator;
import org.apache.fury.codegen.Expression;
import org.apache.fury.codegen.Expression.Literal;
import org.apache.fury.config.CompatibleMode;
import org.apache.fury.config.FuryBuilder;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.meta.ClassDef;
import org.apache.fury.serializer.CodegenSerializer;
import org.apache.fury.serializer.MetaSharedSerializer;
import org.apache.fury.serializer.ObjectSerializer;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.serializer.Serializers;
import org.apache.fury.type.Descriptor;
import org.apache.fury.type.DescriptorGrouper;
import org.apache.fury.util.Preconditions;
import org.apache.fury.util.StringUtils;
import org.apache.fury.util.Utils;
import org.apache.fury.util.record.RecordComponent;
import org.apache.fury.util.record.RecordUtils;

/**
 * A meta-shared compatible deserializer builder based on {@link ClassDef}. This builder will
 * compare fields between {@link ClassDef} and class fields, then create serializer to read and
 * set/skip corresponding fields to support type forward/backward compatibility. Serializer are
 * forward to {@link ObjectCodecBuilder} for now. We can consolidate fields between peers to create
 * better serializers to serialize common fields between peers for efficiency.
 *
 * <p>With meta context share enabled and compatible mode, the {@link ObjectCodecBuilder} will take
 * all non-inner final types as non-final, so that fury can write class definition when write class
 * info for those types.
 *
 * @see CompatibleMode
 * @see FuryBuilder#withMetaContextShare
 * @see GeneratedMetaSharedSerializer
 * @see MetaSharedSerializer
 */
public class MetaSharedCodecBuilder extends ObjectCodecBuilder {
  private final ClassDef classDef;

  public MetaSharedCodecBuilder(TypeToken<?> beanType, Fury fury, ClassDef classDef) {
    super(beanType, fury, GeneratedMetaSharedSerializer.class);
    Preconditions.checkArgument(
        !fury.getConfig().checkClassVersion(),
        "Class version check should be disabled when compatible mode is enabled.");
    this.classDef = classDef;
    Collection<Descriptor> descriptors =
        visitFury(
            f -> MetaSharedSerializer.consolidateFields(f.getClassResolver(), beanClass, classDef));
    DescriptorGrouper grouper =
        DescriptorGrouper.createDescriptorGrouper(
            descriptors, true, fury.compressInt(), fury.compressLong());
    objectCodecOptimizer =
        new ObjectCodecOptimizer(beanClass, grouper, !fury.isBasicTypesRefIgnored(), ctx);
  }

  @Override
  protected String codecSuffix() {
    // For every class def sent from different peer, if the class def are different, then
    // a new serializer needs being generated.
    return "MetaShared" + classDef.getId();
  }

  @Override
  public String genCode() {
    ctx.setPackage(CodeGenerator.getPackage(beanClass));
    String className = codecClassName(beanClass);
    ctx.setClassName(className);
    // don't addImport(beanClass), because user class may name collide.
    ctx.extendsClasses(ctx.type(parentSerializerClass));
    ctx.reserveName(POJO_CLASS_TYPE_NAME);
    ctx.reserveName(SERIALIZER_FIELD_NAME);
    ctx.addField(ctx.type(Fury.class), FURY_NAME);
    String constructorCode =
        StringUtils.format(
            ""
                + "super(${fury}, ${cls});\n"
                + "this.${fury} = ${fury};\n"
                + "${serializer} = ${builderClass}.setCodegenSerializer(${fury}, ${cls}, this);\n",
            "fury",
            FURY_NAME,
            "cls",
            POJO_CLASS_TYPE_NAME,
            "builderClass",
            MetaSharedCodecBuilder.class.getName(),
            "serializer",
            SERIALIZER_FIELD_NAME);
    ctx.clearExprState();
    Expression decodeExpr = buildDecodeExpression();
    String decodeCode = decodeExpr.genCode(ctx).code();
    decodeCode = ctx.optimizeMethodCode(decodeCode);
    ctx.overrideMethod("read", decodeCode, Object.class, MemoryBuffer.class, BUFFER_NAME);
    registerJITNotifyCallback();
    ctx.addConstructor(constructorCode, Fury.class, "fury", Class.class, POJO_CLASS_TYPE_NAME);
    return ctx.genCode();
  }

  @Override
  protected void addCommonImports() {
    super.addCommonImports();
    ctx.addImport(GeneratedMetaSharedSerializer.class);
  }

  // Invoked by JIT.
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static Serializer setCodegenSerializer(
      Fury fury, Class<?> cls, GeneratedMetaSharedSerializer s) {
    // This method hold jit lock, so create jit serializer async to avoid block serialization.
    Class serializerClass =
        fury.getJITContext()
            .registerSerializerJITCallback(
                () -> ObjectSerializer.class,
                () -> CodegenSerializer.loadCodegenSerializer(fury, s.getType()),
                c -> s.serializer = Serializers.newSerializer(fury, s.getType(), c));
    return Serializers.newSerializer(fury, cls, serializerClass);
  }

  @Override
  public Expression buildEncodeExpression() {
    throw new IllegalStateException("unreachable");
  }

  @Override
  protected Expression buildComponentsArray() {
    return buildDefaultComponentsArray();
  }

  protected Expression createRecord(SortedMap<Integer, Expression> recordComponents) {
    RecordComponent[] components = RecordUtils.getRecordComponents(beanClass);
    Object[] defaultValues = RecordUtils.buildRecordComponentDefaultValues(beanClass);
    for (int i = 0; i < defaultValues.length; i++) {
      if (!recordComponents.containsKey(i)) {
        Object defaultValue = defaultValues[i];
        assert components != null;
        RecordComponent component = components[i];
        recordComponents.put(i, new Literal(defaultValue, TypeToken.of(component.getType())));
      }
    }
    Expression[] params = recordComponents.values().toArray(new Expression[0]);
    return new Expression.NewInstance(beanType, params);
  }

  @Override
  protected Expression setFieldValue(Expression bean, Descriptor descriptor, Expression value) {
    if (descriptor.getField() == null) {
      // Field doesn't exist in current class, skip set this field value.
      // Note that the field value shouldn't be an inlined value, otherwise field value read may
      // be ignored.
      // Add an ignored call here to make expression type to void.
      return new Expression.StaticInvoke(Utils.class, "ignore", value);
    }
    return super.setFieldValue(bean, descriptor, value);
  }
}
