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

package org.apache.fory.format.type;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.function.BiConsumer;
import org.apache.fory.annotation.Internal;
import org.apache.fory.codegen.CodegenContext;
import org.apache.fory.codegen.CompileUnit;
import org.apache.fory.codegen.Expression;
import org.apache.fory.codegen.Expression.Reference;
import org.apache.fory.codegen.JaninoUtils;
import org.apache.fory.format.encoder.CustomCodec;
import org.apache.fory.format.encoder.CustomCollectionFactory;
import org.apache.fory.reflect.TypeRef;
import org.apache.fory.util.unsafe.DefineClass;

/**
 * Keep a registry of custom codecs and collection factories. In order to deliver peak performance,
 * the configuration is generated into a class with static methods to avoid the overhead of virtual
 * dispatch. The method signatures match the requested bean / field or collection / element types.
 * Then, we dispatch to the user code via a {@code static final} field. This should allow the JIT to
 * easily inline the calls.
 */
@Internal
public class CustomTypeEncoderRegistry {
  static final Map<CustomTypeRegistration, CustomCodec<?, ?>> REGISTRY = new HashMap<>();
  static final Map<CustomTypeRegistration, CustomCollectionFactory<?, ?>> COLLECTION_REGISTRY =
      new HashMap<>();
  private static int generation = 0;
  private static CustomTypeHandler generatedHandler = CustomTypeHandler.EMPTY;

  static synchronized <T> void registerCustomCodec(
      final CustomTypeRegistration registration, final CustomCodec<T, ?> encoder) {
    REGISTRY.put(registration, encoder);
    generatedHandler = null;
  }

  static synchronized void registerCustomCollection(
      final Class<?> iterableType,
      final Class<?> elementType,
      final CustomCollectionFactory<?, ?> decoder) {
    COLLECTION_REGISTRY.put(new CustomTypeRegistration(iterableType, elementType), decoder);
    generatedHandler = null;
  }

  public static synchronized CustomTypeHandler customTypeHandler() {
    if (generatedHandler != null) {
      return generatedHandler;
    }
    generation++;
    genCode();
    return generatedHandler;
  }

  private static void genCode() {
    final String pkg = CustomTypeEncoderRegistry.class.getPackage().getName();
    final String className = "CustomTypeEncoderRegistry$Gen" + generation;
    final CodegenContext ctx = new CodegenContext(pkg, new HashSet<>(), new LinkedHashSet<>());
    ctx.setClassName(className);
    ctx.implementsInterfaces(CustomTypeHandler.class.getName());

    // Copy mutable state so the generated class is immutable
    ctx.addField(
        true,
        true,
        "java.util.Map<CustomTypeRegistration, CustomCodec<?, ?>>",
        "REGISTRY",
        new Expression.NewInstance(
            TypeRef.of(HashMap.class),
            Reference.fieldRef("CustomTypeEncoderRegistry.REGISTRY", TypeRef.of(Map.class))));
    ctx.addField(
        true,
        true,
        "java.util.Map<CustomTypeRegistration, CustomCollectionFactory<?, ?>>",
        "COLLECTION_REGISTRY",
        new Expression.NewInstance(
            TypeRef.of(HashMap.class),
            Reference.fieldRef(
                "CustomTypeEncoderRegistry.COLLECTION_REGISTRY", TypeRef.of(Map.class))));
    // Dynamic lookup, not used by generated row codec
    ctx.addMethod(
        "public",
        "findCodec",
        "return CustomTypeEncoderRegistry.findCodec(REGISTRY, beanType, fieldType);",
        CustomCodec.class,
        Class.class,
        "beanType",
        Class.class,
        "fieldType");
    ctx.addMethod(
        "public",
        "findCollectionFactory",
        "return CustomTypeEncoderRegistry.findCollectionFactory(COLLECTION_REGISTRY, containerType, elementType);",
        CustomCollectionFactory.class,
        Class.class,
        "containerType",
        Class.class,
        "elementType");
    // Static dispatch table for custom codecs
    REGISTRY.forEach(
        new BiConsumer<CustomTypeRegistration, CustomCodec<?, ?>>() {
          int codecCount = 0;

          @Override
          public void accept(final CustomTypeRegistration reg, final CustomCodec<?, ?> enc) {
            codecCount++;
            final String codecFieldName =
                "CODEC_"
                    + reg.getBeanType().getSimpleName()
                    + "_"
                    + reg.getFieldType().getSimpleName()
                    + "_"
                    + codecCount;
            ctx.addStaticMethod(
                "encode",
                "return ("
                    + ctx.type(enc.encodedType())
                    + ")"
                    + codecFieldName
                    + ".encode(fieldValue);",
                enc.encodedType().getRawType(),
                reg.getBeanType(),
                "bean",
                reg.getFieldType(),
                "fieldValue");
            ctx.addStaticMethod(
                "decode",
                "return ("
                    + reg.getFieldType().getName()
                    + ")"
                    + codecFieldName
                    + ".decode(encodedValue);",
                reg.getFieldType(),
                reg.getBeanType(),
                "bean",
                reg.getFieldType(),
                "fieldNull",
                enc.encodedType().getRawType(),
                "encodedValue");
            ctx.addField(
                true,
                true,
                CustomCodec.class.getName(),
                codecFieldName,
                Expression.Invoke.inlineInvoke(
                    new Expression.Reference("CustomTypeEncoderRegistry"),
                    "findCodec",
                    TypeRef.of(CustomCodec.class),
                    false,
                    new Expression.Reference("REGISTRY", TypeRef.of(Map.class)),
                    Expression.Literal.ofClass(reg.getBeanType()),
                    Expression.Literal.ofClass(reg.getFieldType())));
          }
        });

    // Static dispatch table for custom collection factories
    COLLECTION_REGISTRY.forEach(
        new BiConsumer<CustomTypeRegistration, CustomCollectionFactory<?, ?>>() {
          int factoryCount = 0;

          @Override
          public void accept(
              final CustomTypeRegistration reg, final CustomCollectionFactory<?, ?> dec) {
            factoryCount++;
            final String factoryFieldName =
                "FACTORY_"
                    + reg.getBeanType().getSimpleName()
                    + "_"
                    + reg.getFieldType().getSimpleName()
                    + "_"
                    + factoryCount;
            ctx.addStaticMethod(
                "newCollection",
                "return ("
                    + ctx.type(reg.getBeanType())
                    + ")"
                    + factoryFieldName
                    + ".newCollection(numElements);",
                reg.getBeanType(),
                reg.getBeanType(),
                "collectionNull",
                reg.getFieldType(),
                "elementNull",
                int.class,
                "numElements");
            ctx.addField(
                true,
                true,
                ctx.type(CustomCollectionFactory.class),
                factoryFieldName,
                Expression.Invoke.inlineInvoke(
                    new Expression.Reference("CustomTypeEncoderRegistry"),
                    "findCollectionFactory",
                    TypeRef.of(CustomCollectionFactory.class),
                    false,
                    new Expression.Reference("COLLECTION_REGISTRY", TypeRef.of(Map.class)),
                    Expression.Literal.ofClass(reg.getBeanType()),
                    Expression.Literal.ofClass(reg.getFieldType())));
          }
        });

    final CompileUnit compileUnit = new CompileUnit(pkg, className, ctx.genCode());
    final Map<String, byte[]> classByteCodes =
        JaninoUtils.toBytecode(CustomTypeEncoderRegistry.class.getClassLoader(), compileUnit);
    final byte[] code = classByteCodes.values().iterator().next();
    try {
      generatedHandler =
          (CustomTypeHandler)
              DefineClass.defineClass(
                      compileUnit.getQualifiedClassName(),
                      CustomTypeEncoderRegistry.class,
                      CustomTypeEncoderRegistry.class.getClassLoader(),
                      CustomTypeEncoderRegistry.class.getProtectionDomain(),
                      code)
                  .getConstructor()
                  .newInstance();
    } catch (final Exception e) {
      if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  static CustomCodec<?, ?> findCodec(
      final Map<CustomTypeRegistration, CustomCodec<?, ?>> registry,
      final Class<?> beanType,
      final Class<?> fieldType) {
    CustomCodec<?, ?> result = registry.get(new CustomTypeRegistration(beanType, fieldType));
    if (result == null) {
      result = registry.get(new CustomTypeRegistration(Object.class, fieldType));
    }
    return result;
  }

  static CustomCollectionFactory<?, ?> findCollectionFactory(
      final Map<CustomTypeRegistration, CustomCollectionFactory<?, ?>> registry,
      final Class<?> containerType,
      final Class<?> elementType) {
    return registry.get(new CustomTypeRegistration(containerType, elementType));
  }
}
