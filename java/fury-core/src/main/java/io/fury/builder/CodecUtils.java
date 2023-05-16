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

package io.fury.builder;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import io.fury.Fury;
import io.fury.codegen.CodeGenerator;
import io.fury.codegen.CompileUnit;
import io.fury.resolver.FieldResolver;
import io.fury.serializer.Serializer;
import io.fury.type.ClassDef;
import java.util.Collections;

/**
 * Codec util to create and load jit serializer class.
 *
 * @author chaokunyang
 */
public class CodecUtils {

  public static <T> Class<? extends Serializer<T>> loadOrGenObjectCodecClass(
      Class<T> cls, Fury fury) {
    Preconditions.checkNotNull(fury);
    BaseObjectCodecBuilder codecBuilder = new ObjectCodecBuilder(cls, fury);
    return loadOrGenCodecClass(cls, fury, codecBuilder);
  }

  public static <T> Class<? extends Serializer<T>> loadOrGenMetaSharedCodecClass(
      Fury fury, Class<T> cls, ClassDef classDef) {
    Preconditions.checkNotNull(fury);
    MetaSharedCodecBuilder codecBuilder =
        new MetaSharedCodecBuilder(TypeToken.of(cls), fury, classDef);
    return loadOrGenCodecClass(cls, fury, codecBuilder);
  }

  public static <T> Class<? extends Serializer<T>> loadOrGenCompatibleCodecClass(
      Class<T> cls, Fury fury) {
    FieldResolver resolver = FieldResolver.of(fury, cls, true, false);
    return loadOrGenCompatibleCodecClass(cls, fury, resolver, Generated.GeneratedSerializer.class);
  }

  public static <T> Class<? extends Serializer<T>> loadOrGenCompatibleCodecClass(
      Class<T> cls, Fury fury, FieldResolver fieldResolver, Class<?> parentSerializerClass) {
    Preconditions.checkNotNull(fury);
    BaseObjectCodecBuilder codecBuilder =
        new CompatibleCodecBuilder(TypeToken.of(cls), fury, fieldResolver, parentSerializerClass);
    return loadOrGenCodecClass(cls, fury, codecBuilder);
  }

  @SuppressWarnings("unchecked")
  static <T> Class<? extends Serializer<T>> loadOrGenCodecClass(
      Class<T> beanClass, Fury fury, BaseObjectCodecBuilder codecBuilder) {
    // use genCodeFunc to avoid gen code repeatedly
    CompileUnit compileUnit =
        new CompileUnit(
            CodeGenerator.getPackage(beanClass),
            codecBuilder.codecClassName(beanClass),
            codecBuilder::genCode);
    CodeGenerator codeGenerator;
    ClassLoader beanClassClassLoader =
        beanClass.getClassLoader() == null
            ? Thread.currentThread().getContextClassLoader()
            : beanClass.getClassLoader();
    try {
      // generated code imported fury classes.
      beanClassClassLoader.loadClass(Fury.class.getName());
      codeGenerator = CodeGenerator.getSharedCodeGenerator(beanClassClassLoader);
    } catch (ClassNotFoundException e) {
      codeGenerator =
          CodeGenerator.getSharedCodeGenerator(
              beanClassClassLoader, fury.getClass().getClassLoader());
    }
    ClassLoader classLoader =
        codeGenerator.compile(
            Collections.singletonList(compileUnit), compileState -> compileState.lock.lock());
    String className = codecBuilder.codecQualifiedClassName(beanClass);
    try {
      return (Class<? extends Serializer<T>>) classLoader.loadClass(className);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("Impossible because we just compiled class", e);
    }
  }
}
