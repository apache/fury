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

import com.google.common.reflect.TypeToken;
import java.util.Collections;
import org.apache.fury.Fury;
import org.apache.fury.codegen.CodeGenerator;
import org.apache.fury.codegen.CompileUnit;
import org.apache.fury.meta.ClassDef;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.resolver.FieldResolver;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.util.Preconditions;

/** Codec util to create and load jit serializer class. */
public class CodecUtils {

  // TODO(chaokunyang) how to uninstall org.apache.fury.codegen/builder classes for graalvm build
  // time
  //  maybe use a temporal URLClassLoader
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
    if (beanClassClassLoader == null) {
      beanClassClassLoader = fury.getClass().getClassLoader();
    }
    ClassResolver classResolver = fury.getClassResolver();
    try {
      // generated code imported fury classes.
      beanClassClassLoader.loadClass(Fury.class.getName());
      codeGenerator = classResolver.getCodeGenerator(beanClassClassLoader);
      if (codeGenerator == null) {
        codeGenerator = CodeGenerator.getSharedCodeGenerator(beanClassClassLoader);
        // Hold strong reference of {@link CodeGenerator}, so the referent of `DelayedRef`
        // won't be null.
        classResolver.setCodeGenerator(beanClassClassLoader, codeGenerator);
      }
    } catch (ClassNotFoundException e) {
      codeGenerator =
          classResolver.getCodeGenerator(beanClassClassLoader, fury.getClass().getClassLoader());
      ClassLoader[] loaders = {beanClassClassLoader, fury.getClass().getClassLoader()};
      if (codeGenerator == null) {
        codeGenerator =
            CodeGenerator.getSharedCodeGenerator(
                beanClassClassLoader, fury.getClass().getClassLoader());
        // Hold strong reference of {@link CodeGenerator}, so the referent of `DelayedRef`
        // won't be null.
        classResolver.setCodeGenerator(loaders, codeGenerator);
      }
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
