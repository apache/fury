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

package io.fury.resolver;

import com.google.common.base.Preconditions;
import io.fury.config.Language;
import io.fury.serializer.Serializer;
import io.fury.util.ReflectionUtils;
import io.fury.util.function.Functions;

/**
 * This class put together object type related information to reduce array/map loop up when
 * serialization.
 *
 * @author chaokunyang
 */
public class ClassInfo {
  final Class<?> cls;
  final EnumStringBytes fullClassNameBytes;
  final EnumStringBytes packageNameBytes;
  final EnumStringBytes classNameBytes;
  final boolean isDynamicGeneratedClass;
  final EnumStringBytes typeTagBytes;
  Serializer<?> serializer;
  // use primitive to avoid boxing
  short classId;

  ClassInfo(
      Class<?> cls,
      EnumStringBytes fullClassNameBytes,
      EnumStringBytes packageNameBytes,
      EnumStringBytes classNameBytes,
      boolean isDynamicGeneratedClass,
      EnumStringBytes typeTagBytes,
      Serializer<?> serializer,
      short classId) {
    this.cls = cls;
    this.fullClassNameBytes = fullClassNameBytes;
    this.packageNameBytes = packageNameBytes;
    this.classNameBytes = classNameBytes;
    this.isDynamicGeneratedClass = isDynamicGeneratedClass;
    this.typeTagBytes = typeTagBytes;
    this.serializer = serializer;
    this.classId = classId;
    if (cls != null && classId == ClassResolver.NO_CLASS_ID) {
      Preconditions.checkArgument(classNameBytes != null);
    }
  }

  ClassInfo(
      ClassResolver classResolver,
      Class<?> cls,
      String tag,
      Serializer<?> serializer,
      short classId) {
    this.cls = cls;
    this.serializer = serializer;
    EnumStringResolver enumStringResolver = classResolver.getEnumStringResolver();
    if (cls != null && classResolver.getFury().getLanguage() != Language.JAVA) {
      this.fullClassNameBytes = enumStringResolver.getOrCreateEnumStringBytes(cls.getName());
    } else {
      this.fullClassNameBytes = null;
    }
    if (cls != null
        && (classId == ClassResolver.NO_CLASS_ID || classId == ClassResolver.REPLACE_STUB_ID)) {
      // REPLACE_STUB_ID for write replace class in `ClassSerializer`.
      String packageName = ReflectionUtils.getPackage(cls);
      this.packageNameBytes = enumStringResolver.getOrCreateEnumStringBytes(packageName);
      this.classNameBytes =
          enumStringResolver.getOrCreateEnumStringBytes(
              ReflectionUtils.getClassNameWithoutPackage(cls));
    } else {
      this.packageNameBytes = null;
      this.classNameBytes = null;
    }
    if (tag != null) {
      this.typeTagBytes = enumStringResolver.getOrCreateEnumStringBytes(tag);
    } else {
      this.typeTagBytes = null;
    }
    this.classId = classId;
    if (cls != null) {
      boolean isLambda = Functions.isLambda(cls);
      boolean isProxy = ReflectionUtils.isJdkProxy(cls);
      this.isDynamicGeneratedClass = isLambda || isProxy;
      if (isLambda) {
        this.classId = ClassResolver.LAMBDA_STUB_ID;
      }
      if (isProxy) {
        this.classId = ClassResolver.JDK_PROXY_STUB_ID;
      }
    } else {
      this.isDynamicGeneratedClass = false;
    }
  }

  public Class<?> getCls() {
    return cls;
  }

  public short getClassId() {
    return classId;
  }

  public EnumStringBytes getPackageNameBytes() {
    return packageNameBytes;
  }

  public EnumStringBytes getClassNameBytes() {
    return classNameBytes;
  }

  @SuppressWarnings("unchecked")
  public <T> Serializer<T> getSerializer() {
    return (Serializer<T>) serializer;
  }

  @Override
  public String toString() {
    return "ClassInfo{"
        + "cls="
        + cls
        + ", fullClassNameBytes="
        + fullClassNameBytes
        + ", isDynamicGeneratedClass="
        + isDynamicGeneratedClass
        + ", serializer="
        + serializer
        + ", classId="
        + classId
        + '}';
  }
}
