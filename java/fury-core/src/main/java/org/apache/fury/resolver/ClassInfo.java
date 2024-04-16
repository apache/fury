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

package org.apache.fury.resolver;

import org.apache.fury.config.Language;
import org.apache.fury.meta.MetaString.Encoding;
import org.apache.fury.meta.MetaStringEncoder;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.util.Preconditions;
import org.apache.fury.util.ReflectionUtils;
import org.apache.fury.util.function.Functions;

/**
 * This class put together object type related information to reduce array/map loop up when
 * serialization.
 */
public class ClassInfo {
  final Class<?> cls;
  final MetaStringBytes fullClassNameBytes;
  final MetaStringBytes packageNameBytes;
  final MetaStringBytes classNameBytes;
  final boolean isDynamicGeneratedClass;
  final MetaStringBytes typeTagBytes;
  Serializer<?> serializer;
  // use primitive to avoid boxing
  // class id must be less than Integer.MAX_VALUE/2 since we use bit 0 as class id flag.
  short classId;

  ClassInfo(
      Class<?> cls,
      MetaStringBytes fullClassNameBytes,
      MetaStringBytes packageNameBytes,
      MetaStringBytes classNameBytes,
      boolean isDynamicGeneratedClass,
      MetaStringBytes typeTagBytes,
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
    MetaStringResolver metaStringResolver = classResolver.getMetaStringResolver();
    if (cls != null && classResolver.getFury().getLanguage() != Language.JAVA) {
      this.fullClassNameBytes =
          metaStringResolver.getOrCreateMetaStringBytes(
              new MetaStringEncoder('.', '_').encode(cls.getName(), Encoding.UTF_8));
    } else {
      this.fullClassNameBytes = null;
    }
    if (cls != null
        && (classId == ClassResolver.NO_CLASS_ID || classId == ClassResolver.REPLACE_STUB_ID)) {
      // REPLACE_STUB_ID for write replace class in `ClassSerializer`.
      String packageName = ReflectionUtils.getPackage(cls);
      this.packageNameBytes =
          metaStringResolver.getOrCreateMetaStringBytes(
              new MetaStringEncoder('.', '_').encode(packageName));
      this.classNameBytes =
          metaStringResolver.getOrCreateMetaStringBytes(
              new MetaStringEncoder('_', '$')
                  .encode(ReflectionUtils.getClassNameWithoutPackage(cls)));
    } else {
      this.packageNameBytes = null;
      this.classNameBytes = null;
    }
    if (tag != null) {
      this.typeTagBytes =
          metaStringResolver.getOrCreateMetaStringBytes(
              new MetaStringEncoder('.', '_').encode(tag, Encoding.UTF_8));
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

  public MetaStringBytes getPackageNameBytes() {
    return packageNameBytes;
  }

  public MetaStringBytes getClassNameBytes() {
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
